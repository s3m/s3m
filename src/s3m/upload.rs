use crate::s3::actions;
use crate::s3::S3;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use std::collections::BTreeMap;
use std::error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

const MAX_PARTS_PER_UPLOAD: u64 = 10_000;
const MAX_PART_SIZE: u64 = 5_368_709_120;

async fn progress_bar_bytes(
    file_size: u64,
    mut receiver: UnboundedReceiver<usize>,
) -> Result<(), Box<dyn error::Error>> {
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({eta})")
            .progress_chars("█▉▊▋▌▍▎▏  ·"),
    );
    // print progress bar
    let mut uploaded = 0;
    while let Some(i) = receiver.recv().await {
        let new = min(uploaded + i as u64, file_size);
        uploaded = new;
        pb.set_position(new);
    }
    pb.finish();
    Ok(())
}

pub async fn upload(
    s3: S3,
    key: String,
    file: String,
    file_size: u64,
) -> Result<String, Box<dyn error::Error>> {
    let (sender, receiver) = unbounded_channel();
    let action = actions::PutObject::new(key, file, Some(sender));
    let response = tokio::try_join!(progress_bar_bytes(file_size, receiver), action.request(s3))?.1;
    Ok(response)
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
pub async fn multipart_upload(
    s3: S3,
    key: String,
    file: String,
    file_size: u64,
    chunk_size: u64,
    threads: usize,
) -> Result<String, Box<dyn error::Error>> {
    // Initiate Multipart Upload - request an Upload ID
    let action = actions::CreateMultipartUpload::new(key.clone());
    let response = action.request(s3.clone()).await?;
    let upload_id = response.upload_id;

    // calculate the chunk size
    let mut parts = file_size / chunk_size;
    let mut chunk = chunk_size;

    while parts > MAX_PARTS_PER_UPLOAD {
        chunk = chunk * 2;
        parts = file_size / chunk;
    }

    if chunk > MAX_PART_SIZE {
        return Err("Max part size 5 GB".into());
    }

    let mut seek: u64 = 0;
    // Part: [chunk, etag, part_number, seek]
    let mut parts: Vec<actions::Part> = Vec::new();
    let mut number: u16 = 1;
    while seek < file_size {
        if (file_size - seek) <= chunk {
            chunk = file_size % chunk;
        }
        parts.push(actions::Part {
            number,
            seek,
            chunk,
            ..Default::default()
        });
        seek += chunk;
        number += 1;
    }

    // Upload parts
    let pb = ProgressBar::new(parts.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {pos}/{len} ({eta})")
            .progress_chars("█▉▊▋▌▍▎▏  ·"),
    );
    let mut tasks = FuturesUnordered::new();
    let mut uploaded: BTreeMap<u16, actions::Part> = BTreeMap::new();
    let total_parts = parts.len();
    let mut retry_parts: Vec<actions::Part> = Vec::new();
    for part in &mut parts {
        let key = key.clone();
        let file = file.clone();
        let uid = upload_id.clone();
        let s3 = s3.clone();
        tasks.push(async move { upload_part(s3, key, file, uid, part.clone()).await });

        // limit to N threads
        if tasks.len() == threads {
            while let Some(p) = tasks.next().await {
                if p.etag.is_empty() {
                    retry_parts.push(p);
                } else {
                    uploaded.insert(p.number, p);
                    pb.inc(1)
                }
            }
        }
    }

    loop {
        match tasks.next().await {
            Some(p) => {
                if p.etag.is_empty() {
                    retry_parts.push(p);
                } else {
                    uploaded.insert(p.number, p);
                    pb.inc(1)
                }
            }
            None => {
                pb.finish();
                break;
            }
        }
    }

    println!("retry: {:#?}", retry_parts);
    while !retry_parts.is_empty() {
        todo!();
    }

    if uploaded.len() < total_parts {
        eprintln!(
            "probably missing parts {} < {}",
            uploaded.len(),
            total_parts
        );
        todo!();
    }

    // Complete Multipart Upload
    let action = actions::CompleteMultipartUpload::new(key.clone(), upload_id, uploaded);
    let rs = action.request(s3).await?;
    Ok(format!("ETag: {}", rs.e_tag))
}

async fn upload_part(
    s3: S3,
    key: String,
    file: String,
    uid: String,
    mut part: actions::Part,
) -> actions::Part {
    let action = actions::UploadPart::new(
        key,
        file,
        format!("{}", part.number),
        uid,
        part.seek,
        part.chunk,
    );
    if let Ok(etag) = action.request(s3).await {
        part.etag = etag;
        part
    } else {
        part
    }
}
