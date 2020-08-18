use crate::s3::actions;
use crate::s3::S3;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde_cbor::{de::from_mut_slice, to_vec};
use std::cmp::min;
use std::collections::BTreeMap;
use std::error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

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
    s3: &S3,
    key: &str,
    file: &str,
    file_size: u64,
) -> Result<String, Box<dyn error::Error>> {
    let (sender, receiver) = unbounded_channel();
    let action = actions::PutObject::new(&key, &file, Some(sender));
    let response =
        tokio::try_join!(progress_bar_bytes(file_size, receiver), action.request(&s3))?.1;
    Ok(response)
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
pub async fn multipart_upload(
    s3: &S3,
    key: &str,
    file: &str,
    file_size: u64,
    chunk_size: u64,
    threads: usize,
    checksum: &str,
    home_dir: &str,
) -> Result<String, Box<dyn error::Error>> {
    // try to upload/retry broken uploads besides keeping track of uploaded files (prevent
    // uploading again)
    let db = sled::Config::new()
        .path(format!("{}/.s3m/streams/{}", home_dir, checksum))
        .open()?;

    let mut upload_id = String::new();
    if let Ok(u) = db.get("uid") {
        if let Some(u) = u {
            if let Ok(u) = String::from_utf8(u.to_vec()) {
                println!("uid found...");
                upload_id = u;
            }
        }
    };

    if upload_id.is_empty() {
        // Initiate Multipart Upload - request an Upload ID
        let action = actions::CreateMultipartUpload::new(&key);
        let response = action.request(s3).await?;
        upload_id = response.upload_id;
    }
    db.insert("uid", upload_id.as_bytes())?;

    let mut chunk = chunk_size;
    let mut seek: u64 = 0;
    // Part: [chunk, etag, part_number, seek]
    let mut parts: Vec<actions::Part> = Vec::new();
    let mut number: u16 = 1;
    while seek < file_size {
        if (file_size - seek) <= chunk {
            chunk = file_size % chunk;
        }
        let part = actions::Part {
            number,
            seek,
            chunk,
            ..Default::default()
        };
        let cbor_part = to_vec(&part)?;
        db.insert(format!("{}", number), cbor_part)?;
        db.flush()?;
        parts.push(part);
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
        let part = part.clone();
        tasks.push(async { upload_part(&s3, &key, &file, &upload_id, part).await });

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
    let action = actions::CompleteMultipartUpload::new(&key, &upload_id, uploaded);
    let rs = action.request(&s3).await?;
    Ok(format!("ETag: {}", rs.e_tag))
}

async fn upload_part(
    s3: &S3,
    key: &str,
    file: &str,
    uid: &str,
    mut part: actions::Part,
) -> actions::Part {
    let pn = format!("{}", part.number);
    let action = actions::UploadPart::new(&key, &file, &pn, &uid, part.seek, part.chunk);
    match action.request(&s3).await {
        Ok(etag) => part.etag = etag,
        Err(e) => eprintln!("Could not upload part #{}, error: {}", part.number, e),
    }
    part
}
