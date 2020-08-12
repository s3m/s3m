use crate::s3::actions;
use crate::s3::S3;
use futures::stream::FuturesUnordered;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use std::collections::BTreeMap;
use std::error;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task;

async fn progress_bar(
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
    let response = tokio::try_join!(progress_bar(file_size, receiver), action.request(s3))?.1;
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
    let action = actions::CreateMultipartUpload::new(key.clone());
    // do request and try to get upload_id
    let response = action.request(s3.clone()).await?;
    let upload_id = response.upload_id;
    // TODO
    // calculate parts,chunk
    let mut seek: u64 = 0;
    let mut chunk = chunk_size;
    // [[seek,chunk]..]
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

    let mut tasks = FuturesUnordered::new();
    let mut uploaded: BTreeMap<u16, actions::Part> = BTreeMap::new();
    let total_parts = parts.len();
    let pb = ProgressBar::new(total_parts as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {pos}/{len} ({eta})")
            .progress_chars("█▉▊▋▌▍▎▏  ·"),
    );
    for part in parts {
        let k = key.clone();
        let f = file.clone();
        let uid = upload_id.clone();
        let s3 = s3.clone();
        tasks.push(task::spawn(async move {
            let action = actions::UploadPart::new(
                k,
                f,
                format!("{}", part.number),
                uid,
                part.seek,
                part.chunk,
            );
            if let Ok(etag) = action.request(s3).await {
                return (
                    part.number,
                    actions::Part {
                        number: part.number,
                        etag,
                        ..Default::default()
                    },
                );
            } else {
                println!("failed part: {:#?}", part);
                todo!();
            };
        }));
        if tasks.len() == threads {
            while let Some(part) = tasks.next().await {
                if let Ok(p) = part {
                    uploaded.insert(p.0, p.1);
                }
                pb.inc(1);
            }
        }
    }
    // This loop is how to wait for all the elements in a `FuturesUnordered<T>`
    // to complete. `_item` is just the unit tuple, `()`, because we did not
    // return anything
    while let Some(part) = tasks.next().await {
        if let Ok(p) = part {
            uploaded.insert(p.0, p.1);
        }
        pb.inc(1);
    }
    pb.finish();

    if uploaded.len() < total_parts {
        todo!();
    }

    // finish multipart
    let action = actions::CompleteMultipartUpload::new(key.clone(), upload_id, uploaded);
    let rs = action.request(s3).await?;
    Ok(format!("ETag: {}", rs.e_tag))
}
