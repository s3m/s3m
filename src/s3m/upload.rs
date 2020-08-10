use crate::s3::actions;
use crate::s3::S3;
use futures::stream::FuturesUnordered;
use std::error;
use std::sync::{Arc, Mutex};
use tokio::stream::StreamExt;
use tokio::task;

pub async fn upload(s3: S3, key: String, file: String) -> Result<String, Box<dyn error::Error>> {
    let action = actions::PutObject::new(key, file);
    Ok(action.request(s3).await?)
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
) -> Result<(), Box<dyn error::Error>> {
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
    // TODO
    //  https://doc.rust-lang.org/std/sync/mpsc/
    // https://doc.rust-lang.org/stable/book/second-edition/ch16-02-message-passing.html
    let total_parts = parts.len();
    let uploaded = Arc::new(Mutex::new(Vec::new()));
    for part in parts {
        let k = key.clone();
        let f = file.clone();
        let uid = upload_id.clone();
        let s3 = s3.clone();
        let clone = Arc::clone(&uploaded);
        tasks.push(task::spawn(async move {
            println!(
                "part: {}, seek: {}, chunk: {}",
                number, part.seek, part.chunk
            );
            let action =
                actions::UploadPart::new(k, f, format!("{}", number), uid, part.seek, part.chunk);
            if let Ok(etag) = action.request(s3).await {
                let mut v = clone.lock().unwrap();
                v.push(actions::Part {
                    number,
                    etag: etag,
                    ..Default::default()
                });
            } else {
                println!("failed part: {:#?}", part);
                todo!();
            };
        }));
        if tasks.len() == threads {
            tasks.next().await;
        }
    }
    // This loop is how to wait for all the elements in a `FuturesUnordered<T>`
    // to complete. `_item` is just the unit tuple, `()`, because we did not
    // return anything
    while let Some(_item) = tasks.next().await {}

    // finish multipart

    let v = uploaded.lock().unwrap();
    if total_parts == v.len() {
        let action = actions::CompleteMultipartUpload::new(key.clone(), upload_id, v.clone());
        let _response = action.request(s3).await?;
    }
    Ok(())
}
