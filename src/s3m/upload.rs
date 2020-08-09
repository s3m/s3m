use crate::s3::actions;
use crate::s3::S3;
use futures::stream::FuturesUnordered;
use std::error;
//use tokio::fs::File;
//use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::task;
//use tokio_util::codec::{BytesCodec, FramedRead};

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
    let mut parts: Vec<Vec<u64>> = Vec::new();
    while seek < file_size {
        if (file_size - seek) <= chunk {
            chunk = file_size % chunk;
        }
        parts.push(vec![seek, chunk]);
        seek += chunk;
    }

    let mut tasks = FuturesUnordered::new();
    for part in 0..parts.len() {
        let p = parts.clone();
        let k = key.clone();
        let f = file.clone();
        let uid = upload_id.clone();
        let s3 = s3.clone();
        tasks.push(task::spawn(async move {
            println!(
                "part: {}, seek: {}, chunk: {}",
                part, p[part][0], p[part][1]
            );
            let action = actions::UploadPart::new(
                k,
                f,
                format!("{}", part),
                uid,
                p[part][0] + 1,
                p[part][1],
            );
            match action.request(s3).await {
                Ok(rs) => println!("---\n{:#?}\n---", rs),
                Err(e) => eprintln!("{}", e),
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
    let action = actions::CompleteMultipartUpload::new(key.clone(), upload_id);
    let response = action.request(s3).await?;

    Ok(())
}
