use crate::s3::actions;
use crate::s3::S3;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_cbor::{de::from_reader, to_vec};
use sled::transaction::Transactional;
use std::cmp::min;
use std::collections::BTreeMap;
use std::error;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

#[derive(Debug, Serialize, Deserialize, Default)]
struct Part {
    etag: String,
    number: u16,
    seek: u64,
    chunk: u64,
}

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
    // keep track of uploads
    db.open_tree(b"uploaded")?;

    let db_parts = db.open_tree("parts")?;

    let mut chunk = chunk_size;
    let mut seek: u64 = 0;
    let mut number: u16 = 1;
    while seek < file_size {
        if (file_size - seek) <= chunk {
            chunk = file_size % chunk;
        }
        let part = Part {
            number,
            seek,
            chunk,
            ..Default::default()
        };
        let cbor_part = to_vec(&part)?;
        db_parts.insert(format!("{}", number), cbor_part)?;
        seek += chunk;
        number += 1;
    }
    db_parts.flush()?;

    // Upload parts progress bar
    let pb = ProgressBar::new(db_parts.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {pos}/{len} ({eta})")
            .progress_chars("█▉▊▋▌▍▎▏  ·"),
    );

    let mut tasks = FuturesUnordered::new();

    let mut parts = db_parts.iter().values();
    while let Some(part) = parts.next() {
        if let Ok(p) = part {
            let part: Part = from_reader(&p[..])?;
            tasks.push(async { upload_part(&s3, &key, &file, &upload_id, &db, part).await });
            // limit to N threads
            if tasks.len() == threads {
                while let Some(r) = tasks.next().await {
                    if let Ok(_) = r {
                        pb.inc(1)
                    }
                }
            }
        }
    }

    // consume remaining tasks
    loop {
        match tasks.next().await {
            Some(r) => {
                if let Ok(_) = r {
                    pb.inc(1)
                }
            }
            None => {
                pb.finish();
                break;
            }
        }
    }

    println!("parts: {}", db_parts.len());

    /*
    // Complete Multipart Upload
    let action = actions::CompleteMultipartUpload::new(&key, &upload_id, uploaded);
    let rs = action.request(&s3).await?;
    Ok(format!("ETag: {}", rs.e_tag))
    */
    Ok(format!("ETag: {}", "sopas"))
}

async fn a_test(
    db: &sled::Db,
) -> Result<(), Box<dyn error::Error>> {
    let unprocessed = db.open_tree(b"tasks")?;
    let processed = db.open_tree(b"done")?;

    let part = Part {1, "foo".to_string(), "bar".to_string()};
    let cbor_part = to_vec(&part)?;
    let x = (&unprocessed, &processed)
        .transaction(|(unprocessed, processed)| {
            unprocessed.remove(pn.as_bytes())?;
            processed.insert(pn.as_bytes(), cbor_part.clone())?;
            Ok(true)
        })
        .map_err(|err| match err {
            sled::transaction::TransactionError::Abort(err) => err,
            sled::transaction::TransactionError::Storage(err) => err,
        })?;

    Ok(())
}
