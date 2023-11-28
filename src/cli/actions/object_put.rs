use crate::{
    cli::{actions::Action, progressbar::Bar},
    s3::{
        checksum::{Checksum, ChecksumAlgorithm},
        tools, S3,
    },
    stream::{
        db::Db, upload_default::upload, upload_multipart::upload_multipart, upload_stdin::stream,
    },
};
use anyhow::{anyhow, Context, Result};
use std::{
    fs::metadata,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

const MAX_PART_SIZE: usize = 5_368_709_120;
const MAX_FILE_SIZE: usize = 5_497_558_138_880;
const MAX_PARTS_PER_UPLOAD: usize = 10_000;

pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::PutObject {
        acl,
        meta,
        mut buf_size,
        file,
        key,
        pipe,
        s3m_dir,
        quiet,
        tmp_dir,
        checksum_algorithm,
    } = action
    {
        if pipe {
            let etag = stream(s3, &key, acl, meta, quiet, tmp_dir).await?;
            if !quiet {
                println!("ETag: {etag}");
            }
        } else if let Some(file) = &file {
            // Get file size and last modified time
            let (file_size, file_mtime) = metadata(file)
                .map(|m| {
                    if m.is_file() {
                        Ok(m)
                    } else {
                        Err(anyhow!(
                            "cannot read the file: {}, verify file exist and is not a directory.",
                            &file
                        ))
                    }
                })?
                .and_then(|md| {
                    Ok((
                        md.len(),
                        md.modified()
                            .unwrap_or_else(|_| SystemTime::now())
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis())?,
                    ))
                })?;

            // <https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/>
            if file_size > MAX_FILE_SIZE as u64 {
                return Err(anyhow!("object size limit 5 TB"));
            }

            // calculate the chunk size
            let mut parts = file_size / buf_size as u64;
            while parts > MAX_PARTS_PER_UPLOAD as u64 {
                buf_size *= 2;
                parts = file_size / buf_size as u64;
            }

            if buf_size > MAX_PART_SIZE {
                return Err(anyhow!("max part size 5 GB"));
            }

            // get the checksum with progress bar
            let blake3_checksum = checksum(file, quiet)?;

            let additional_checksum =
                calculate_additional_checksum(file.to_owned(), checksum_algorithm).await;

            // keep track of the uploaded parts
            let db = Db::new(s3, &key, &blake3_checksum, file_mtime, &s3m_dir)
                .context("could not create stream tree, try option \"--clean\"")?;

            // check if file has been uploaded already
            let etag = &db
                .check()?
                .context("could not query db, try option \"--clean\", to clean it");

            // if file has been uploaded already, return the etag
            if let Ok(etag) = etag {
                if !quiet {
                    println!("{etag}");
                }
                return Ok(());
            };

            // depending on the file size, upload the file in parts or as a whole
            if file_size > buf_size as u64 {
                let rs = upload_multipart(
                    s3,
                    &key,
                    file,
                    file_size,
                    buf_size as u64,
                    &db,
                    acl,
                    meta,
                    quiet,
                )
                .await
                .context("multipart upload failed")?;
                if !quiet {
                    println!("{rs}");
                }
            } else {
                // upload the file as a whole if it is smaller than the chunk size (buf_size)
                let rs = upload(
                    s3,
                    &key,
                    file,
                    file_size,
                    &db,
                    acl,
                    meta,
                    quiet,
                    additional_checksum,
                )
                .await?;
                if !quiet {
                    println!("{rs}");
                }
            }
        }
    }

    Ok(())
}

/// Calculate the blake3 checksum of a file
pub fn checksum(file: &str, quiet: bool) -> Result<String> {
    let pb = if quiet {
        Bar::default()
    } else {
        Bar::new_spinner()
    };

    let checksum = tools::blake3(file).context("could not calculate the checksum")?;

    if let Some(pb) = pb.progress.as_ref() {
        pb.finish_and_clear();
        println!("checksum: {}", &checksum);
    }

    Ok(checksum)
}

async fn additional_checksum(file: String, algorithm: String) -> Result<Checksum> {
    let algorithm = ChecksumAlgorithm::from_str(&algorithm.to_lowercase())
        .map_err(|()| anyhow!("invalid checksum algorithm: {}", algorithm))?;

    let mut checksum = Checksum::new(algorithm);
    checksum
        .calculate(&file)
        .context("could not calculate the checksum")?;

    Ok(checksum)
}

// New function encapsulating the logic
async fn calculate_additional_checksum(
    file: String,
    checksum_algorithm: Option<String>,
) -> Option<Checksum> {
    if let Some(algorithm) = checksum_algorithm {
        let file_clone = file.clone();

        // Use oneshot channel to send the result back from the async task
        let (sender, receiver) = oneshot::channel();
        let additional_checksum_task = additional_checksum(file_clone, algorithm);

        // Spawn the task and send the result to the main thread
        tokio::spawn(async move {
            let additional_checksum_result = additional_checksum_task.await;
            let _ = sender.send(additional_checksum_result);
        });

        // Wait for the result from the async task
        (receiver.await).map_or(None, |additional_checksum| {
            Some(
                additional_checksum
                    .context("could not calculate the additional checksum")
                    .unwrap_or_else(|_| Checksum::new(ChecksumAlgorithm::None)),
            )
        })
    } else {
        None
    }
}
