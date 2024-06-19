use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
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
    path::Path,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action, globals: GlobalArgs) -> Result<()> {
    if let Action::PutObject {
        acl,
        meta,
        buf_size,
        file,
        key,
        pipe,
        s3m_dir,
        quiet,
        tmp_dir,
        checksum_algorithm,
        number,
        compress,
    } = action
    {
        if pipe {
            let etag = stream(s3, &key, acl, meta, quiet, tmp_dir, globals, compress).await?;
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

            // get the part/chunk size
            let part_size = tools::calculate_part_size(file_size, buf_size as u64)?;

            // file_path
            let file_path = Path::new(file);

            log::info!(
                "file path: {}\nfile size: {file_size}\nlast modified time: {file_mtime}\npart size: {part_size}",
                file_path.display()
            );

            // get the checksum with progress bar
            let blake3_checksum = blake3_checksum(file_path, quiet)?;

            log::info!("checksum: {}", &blake3_checksum);

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

            // compress the file if the option is set
            if compress {
                let etag = stream(s3, &key, acl, meta, quiet, tmp_dir, globals, true).await?;
                if !quiet {
                    println!("ETag: {etag}");
                }
                return Ok(());
            }

            // upload the file in parts if it is bigger than the chunk size (buf_size)
            if file_size > part_size as u64 {
                // return only the the additional checksum algorithm if the option is set
                let additional_checksum =
                    calculate_additional_checksum(file_path, checksum_algorithm, false).await;

                log::debug!("additional checksum: {:#?}", &additional_checksum);

                let rs = upload_multipart(
                    s3,
                    &key,
                    file_path,
                    file_size,
                    part_size,
                    &db,
                    acl,
                    meta,
                    quiet,
                    additional_checksum,
                    number,
                    globals,
                )
                .await
                .context("multipart upload failed")?;

                if !quiet {
                    println!("{rs}");
                }

            // upload the file as a whole if it is smaller than the chunk size (buf_size)
            } else {
                // calculate the additional checksum if the option is set
                let additional_checksum =
                    calculate_additional_checksum(file_path, checksum_algorithm, true).await;

                log::debug!("additional checksum: {:?}", &additional_checksum);

                let rs = upload(
                    s3,
                    &key,
                    file_path,
                    file_size,
                    &db,
                    acl,
                    meta,
                    quiet,
                    additional_checksum,
                    globals,
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
/// # Errors
/// Will return an error if the checksum fails
pub fn blake3_checksum(file: &Path, quiet: bool) -> Result<String> {
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

async fn additional_checksum(file: &Path, algorithm: String, calculate: bool) -> Result<Checksum> {
    let algorithm = ChecksumAlgorithm::from_str(&algorithm.to_lowercase())
        .map_err(|()| anyhow!("invalid checksum algorithm: {}", algorithm))?;

    let mut checksum = Checksum::new(algorithm);

    if calculate {
        checksum
            .calculate(file)
            .await
            .context("could not calculate the checksum")?;
    }

    Ok(checksum)
}

// New function encapsulating the logic
async fn calculate_additional_checksum(
    file: &Path,
    checksum_algorithm: Option<String>,
    calculate: bool,
) -> Option<Checksum> {
    if let Some(algorithm) = checksum_algorithm {
        let file_clone = file;

        // Use oneshot channel to send the result back from the async task
        let (sender, receiver) = oneshot::channel();
        let additional_checksum_task = additional_checksum(file_clone, algorithm, calculate).await;

        // Spawn the task and send the result to the main thread
        tokio::spawn(async move {
            let additional_checksum_result = additional_checksum_task;
            let _ = sender.send(additional_checksum_result);
        });

        // Wait for the result from the async task
        receiver
            .await
            .map_or(None, |additional_checksum| {
                Some(additional_checksum.context("could not calculate the additional checksum"))
            })
            .and_then(Result::ok)
    } else {
        None
    }
}
