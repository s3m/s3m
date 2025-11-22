use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
    s3::{
        S3,
        checksum::{Checksum, ChecksumAlgorithm},
        tools,
    },
    stream::{
        db::Db, upload_compressed::stream_compressed,
        upload_compressed_encrypted::stream_compressed_encrypted, upload_default::upload,
        upload_encrypted::stream_encrypted, upload_multipart::upload_multipart,
        upload_stdin::stream_stdin,
        upload_stdin_compressed_encrypted::stream_stdin_compressed_encrypted,
    },
};
use anyhow::{Context, Result, anyhow};
use std::{
    fs::metadata,
    path::Path,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_FILE_SIZE: u64 = 5_497_558_138_880;

/// # Errors
/// Will return an error if the action fails
#[allow(clippy::too_many_lines)]
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
    } = action
    {
        if pipe {
            log::debug!("PIPE - streaming from stdin");

            let etag = if globals.compress && globals.encrypt {
                log::info!(
                    "COMPRESS + ENCRYPT - streaming compressed and encrypted data from stdin"
                );

                stream_stdin_compressed_encrypted(s3, &key, acl, meta, quiet, tmp_dir, globals)
                    .await?
            } else {
                stream_stdin(s3, &key, acl, meta, quiet, tmp_dir, globals).await?
            };

            print_etag(&etag, quiet);
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

            // file_path
            let file_path = Path::new(file);

            match (globals.compress, globals.encrypt) {
                (true, true) => {
                    log::info!("COMPRESS + ENCRYPT - streaming encrypted and compressed file");

                    let etag = stream_compressed_encrypted(
                        s3, &key, acl, meta, quiet, tmp_dir, globals, file_path,
                    )
                    .await?;

                    print_etag(&etag, quiet);
                }
                (true, false) => {
                    log::info!("COMPRESS - streaming compressed file");

                    let etag =
                        stream_compressed(s3, &key, acl, meta, quiet, tmp_dir, globals, file_path)
                            .await?;

                    print_etag(&etag, quiet);
                }
                (false, true) => {
                    log::info!("ENCRYPT - streaming encrypted file");

                    let etag =
                        stream_encrypted(s3, &key, acl, meta, quiet, tmp_dir, globals, file_path)
                            .await?;

                    print_etag(&etag, quiet);
                }
                (false, false) => {
                    // we check here and not before because compressing the file changes the file size
                    if file_size > MAX_FILE_SIZE {
                        log::error!("object size limit 5 TB");
                        return Err(anyhow!("object size limit 5 TB"));
                    }

                    // get the part/chunk size
                    let part_size = tools::calculate_part_size(file_size, buf_size as u64)?;

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
                    }

                    // upload the file in parts if it is bigger than the chunk size (buf_size)
                    if file_size > part_size as u64 {
                        // return only the the additional checksum algorithm if the option is set
                        let additional_checksum =
                            calculate_additional_checksum(file_path, checksum_algorithm, false)
                                .await;

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

                        print_etag(&rs, quiet);

                    // upload the file as a whole if it is smaller than the chunk size (buf_size)
                    } else {
                        // calculate the additional checksum if the option is set
                        let additional_checksum =
                            calculate_additional_checksum(file_path, checksum_algorithm, true)
                                .await;

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

                        print_etag(&rs, quiet);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Print the `ETag` if not in quiet mode
fn print_etag(etag: &str, quiet: bool) {
    if !quiet {
        println!("ETag: {etag}");
    }
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
        .map_err(|()| anyhow!("invalid checksum algorithm: {algorithm}"))?;

    let mut checksum = Checksum::new(algorithm);

    if calculate {
        checksum
            .calculate(file)
            .await
            .context("could not calculate the checksum")?;
    }

    Ok(checksum)
}

async fn calculate_additional_checksum(
    file: &Path,
    checksum_algorithm: Option<String>,
    calculate: bool,
) -> Option<Checksum> {
    if let Some(algorithm) = checksum_algorithm {
        let file_path = file.to_path_buf(); // Proper cloning for owned data

        // Spawn the task with owned data
        let handle =
            tokio::spawn(
                async move { additional_checksum(&file_path, algorithm, calculate).await },
            );

        // Await the spawned task and handle all errors by returning None
        match handle.await {
            Ok(Ok(checksum)) => Some(checksum),
            Ok(Err(_checksum_error)) => None, // Checksum calculation failed
            Err(_join_error) => None,         // Task join failed
        }
    } else {
        None
    }
}
