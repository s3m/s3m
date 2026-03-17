use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
    s3::{
        S3,
        checksum::{Checksum, ChecksumAlgorithm},
        limits::MAX_OBJECT_SIZE_BYTES,
        tools,
    },
    stream::{
        FileStreamUpload,
        db::Db,
        state::{StreamMetadata, StreamMode, write_metadata},
        upload_compressed::stream_compressed,
        upload_compressed_encrypted::stream_compressed_encrypted,
        upload_default::{UploadRequest, upload},
        upload_encrypted::stream_encrypted,
        upload_multipart::{MultipartUploadRequest, upload_multipart},
        upload_stdin::stream_stdin,
        upload_stdin_compressed_encrypted::stream_stdin_compressed_encrypted,
        upload_stdin_encrypted::stream_stdin_encrypted,
    },
};
use anyhow::{Context, Result, anyhow};
use std::{
    collections::BTreeMap,
    fs::metadata,
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

struct PutObjectRequest {
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    buf_size: usize,
    file: Option<String>,
    host: String,
    key: String,
    pipe: bool,
    s3m_dir: PathBuf,
    quiet: bool,
    tmp_dir: PathBuf,
    checksum_algorithm: Option<String>,
    number: u8,
}

struct LocalFile {
    path: PathBuf,
    size: u64,
    mtime: u128,
}

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action, globals: GlobalArgs) -> Result<()> {
    if let Action::PutObject {
        acl,
        meta,
        buf_size,
        file,
        host,
        key,
        pipe,
        s3m_dir,
        quiet,
        tmp_dir,
        checksum_algorithm,
        number,
    } = action
    {
        return handle_put_object(
            s3,
            PutObjectRequest {
                acl,
                meta,
                buf_size,
                file,
                host,
                key,
                pipe,
                s3m_dir,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            },
            globals,
        )
        .await;
    }

    Ok(())
}

async fn handle_put_object(s3: &S3, request: PutObjectRequest, globals: GlobalArgs) -> Result<()> {
    let quiet = request.quiet;

    if request.pipe {
        let etag = handle_pipe_upload(s3, request, globals).await?;
        print_etag(&etag, quiet);
        return Ok(());
    }

    let local_file = request.load_local_file()?;
    if let Some(etag) = handle_file_upload(s3, request, globals, local_file).await? {
        print_etag(&etag, quiet);
    }

    Ok(())
}

async fn handle_pipe_upload(
    s3: &S3,
    request: PutObjectRequest,
    globals: GlobalArgs,
) -> Result<String> {
    let PutObjectRequest {
        acl,
        meta,
        key,
        quiet,
        tmp_dir,
        ..
    } = request;

    log::debug!("PIPE - streaming from stdin");

    match (globals.compress, globals.encrypt) {
        (true, true) => {
            log::info!("COMPRESS + ENCRYPT - streaming compressed and encrypted data from stdin");
            stream_stdin_compressed_encrypted(s3, &key, acl, meta, quiet, tmp_dir, globals).await
        }
        (false, true) => {
            log::info!("ENCRYPT - streaming encrypted data from stdin");
            stream_stdin_encrypted(s3, &key, acl, meta, quiet, tmp_dir, globals).await
        }
        _ => stream_stdin(s3, &key, acl, meta, quiet, tmp_dir, globals).await,
    }
}

async fn handle_file_upload(
    s3: &S3,
    request: PutObjectRequest,
    globals: GlobalArgs,
    local_file: LocalFile,
) -> Result<Option<String>> {
    let file_path = local_file.path.as_path();

    match (globals.compress, globals.encrypt) {
        (true, true) => {
            log::info!("COMPRESS + ENCRYPT - streaming encrypted and compressed file");
            stream_compressed_encrypted(FileStreamUpload {
                s3,
                object_key: &request.key,
                acl: request.acl,
                meta: request.meta,
                quiet: request.quiet,
                tmp_dir: request.tmp_dir,
                globals,
                file_path,
            })
            .await
            .map(Some)
        }
        (true, false) => {
            log::info!("COMPRESS - streaming compressed file");
            stream_compressed(FileStreamUpload {
                s3,
                object_key: &request.key,
                acl: request.acl,
                meta: request.meta,
                quiet: request.quiet,
                tmp_dir: request.tmp_dir,
                globals,
                file_path,
            })
            .await
            .map(Some)
        }
        (false, true) => {
            log::info!("ENCRYPT - streaming encrypted file");
            stream_encrypted(FileStreamUpload {
                s3,
                object_key: &request.key,
                acl: request.acl,
                meta: request.meta,
                quiet: request.quiet,
                tmp_dir: request.tmp_dir,
                globals,
                file_path,
            })
            .await
            .map(Some)
        }
        (false, false) => handle_standard_file_upload(s3, request, globals, local_file).await,
    }
}

async fn handle_standard_file_upload(
    s3: &S3,
    request: PutObjectRequest,
    globals: GlobalArgs,
    local_file: LocalFile,
) -> Result<Option<String>> {
    if local_file.size > MAX_OBJECT_SIZE_BYTES {
        log::error!("object size limit 5 TB");
        return Err(anyhow!("object size limit 5 TB"));
    }

    let part_size = tools::calculate_part_size(local_file.size, request.buf_size as u64)?;
    log::info!(
        "file path: {}\nfile size: {}\nlast modified time: {}\npart size: {part_size}",
        local_file.path.display(),
        local_file.size,
        local_file.mtime,
    );

    let blake3_checksum = blake3_checksum(&local_file.path, request.quiet)?;
    log::info!("checksum: {}", &blake3_checksum);

    let db = Db::new(
        s3,
        &request.key,
        &blake3_checksum,
        local_file.mtime,
        &request.s3m_dir,
    )
    .context("could not create stream tree, try option \"--clean\"")?;

    maybe_write_stream_metadata(
        &request,
        s3.bucket().unwrap_or_default(),
        &local_file,
        &db,
        &blake3_checksum,
        part_size,
    )?;

    if let Some(etag) = db.check()? {
        if !request.quiet {
            println!("{etag}");
        }
        return Ok(None);
    }

    let additional_checksum = calculate_additional_checksum(
        &local_file.path,
        request.checksum_algorithm,
        local_file.size <= part_size,
    )
    .await;
    log::debug!("additional checksum: {additional_checksum:#?}");

    if local_file.size > part_size {
        let etag = upload_multipart(MultipartUploadRequest {
            s3,
            key: &request.key,
            file: &local_file.path,
            file_size: local_file.size,
            chunk_size: part_size,
            sdb: &db,
            acl: request.acl,
            meta: request.meta,
            quiet: request.quiet,
            additional_checksum,
            max_requests: request.number,
            globals,
        })
        .await
        .context("multipart upload failed")?;

        return Ok(Some(etag));
    }

    let etag = upload(UploadRequest {
        s3,
        key: &request.key,
        file: &local_file.path,
        file_size: local_file.size,
        sdb: &db,
        acl: request.acl,
        meta: request.meta,
        quiet: request.quiet,
        additional_checksum,
        globals,
    })
    .await?;

    Ok(Some(etag))
}

fn maybe_write_stream_metadata(
    request: &PutObjectRequest,
    bucket: &str,
    local_file: &LocalFile,
    db: &Db,
    blake3_checksum: &str,
    part_size: u64,
) -> Result<()> {
    if local_file.size <= part_size {
        return Ok(());
    }

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();

    write_metadata(
        &request.s3m_dir,
        &StreamMetadata {
            version: 1,
            id: blake3_checksum.to_string(),
            host: request.host.clone(),
            bucket: bucket.to_string(),
            key: request.key.clone(),
            source_path: local_file.path.clone(),
            checksum: blake3_checksum.to_string(),
            file_size: local_file.size,
            file_mtime: local_file.mtime,
            part_size,
            db_key: db.state_key().to_string(),
            created_at,
            updated_at: Some(created_at),
            pipe: false,
            compress: false,
            encrypt: false,
            mode: StreamMode::FileMultipart,
        },
    )
    .context("could not write stream state metadata")
}

impl PutObjectRequest {
    fn load_local_file(&self) -> Result<LocalFile> {
        let file = self.file.as_ref().ok_or_else(|| {
            anyhow!(
                "Invalid state: neither pipe mode nor source file specified. \
                This indicates a validation bug in dispatch logic."
            )
        })?;

        let metadata = metadata(file).map(|m| {
            if m.is_file() {
                Ok(m)
            } else {
                Err(anyhow!(
                    "cannot read the file: {file}, verify file exist and is not a directory."
                ))
            }
        })??;

        Ok(LocalFile {
            path: PathBuf::from(file),
            size: metadata.len(),
            mtime: metadata
                .modified()
                .unwrap_or_else(|_| SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())?,
        })
    }
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
            Ok(Ok(checksum)) => {
                log::debug!("Additional checksum calculated successfully: {checksum:?}");
                Some(checksum)
            }
            Ok(Err(checksum_error)) => {
                log::warn!(
                    "Failed to calculate additional checksum: {checksum_error}. Upload will continue without it."
                );
                None
            }
            Err(join_error) => {
                log::error!(
                    "Checksum task panicked or failed to join: {join_error}. Upload will continue without additional checksum."
                );
                None
            }
        }
    } else {
        None
    }
}
