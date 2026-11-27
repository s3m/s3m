use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
    s3::{
        S3, actions,
        checksum::{Checksum, ChecksumAlgorithm},
        limits::MAX_OBJECT_SIZE_BYTES,
        tools,
    },
    stream::{
        FileStreamUpload, StreamAbortFailed,
        db::Db,
        state::{StreamMetadata, StreamMode, load_metadata, write_metadata},
        upload_compressed::stream_compressed,
        upload_compressed_encrypted::stream_compressed_encrypted,
        upload_default::{UploadRequest, upload},
        upload_encrypted::stream_encrypted,
        upload_multipart::{MultipartUploadRequest, upload_multipart},
        upload_stdin::stream_stdin,
        upload_stdin_compressed::stream_stdin_compressed,
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
        let has_object_lock = globals.object_lock.is_some();
        return handle_put_object(
            s3,
            PutObjectRequest {
                acl,
                meta,
                buf_size,
                file,
                host: host.clone(),
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
        .await
        .map_err(|mut error| {
            if let Some(abort) = error.downcast_mut::<StreamAbortFailed>() {
                abort.set_host_alias(host);
            }
            augment_object_lock_error(error, has_object_lock)
        });
    }

    Ok(())
}

/// When an upload carrying Object Lock headers is rejected with `InvalidRequest`,
/// the usual cause is a bucket that was not created with Object Lock enabled.
/// Attach an actionable hint instead of forcing a pre-flight check on every upload.
fn augment_object_lock_error(err: anyhow::Error, has_object_lock: bool) -> anyhow::Error {
    let missing_lock_config = has_object_lock
        && err
            .downcast_ref::<crate::s3::Error>()
            .is_some_and(|e| e.code() == Some("InvalidRequest"));

    if missing_lock_config {
        err.context(
            "the target bucket is not Object-Lock-enabled; create it with `s3m cb --object-lock`",
        )
    } else {
        err
    }
}

async fn handle_put_object(s3: &S3, request: PutObjectRequest, globals: GlobalArgs) -> Result<()> {
    let quiet = request.quiet;

    if request.pipe {
        let report = handle_pipe_upload(s3, request, globals).await?;
        print_report(&report, quiet);
        return Ok(());
    }

    let local_file = request.load_local_file()?;
    let report = handle_file_upload(s3, request, globals, local_file).await?;
    print_report(&report, quiet);

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
        (true, false) => {
            log::info!("COMPRESS - streaming compressed data from stdin");
            stream_stdin_compressed(s3, &key, acl, meta, quiet, tmp_dir, globals).await
        }
        (false, true) => {
            log::info!("ENCRYPT - streaming encrypted data from stdin");
            stream_stdin_encrypted(s3, &key, acl, meta, quiet, tmp_dir, globals).await
        }
        (false, false) => stream_stdin(s3, &key, acl, meta, quiet, tmp_dir, globals).await,
    }
}

async fn handle_file_upload(
    s3: &S3,
    request: PutObjectRequest,
    globals: GlobalArgs,
    local_file: LocalFile,
) -> Result<String> {
    let file_path = local_file.path.as_path();

    // A compressed or encrypted upload is streamed in transformed parts and sends no
    // additional checksum. The --compress flag conflicts with --checksum already; this
    // covers a host that compresses or encrypts through its config.
    if request.checksum_algorithm.is_some() && (globals.compress || globals.encrypt) {
        return Err(anyhow!(
            "--checksum can't be used with compression or encryption: the upload is streamed \
             and sends no additional checksum. Upload without --checksum, or to a host that \
             doesn't compress or encrypt"
        ));
    }

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
        }
        (false, false) => handle_standard_file_upload(s3, request, globals, local_file).await,
    }
}

async fn handle_standard_file_upload(
    s3: &S3,
    request: PutObjectRequest,
    globals: GlobalArgs,
    local_file: LocalFile,
) -> Result<String> {
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
    log::info!("blake3: {blake3_checksum}");

    let db = Db::new(
        s3,
        &request.key,
        &blake3_checksum,
        local_file.mtime,
        &request.s3m_dir,
    )
    .context("could not open upload state")?;

    maybe_write_stream_metadata(
        &request,
        s3.bucket().unwrap_or_default(),
        &local_file,
        &db,
        &blake3_checksum,
        part_size,
    )?;

    if let Some(etag) = cached_upload_still_current(s3, &request.key, &db).await? {
        // The object is current, but the multipart upload it replaced may still be
        // open: retry its abort.
        if db.abort_pending()? {
            abort_replaced_multipart_upload(s3, &request.host, &request.key, &db).await;
        }
        return Ok(etag_report(&etag));
    }

    let requested_checksum = request.checksum_algorithm.clone();
    let additional_checksum = calculate_additional_checksum(
        &local_file.path,
        request.checksum_algorithm,
        local_file.size <= part_size,
    )
    .await;
    log::debug!("additional checksum: {additional_checksum:#?}");

    if local_file.size > part_size {
        warn_if_resume_keeps_another_checksum(&db, requested_checksum.as_deref())?;
        let etag = upload_multipart(MultipartUploadRequest {
            s3,
            key: &request.key,
            file: &local_file.path,
            file_size: local_file.size,
            file_mtime: Some(local_file.mtime),
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

        return Ok(etag);
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

    abort_replaced_multipart_upload(s3, &request.host, &request.key, &db).await;

    Ok(etag)
}

async fn abort_replaced_multipart_upload(s3: &S3, host: &str, key: &str, db: &Db) {
    let upload_id = match db.upload_id() {
        Ok(Some(upload_id)) => upload_id,
        Ok(None) => return,
        Err(error) => {
            eprintln!("warning: could not read the previous multipart upload id ({error})");
            return;
        }
    };
    let abort = actions::AbortMultipartUpload::new(key, &upload_id);
    if let Err(error) = abort.request_if_open(s3).await {
        let command = abort.command(host, s3.bucket().unwrap_or_default());
        // The object is stored, but the multipart upload stays open. The mark keeps the
        // state from counting as complete (`streams clean` keeps it), and the next run
        // retries the abort without uploading the file again.
        let kept = match db.mark_abort_pending() {
            Ok(()) => "the next run retries the abort",
            Err(_) => "its state could not be updated",
        };
        eprintln!(
            "warning: could not abort the previous multipart upload ({error}); {kept}. Abort it with `{command}`"
        );
    } else if let Err(error) = db.forget_multipart_upload() {
        eprintln!(
            "warning: could not clear the state of aborted multipart upload {upload_id} ({error})"
        );
    }
}

/// The local state only records that this file was once uploaded to this key. Before
/// skipping the upload, ask the server: the object may have been deleted or replaced since.
/// Only a matching `ETag` skips it. A missing object, another `ETag`, or a `HEAD` that fails
/// (for example credentials allowed to write but not to read) forgets the cached upload so
/// the file is uploaded again.
async fn cached_upload_still_current(s3: &S3, key: &str, db: &Db) -> Result<Option<String>> {
    let Some(cached) = db.cached_etag()? else {
        return Ok(None);
    };

    match actions::HeadObject::new(key, None).request(s3).await {
        Ok(headers) => match headers.get("etag") {
            Some(remote) if same_etag(remote, &cached) => {
                log::info!("{key} is already uploaded with ETag {cached}, skipping");
                return Ok(Some(cached));
            }
            remote => log::info!(
                "{key} changed on the server (ETag {remote:?}, uploaded {cached}), uploading again"
            ),
        },
        Err(error) => {
            log::info!("could not confirm the earlier upload of {key} ({error}), uploading again");
        }
    }

    // The object changed: forget its ETag. A multipart upload whose abort is still
    // pending is kept, so that it is aborted after this run's upload.
    if db.abort_pending()? {
        db.forget_etag()?;
    } else {
        db.forget_upload()?;
    }
    Ok(None)
}

/// `ETag`s compare without their quotes or a weak `W/` prefix: `CompleteMultipartUpload`
/// and `HEAD` do not always quote them the same way.
fn same_etag(a: &str, b: &str) -> bool {
    fn bare(etag: &str) -> &str {
        etag.trim().trim_start_matches("W/").trim_matches('"')
    }
    let a = bare(a);
    !a.is_empty() && a == bare(b)
}

fn maybe_write_stream_metadata(
    request: &PutObjectRequest,
    bucket: &str,
    local_file: &LocalFile,
    db: &Db,
    blake3_checksum: &str,
    part_size: u64,
) -> Result<()> {
    let id = db
        .path()
        .file_name()
        .and_then(|id| id.to_str())
        .context("invalid stream state directory name")?;
    // Only the legacy file-hash directory needs to identify its owner during a
    // single PUT. Keep matching multipart metadata, including its original part size.
    if local_file.size <= part_size
        && (id != blake3_checksum
            || load_metadata(db.path()).is_ok_and(|m| m.db_key == db.state_key()))
    {
        return Ok(());
    }

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_default(|duration| duration.as_secs());

    write_metadata(
        &request.s3m_dir,
        &StreamMetadata {
            version: 1,
            id: id.to_owned(),
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

/// A multipart upload in progress keeps the additional checksum it was created with:
/// the server's upload is bound to it. When this run asks for another one, say so,
/// because the checksum the user asked for is not the one the object gets.
fn warn_if_resume_keeps_another_checksum(db: &Db, requested: Option<&str>) -> Result<()> {
    let Some(requested) = requested.and_then(|name| name.parse::<ChecksumAlgorithm>().ok()) else {
        return Ok(());
    };
    if db.upload_id()?.is_none() {
        return Ok(());
    }
    let used = db.resume_checksum(Some(&requested))?;
    if used.as_ref() != Some(&requested) {
        eprintln!(
            "warning: resuming the upload in progress with the checksum it was created with ({}), not {}",
            used.as_ref().map_or("none", ChecksumAlgorithm::name),
            requested.name()
        );
    }
    Ok(())
}

/// The report of an upload that was skipped because the object is already there: only
/// its cached `ETag` is known.
fn etag_report(etag: &str) -> String {
    format!("ETag: {etag}")
}

/// Print the upload's report unless in quiet mode
fn print_report(report: &str, quiet: bool) {
    if !quiet {
        println!("{}", report.trim_end());
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
        println!("blake3: {checksum}");
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{augment_object_lock_error, cached_upload_still_current, same_etag};
    use crate::{
        s3::{ApiError, Credentials, Error, Region, S3},
        stream::db::Db,
    };
    use mockito::Server;
    use secrecy::SecretString;
    use tempfile::TempDir;

    fn mock_s3(endpoint: &str) -> S3 {
        S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", endpoint),
            Some("bucket".to_string()),
            false,
        )
    }

    #[tokio::test]
    async fn multipart_put_rejects_a_same_size_source_rewrite_after_stat() {
        use super::{GlobalArgs, PutObjectRequest, handle_standard_file_upload};
        use std::{
            fs::File,
            io::Write as _,
            time::{Duration, UNIX_EPOCH},
        };

        const PART_SIZE: usize = 5 * 1024 * 1024;
        let mut server = Server::new_async().await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source.bin");
        let mut file = File::create(&source).unwrap();
        file.write_all(&vec![0; PART_SIZE + 1]).unwrap();
        file.set_modified(UNIX_EPOCH + Duration::from_secs(10))
            .unwrap();
        let request = PutObjectRequest {
            acl: None,
            meta: None,
            buf_size: PART_SIZE,
            file: Some(source.to_str().unwrap().to_owned()),
            host: "s3".into(),
            key: "key".into(),
            pipe: false,
            s3m_dir: dir.path().to_owned(),
            quiet: true,
            tmp_dir: dir.path().to_owned(),
            checksum_algorithm: None,
            number: 1,
        };
        let local_file = request.load_local_file().unwrap();
        drop(file);
        let mut replacement = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        replacement.write_all(&vec![1; PART_SIZE + 1]).unwrap();
        replacement
            .as_file()
            .set_modified(UNIX_EPOCH + Duration::from_secs(20))
            .unwrap();
        replacement.persist(&source).unwrap();

        let initiate = server
            .mock("POST", "/bucket/key")
            .match_query(mockito::Matcher::Any)
            .with_status(500)
            .expect(0)
            .create_async()
            .await;
        let error = handle_standard_file_upload(&s3, request, GlobalArgs::new(), local_file)
            .await
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("modification time changed since it was read"),
            "multipart PUT must reject a same-size rewrite after stat: {error:#}"
        );
        initiate.assert_async().await;
    }

    /// A state db that already holds a completed multipart upload of this file to `key`.
    fn completed_upload(s3: &S3, dir: &TempDir, etag: &str) -> Db {
        let db = Db::new(s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("completed-upload-id").unwrap();
        db.save_etag(etag).unwrap();
        db
    }

    #[test]
    fn test_same_etag_ignores_quotes_and_weak_prefix() {
        assert!(same_etag("\"abc-2\"", "abc-2"));
        assert!(same_etag("W/\"abc\"", "\"abc\""));
        assert!(!same_etag("\"abc\"", "\"abd\""));
        assert!(!same_etag("\"\"", ""));
    }

    #[tokio::test]
    async fn test_cache_hit_skips_when_server_has_same_etag() {
        let mut server = Server::new_async().await;
        let head = server
            .mock("HEAD", "/bucket/key")
            .with_status(200)
            .with_header("etag", "\"abc-2\"")
            .create_async()
            .await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let db = completed_upload(&s3, &dir, "\"abc-2\"");

        let skipped = cached_upload_still_current(&s3, "key", &db).await.unwrap();

        head.assert_async().await;
        assert_eq!(skipped, Some("\"abc-2\"".to_string()));
        assert_eq!(
            db.upload_id().unwrap().as_deref(),
            Some("completed-upload-id")
        );
    }

    // Regression: a cached ETag used to report success with no request at all, even when
    // the object had been deleted on the server.
    #[tokio::test]
    async fn test_cache_hit_uploads_again_when_object_is_gone() {
        let mut server = Server::new_async().await;
        let head = server
            .mock("HEAD", "/bucket/key")
            .with_status(404)
            .create_async()
            .await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let db = completed_upload(&s3, &dir, "\"abc-2\"");

        let skipped = cached_upload_still_current(&s3, "key", &db).await.unwrap();

        head.assert_async().await;
        assert_eq!(skipped, None);
        assert_eq!(db.check().unwrap(), None);
        // The completed upload id must go too, or the re-upload resumes a finished upload.
        assert_eq!(db.upload_id().unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_hit_uploads_again_when_object_changed() {
        let mut server = Server::new_async().await;
        let _head = server
            .mock("HEAD", "/bucket/key")
            .with_status(200)
            .with_header("etag", "\"other\"")
            .create_async()
            .await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let db = completed_upload(&s3, &dir, "\"abc-2\"");

        assert_eq!(
            cached_upload_still_current(&s3, "key", &db).await.unwrap(),
            None
        );
        assert_eq!(db.cached_etag().unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_hit_uploads_again_when_head_is_forbidden() {
        let mut server = Server::new_async().await;
        let _head = server
            .mock("HEAD", "/bucket/key")
            .with_status(403)
            .create_async()
            .await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let db = completed_upload(&s3, &dir, "\"abc-2\"");

        assert_eq!(
            cached_upload_still_current(&s3, "key", &db).await.unwrap(),
            None
        );
        assert_eq!(db.cached_etag().unwrap(), None);
    }

    #[tokio::test]
    async fn test_no_cached_upload_sends_no_request() {
        let mut server = Server::new_async().await;
        let head = server
            .mock("HEAD", "/bucket/key")
            .expect(0)
            .create_async()
            .await;
        let s3 = mock_s3(&server.url());
        let dir = TempDir::new().unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();

        assert_eq!(
            cached_upload_still_current(&s3, "key", &db).await.unwrap(),
            None
        );
        head.assert_async().await;
    }

    fn api_error(code: &str) -> anyhow::Error {
        anyhow::Error::new(Error::Api(ApiError {
            status: 400,
            code: Some(code.to_string()),
            message: None,
            details: String::new(),
        }))
    }

    #[test]
    fn test_hint_added_for_invalid_request_with_object_lock() {
        let err = augment_object_lock_error(api_error("InvalidRequest"), true);
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("cb --object-lock") && rendered.contains("not Object-Lock-enabled"),
            "expected the Object Lock hint, got: {rendered}"
        );
    }

    #[test]
    fn test_no_hint_when_object_lock_not_requested() {
        let err = augment_object_lock_error(api_error("InvalidRequest"), false);
        assert!(!format!("{err:#}").contains("cb --object-lock"));
    }

    #[test]
    fn test_no_hint_for_other_error_codes() {
        let err = augment_object_lock_error(api_error("AccessDenied"), true);
        assert!(!format!("{err:#}").contains("cb --object-lock"));
    }
}
