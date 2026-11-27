use crate::{
    progressbar::Bar,
    s3::RequestOptions,
    s3::{
        S3, actions,
        checksum::{Checksum, ChecksumAlgorithm},
    },
    stream::{db::Db, iterator::PartIterator, part::Part, retry::backoff_secs, upload_report},
};
use anyhow::{Result, anyhow};
use futures::stream::{FuturesUnordered, StreamExt};
use rkyv::{from_bytes, rancor::Error as RkyvError, to_bytes};
use sled::transaction::{TransactionError, Transactional};
use std::{collections::BTreeMap, path::Path};
use tokio::time::{Duration, sleep};

pub struct MultipartUploadRequest<'a> {
    pub s3: &'a S3,
    pub key: &'a str,
    pub file: &'a Path,
    pub file_size: u64,
    pub chunk_size: u64,
    pub sdb: &'a Db,
    pub acl: Option<String>,
    pub meta: Option<BTreeMap<String, String>>,
    pub quiet: bool,
    pub additional_checksum: Option<Checksum>,
    pub max_requests: u8,
    pub globals: RequestOptions,
}

struct UploadPartRequest<'a> {
    s3: &'a S3,
    key: &'a str,
    file: &'a Path,
    part_number: u16,
    uid: &'a str,
    seek: u64,
    chunk: u64,
    additional_checksum: &'a mut Option<Checksum>,
    globals: RequestOptions,
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
/// # Errors
/// Will return an error if the upload fails
pub async fn upload_multipart(request: MultipartUploadRequest<'_>) -> Result<String> {
    log::debug!(
        "Starting multi part upload:
        key: {}
        file: {}
        file_size: {}
        part size: {}
        acl: {:#?}
        meta: {:#?}
        additional checksum: {:#?}",
        request.key,
        request.file.display(),
        request.file_size,
        request.chunk_size,
        request.acl.as_ref(),
        request.meta.as_ref(),
        request.additional_checksum.as_ref()
    );

    // trees for keeping track of parts to upload
    let db_parts = request.sdb.db_parts()?;
    let db_uploaded = request.sdb.db_uploaded()?;

    let (upload_id, additional_checksum) = start_or_resume_upload(
        request.s3,
        request.sdb,
        actions::CreateMultipartUpload::new(
            request.key,
            request.acl,
            request.meta,
            request.additional_checksum.clone(),
            request.globals.object_lock.clone(),
        ),
        request.additional_checksum,
    )
    .await?;

    log::debug!("upload_id: {upload_id}");

    // if db_parts is not empty it means that a previous upload did not finish successfully.
    // skip creating the parts again and try to re-upload the pending ones. If every part
    // was uploaded but CompleteMultipartUpload failed (S3 can answer 200 OK with an
    // <Error>), only the completion is retried.
    if db_parts.is_empty() && db_uploaded.is_empty() {
        for (number, seek, chunk) in PartIterator::new(request.file_size, request.chunk_size) {
            request
                .sdb
                .create_part(number, seek, chunk, additional_checksum.clone())?;
        }
        db_parts.flush()?;
    }

    // Upload parts progress bar
    let pb = if request.quiet {
        Bar::default()
    } else {
        Bar::new(request.file_size)
    };

    increment_progress_bar(&pb, db_uploaded.len() as u64 * request.chunk_size, None);

    let mut tasks = FuturesUnordered::new();

    log::info!("Max concurrent requests: {}", request.max_requests);

    for part in db_parts
        .iter()
        .values()
        .filter_map(Result::ok)
        .map(|p| from_bytes::<Part, RkyvError>(&p).map_err(anyhow::Error::from))
    {
        let part: Part = part?;

        log::info!("Task push part: {}", part.get_number());

        // spawn task (upload part)
        tasks.push(upload_part(
            request.s3,
            request.key,
            request.file,
            &upload_id,
            request.sdb,
            part,
            &request.globals,
        ));

        await_tasks(
            &mut tasks,
            &pb,
            request.chunk_size,
            request.max_requests.into(),
        )
        .await?;
    }

    // wait for the remaining tasks
    await_remaining_tasks(&mut tasks, &pb, request.chunk_size).await?;

    // finish progress bar
    increment_progress_bar(&pb, 0, Some(true));

    if !db_parts.is_empty() {
        return Err(anyhow!("could not upload all parts"));
    }

    // Complete Multipart Upload
    let uploaded = request.sdb.uploaded_parts()?;
    let action = actions::CompleteMultipartUpload::new(
        request.key,
        &upload_id,
        uploaded,
        additional_checksum,
    );
    let checksum = action.checksum()?;
    let rs = action.request(request.s3).await?;

    // cleanup uploads tree
    db_uploaded.clear()?;

    // save the returned Etag
    request.sdb.save_etag(&rs.e_tag)?;

    // upload finished
    log::info!("Upload finished, ETag: {}", rs.e_tag);

    Ok(upload_report(rs, checksum.as_ref()))
}

/// The upload id to send parts to, and the additional checksum to send them with.
///
/// An upload in progress keeps the algorithm it was created with (or none), whatever
/// this run was given: its parts and its completion must use the same one. Otherwise
/// the multipart upload is initiated, and its id and algorithm are recorded for a
/// resume.
async fn start_or_resume_upload(
    s3: &S3,
    db: &Db,
    create: actions::CreateMultipartUpload<'_>,
    additional_checksum: Option<Checksum>,
) -> Result<(String, Option<Checksum>)> {
    if let Some(upload_id) = db.upload_id()? {
        return Ok((upload_id, resumed_checksum(db, additional_checksum)?));
    }

    let response = create.request(s3).await?;

    // A new upload id starts from nothing. The part trees are shared by every upload of
    // the same content, so parts left by an interrupted upload (another key, or this key
    // before the file was touched) would otherwise be planned or completed again; a stale
    // part in CompleteMultipartUpload fails it with InvalidPart.
    db.db_parts()?.clear()?;
    db.db_uploaded()?.clear()?;
    // Recorded before the upload id, so a resume always finds the algorithm of the
    // upload id it finds.
    db.save_upload_checksum(
        additional_checksum
            .as_ref()
            .map(|checksum| &checksum.algorithm),
    )?;
    // save the upload_id to resume if required
    db.save_upload_id(&response.upload_id)?;
    Ok((response.upload_id, additional_checksum))
}

/// The additional checksum of a multipart upload that is being resumed: the one the
/// upload was created with (see [`Db::resume_checksum`]), whatever this run asked for.
fn resumed_checksum(db: &Db, requested: Option<Checksum>) -> Result<Option<Checksum>> {
    let requested = requested.map(|checksum| checksum.algorithm);
    let used = db.resume_checksum(requested.as_ref())?;
    if requested.is_some() && requested != used {
        log::warn!(
            "Resuming with the checksum the upload was created with ({}), not {}",
            used.as_ref().map_or("none", ChecksumAlgorithm::name),
            requested.as_ref().map_or("none", ChecksumAlgorithm::name)
        );
    }
    Ok(used.map(Checksum::new))
}

#[cfg(test)]
#[path = "multipart_scheduling_tests.rs"]
mod scheduling_tests;

#[cfg(test)]
#[path = "multipart_attempts_tests.rs"]
mod attempts_tests;

async fn await_tasks<T>(
    tasks: &mut FuturesUnordered<T>,
    pb: &Bar,
    chunk_size: u64,
    max_requests: usize,
) -> Result<()>
where
    T: std::future::Future<Output = Result<usize>> + Send,
{
    log::debug!("Running tasks: {}", tasks.len());

    // limit to available parallelism - 2 or 1 (see tools::default_concurrency)
    while tasks.len() >= max_requests {
        if let Some(r) = tasks.next().await {
            r.map_err(|e| anyhow!("{e}"))?;
            increment_progress_bar(pb, chunk_size, None);
        }
    }

    Ok(())
}

// consume remaining tasks
async fn await_remaining_tasks<T>(
    tasks: &mut FuturesUnordered<T>,
    pb: &Bar,
    chunk_size: u64,
) -> Result<()>
where
    T: std::future::Future<Output = Result<usize>> + Send,
{
    log::debug!("Remaining tasks: {}", tasks.len());

    while let Some(r) = tasks.next().await {
        r.map_err(|e| anyhow!("{e}"))?;
        increment_progress_bar(pb, chunk_size, None);
    }
    Ok(())
}

fn increment_progress_bar(pb: &Bar, chunk_size: u64, finish: Option<bool>) {
    if let Some(pb) = pb.progress.as_ref() {
        pb.inc(chunk_size);

        if finish == Some(true) {
            pb.finish();
        }
    }
}

async fn upload_part(
    s3: &S3,
    key: &str,
    file: &Path,
    uid: &str,
    db: &Db,
    part: Part,
    globals: &RequestOptions,
) -> Result<usize> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    let mut additional_checksum = part.get_checksum();

    // do request to get the ETag and update the checksum if required
    let part_number = part.get_number();

    let mut etag: String = String::new();

    // Retry with exponential backoff, capped like streamed parts
    let attempts = globals.retries.max(1);
    for attempt in 1..=attempts {
        let backoff_time = backoff_secs(attempt);
        if attempt > 1 {
            log::warn!("Error uploading part: {part_number}, retrying in {backoff_time} seconds");

            sleep(Duration::from_secs(backoff_time)).await;
        }

        match try_upload_part(UploadPartRequest {
            s3,
            key,
            file,
            part_number,
            uid,
            seek: part.get_seek(),
            chunk: part.get_chunk(),
            additional_checksum: &mut additional_checksum,
            globals: globals.clone(),
        })
        .await
        {
            Ok(e) => {
                etag = e;

                log::info!(
                    "Uploaded part: {}, etag: {}{}",
                    part_number,
                    etag,
                    additional_checksum
                        .as_ref()
                        .map_or_default(|c| format!(" additional_checksum: {}", c.checksum))
                );

                break;
            }

            Err(e) => {
                // Only before any part was accepted: after that, the server does
                // support the checksum, and aborting would discard uploaded parts.
                if let Some(checksum) = additional_checksum
                    .as_ref()
                    .filter(|_| is_not_implemented(&e) && processed.is_empty())
                {
                    return Err(abort_unsupported_checksum(s3, key, uid, db, checksum, &e).await);
                }

                log::error!(
                    "Error uploading part: {}, attempt {}/{} failed: {}",
                    part.get_number(),
                    attempt,
                    attempts,
                    e
                );

                if attempt == attempts {
                    return Err(e);
                }
            }
        }
    }

    // update part with the etag and checksum if any
    let part = part.set_etag(etag).set_checksum(additional_checksum);

    let rkyv_part = to_bytes::<RkyvError>(&part)?;

    // move part to uploaded
    (&unprocessed, &processed)
        .transaction(|(unprocessed, processed)| {
            unprocessed.remove(&part_number.to_be_bytes())?;
            processed.insert(&part_number.to_be_bytes(), rkyv_part.as_slice())?;
            Ok(())
        })
        .map_err(|err| match err {
            TransactionError::Abort(err) | TransactionError::Storage(err) => err,
        })?;

    db.flush()
}

/// A 501 answer with the S3 error code `NotImplemented`: the server doesn't support
/// what the request asked for, so sending it again can't succeed. A bare 501 from
/// something in between (a proxy) is not that.
fn is_not_implemented(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::s3::error::Error>()
        .is_some_and(|error| error.status() == Some(501) && error.code() == Some("NotImplemented"))
}

/// The server rejected the first parts' additional checksum as not implemented
/// (Cloudflare R2 does for SHA-1 and SHA-256). Neither a retry nor a resume can
/// succeed, and the upload in progress would keep its algorithm, so it is aborted and
/// forgotten: the next run starts a new upload. Returns the error to report.
///
/// When the abort fails the upload is still open on the server: the state is kept,
/// and the error says how to abort it.
async fn abort_unsupported_checksum(
    s3: &S3,
    key: &str,
    upload_id: &str,
    db: &Db,
    checksum: &Checksum,
    error: &anyhow::Error,
) -> anyhow::Error {
    let algorithm = checksum.algorithm.name();
    let advice = "Use another --checksum algorithm (Cloudflare R2 accepts crc32 and crc32c on \
                  multipart uploads), or a --buffer larger than the file to upload it as a \
                  single part.";

    let aborted = match actions::AbortMultipartUpload::new(key, upload_id)
        .request(s3)
        .await
    {
        Ok(_) => Ok(()),
        // Already gone: another part's failure aborted it first.
        Err(abort) if abort.code() == Some("NoSuchUpload") => Ok(()),
        Err(abort) => Err(abort),
    };

    match aborted {
        Ok(()) => {
            if let Err(forget) = forget_aborted_upload(db) {
                log::warn!("Could not forget the aborted multipart upload: {forget}");
            }
            anyhow!(
                "the server doesn't support {algorithm} checksums on the parts of a multipart \
                 upload, and the upload was aborted. {advice}\n{error}"
            )
        }
        Err(abort) => anyhow!(
            "the server doesn't support {algorithm} checksums on the parts of a multipart \
             upload. The multipart upload could not be aborted ({abort}); abort it with \
             `s3m rm <host>/<bucket>/{key} --abort {upload_id}`. {advice}\n{error}"
        ),
    }
}

/// Forget an upload the server no longer has, with the parts planned for it.
fn forget_aborted_upload(db: &Db) -> Result<()> {
    db.forget_upload()?;
    db.db_parts()?.clear()?;
    db.db_uploaded()?.clear()?;
    Ok(())
}

async fn try_upload_part(request: UploadPartRequest<'_>) -> Result<String> {
    let action = actions::UploadPart::new(
        request.key,
        request.file,
        request.part_number,
        request.uid,
        request.seek,
        request.chunk,
        request.additional_checksum.as_mut(),
    );

    log::debug!("Uploading part: {}", request.part_number);

    Ok(action.request(request.s3, &request.globals).await?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{MultipartUploadRequest, upload_multipart};
    use crate::{
        s3::{
            Credentials, Region, RequestOptions, S3,
            checksum::{Checksum, ChecksumAlgorithm},
        },
        stream::{db::Db, part::Part},
    };
    use mockito::{Matcher, Server};
    use rkyv::{rancor::Error as RkyvError, to_bytes};
    use secrecy::SecretString;
    use std::io::Write;

    // Regression: a fresh upload id kept the parts another upload of the same content had
    // already sent, and CompleteMultipartUpload listed them (S3 answers InvalidPart).
    #[tokio::test]
    async fn new_upload_does_not_complete_with_stale_uploaded_parts() {
        let mut server = Server::new_async().await;
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let stale = Part::new(3, 10, 5, None).set_etag("\"STALE\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                3_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&stale).unwrap().as_slice(),
            )
            .unwrap();

        let _initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>new-id</UploadId></InitiateMultipartUploadResult>")
            .create_async()
            .await;
        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), "new-id".into()),
                Matcher::Regex("partNumber=[12]".into()),
            ]))
            .with_header("ETag", "\"fresh\"")
            .expect(2)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "new-id".into()))
            .with_body_from_request(|request| {
                let body = String::from_utf8_lossy(request.body().unwrap()).to_string();
                if body.contains("STALE") {
                    b"stale part sent".to_vec()
                } else {
                    b"<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>".to_vec()
                }
            })
            .create_async()
            .await;

        let result = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await;

        parts.assert_async().await;
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-2\"");
    }

    // When every part was uploaded but CompleteMultipartUpload failed (for example a
    // 200 OK with an <Error> document), the next run only retries the completion.
    #[tokio::test]
    async fn completion_retry_does_not_upload_parts_again() {
        let mut server = Server::new_async().await;
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        );
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        for (number, seek) in [(1_u16, 0), (2, 5)] {
            let part = Part::new(number, seek, 5, None).set_etag(format!("\"p{number}\""));
            db.db_uploaded()
                .unwrap()
                .insert(
                    number.to_be_bytes(),
                    to_bytes::<RkyvError>(&part).unwrap().as_slice(),
                )
                .unwrap();
        }

        let parts = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex(
                "<ETag>\"p1\"</ETag><PartNumber>1</PartNumber>.*<ETag>\"p2\"</ETag><PartNumber>2</PartNumber>".into(),
            ))
            .with_body("<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;

        let result = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await;

        parts.assert_async().await;
        complete.assert_async().await;
        assert_eq!(result.unwrap(), "ETag: \"done-2\"");
    }

    const SHA256_PART_1: &str = "xWX+A8qbYkLgHf3e/pu6PZiycOGc0C/YXOr3XislvxI=";
    const SHA256_PART_2: &str = "92BDp07DO2rvuyiQUPr3qo1IIJVHc5fj5jNFEl1J9Sc=";
    // base64(sha256(sha256("01234") || sha256("56789")))-2
    const SHA256_COMPOSITE: &str = "N7HU4sCX924sGwPEjjVrehEoVZGWFs9+jec8UebzU+k=-2";

    fn sha256(checksum: &str) -> Checksum {
        Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            checksum: checksum.to_string(),
        }
    }

    fn checksum_s3(server: &Server) -> S3 {
        S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        )
    }

    async fn resume_without_checksum(s3: &S3, db: &Db, file: &std::path::Path) -> String {
        upload_multipart(MultipartUploadRequest {
            s3,
            key: "key",
            file,
            file_size: 10,
            chunk_size: 5,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: None,
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await
        .unwrap()
    }

    // <https://github.com/s3m/s3m/issues/77>: `streams resume` doesn't know the
    // --checksum of the original upload. A state written before the algorithm was
    // saved still has it on its parts: the pending part is sent with its checksum, and
    // the report has the checksum line like the original upload.
    #[tokio::test]
    async fn resume_recovers_the_checksum_algorithm_from_the_parts() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 5, Some(sha256(SHA256_PART_1))).set_etag("\"p1\"".into());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        db.create_part(2, 5, 5, Some(Checksum::new(ChecksumAlgorithm::Sha256)))
            .unwrap();

        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", SHA256_PART_2)
            .match_header("content-md5", Matcher::Missing)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex(format!(
                "<ChecksumSHA256>{}</ChecksumSHA256>.*<ChecksumSHA256>{}</ChecksumSHA256>",
                regex_escape(SHA256_PART_1),
                regex_escape(SHA256_PART_2)
            )))
            .with_body("<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;

        let report = resume_without_checksum(&s3, &db, file.path()).await;

        part.assert_async().await;
        complete.assert_async().await;
        assert_eq!(
            report,
            format!("ETag: \"done-2\"\nsha256 (base64): {SHA256_COMPOSITE}")
        );
    }

    fn regex_escape(text: &str) -> String {
        text.replace('+', "\\+")
    }

    const COMPLETED: &str = "<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>";

    // #77: the upload id is saved but s3m stopped before recording the parts. The
    // resume creates them with the algorithm the upload was created with; without it
    // the parts would carry Content-MD5 and S3 would reject the completion.
    #[tokio::test]
    async fn resume_creates_parts_with_the_saved_checksum_algorithm() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_checksum(Some(&ChecksumAlgorithm::Sha256))
            .unwrap();
        db.save_upload_id("uid").unwrap();

        let mut parts = Vec::new();
        for (number, checksum) in [("1", SHA256_PART_1), ("2", SHA256_PART_2)] {
            parts.push(
                server
                    .mock("PUT", "/bucket/key")
                    .match_query(Matcher::UrlEncoded("partNumber".into(), number.into()))
                    .match_header("x-amz-checksum-sha256", checksum)
                    .match_header("content-md5", Matcher::Missing)
                    .with_header("etag", format!("\"p{number}\"").as_str())
                    .expect(1)
                    .create_async()
                    .await,
            );
        }
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .match_body(Matcher::Regex("<ChecksumSHA256>".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = resume_without_checksum(&s3, &db, file.path()).await;

        for part in parts {
            part.assert_async().await;
        }
        complete.assert_async().await;
        assert_eq!(
            report,
            format!("ETag: \"done-2\"\nsha256 (base64): {SHA256_COMPOSITE}")
        );
    }

    // <https://github.com/s3m/s3m/issues/80>: Cloudflare R2 answers 501 NotImplemented
    // to a part that carries a SHA checksum. No retry can succeed, and neither can a
    // resume: the upload is aborted and forgotten, and the error says what to do.
    #[tokio::test]
    async fn part_checksum_not_implemented_aborts_the_upload_without_retrying() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();

        let create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .match_header("x-amz-checksum-algorithm", "SHA256")
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body(r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NotImplemented</Code><Message>Header 'x-amz-checksum-sha256' with value 'xWX+A8qbYkLgHf3e/pu6PZiycOGc0C/YXOr3XislvxI=' not implemented</Message></Error>"#)
            .expect(1)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions {
                retries: 3,
                ..RequestOptions::default()
            },
        })
        .await
        .unwrap_err()
        .to_string();

        create.assert_async().await;
        part.assert_async().await;
        abort.assert_async().await;
        assert!(
            error.contains("doesn't support sha256 checksums on the parts of a multipart upload"),
            "{error}"
        );
        assert!(error.contains("and the upload was aborted"), "{error}");
        assert!(
            error.contains("Use another --checksum algorithm"),
            "{error}"
        );
        assert_eq!(db.upload_id().unwrap(), None);
        assert_eq!(db.upload_checksum().unwrap(), None);
    }

    // #77 review: a state from an older s3m has no record, and its parts were sent
    // without a checksum. Running the command again with --checksum must not change
    // what the pending part carries, nor print a checksum the server doesn't have.
    #[tokio::test]
    async fn resume_of_an_old_state_keeps_parts_without_a_checksum() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();

        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("uid").unwrap();
        let uploaded = Part::new(1, 0, 5, None).set_etag("\"p1\"".into());
        db.db_uploaded()
            .unwrap()
            .insert(
                1_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&uploaded).unwrap().as_slice(),
            )
            .unwrap();
        db.create_part(2, 5, 5, None).unwrap();

        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .match_header("content-md5", Matcher::Any)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = upload_multipart(MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions::default(),
        })
        .await
        .unwrap();

        part.assert_async().await;
        complete.assert_async().await;
        assert_eq!(report, "ETag: \"done-2\"");
    }

    const SHA256_NOT_IMPLEMENTED: &str = r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NotImplemented</Code><Message>Header 'x-amz-checksum-sha256' not implemented</Message></Error>"#;
    const INITIATED: &str = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>";

    fn sha256_upload<'a>(
        s3: &'a S3,
        db: &'a Db,
        file: &'a std::path::Path,
    ) -> MultipartUploadRequest<'a> {
        MultipartUploadRequest {
            s3,
            key: "key",
            file,
            file_size: 10,
            chunk_size: 5,
            sdb: db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: Some(Checksum::new(ChecksumAlgorithm::Sha256)),
            max_requests: 1,
            globals: RequestOptions {
                retries: 1,
                ..RequestOptions::default()
            },
        }
    }

    // #80 review: when the abort itself fails, the upload is still open on the
    // server. The error must not claim it was aborted, and the state keeps the upload
    // id so it can still be found and aborted.
    #[tokio::test]
    async fn part_checksum_not_implemented_reports_a_failed_abort() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let _part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            .expect(1)
            .create_async()
            .await;

        let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        abort.assert_async().await;
        assert!(!error.contains("the upload was aborted"), "{error}");
        assert!(error.contains("could not be aborted"), "{error}");
        assert!(error.contains("--abort uid"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
    }

    // #80 review: a 501 after a part was accepted with its checksum is not "the
    // server doesn't support this checksum". The parts already uploaded are kept,
    // and nothing is aborted.
    #[tokio::test]
    async fn not_implemented_after_an_uploaded_part_keeps_the_upload() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        let _create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .with_body(INITIATED)
            .create_async()
            .await;
        let part_1 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
            .match_header("x-amz-checksum-sha256", SHA256_PART_1)
            .with_header("etag", "\"p1\"")
            .expect(1)
            .create_async()
            .await;
        let _part_2 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .with_status(501)
            .with_body(SHA256_NOT_IMPLEMENTED)
            .create_async()
            .await;
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let error = upload_multipart(sha256_upload(&s3, &db, file.path()))
            .await
            .unwrap_err()
            .to_string();

        part_1.assert_async().await;
        abort.assert_async().await;
        assert!(!error.contains("aborted"), "{error}");
        assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid"));
        assert_eq!(db.uploaded_parts().unwrap().len(), 1);
    }

    // A new upload records its algorithm (or that it has none), and a resume keeps
    // that choice even when it is run with another --checksum: the server's upload
    // was created without one, so its parts can't carry one.
    #[tokio::test]
    async fn upload_records_its_checksum_algorithm_and_resume_keeps_it() {
        let mut server = Server::new_async().await;
        let s3 = checksum_s3(&server);
        let dir = tempfile::tempdir().unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"0123456789").unwrap();
        let db = Db::new(&s3, "key", "checksum", 1, dir.path()).unwrap();
        assert_eq!(db.upload_checksum().unwrap(), None);

        // First run: created without a checksum, part 2 fails.
        let create = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".into()))
            .match_header("x-amz-checksum-algorithm", Matcher::Missing)
            .with_body("<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>uid</UploadId></InitiateMultipartUploadResult>")
            .expect(1)
            .create_async()
            .await;
        let part_1 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .with_header("etag", "\"p1\"")
            .expect(1)
            .create_async()
            .await;
        let part_2_fails = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            .create_async()
            .await;
        let request = |checksum: Option<Checksum>| MultipartUploadRequest {
            s3: &s3,
            key: "key",
            file: file.path(),
            file_size: 10,
            chunk_size: 5,
            sdb: &db,
            acl: None,
            meta: None,
            quiet: true,
            additional_checksum: checksum,
            max_requests: 1,
            globals: RequestOptions {
                retries: 1,
                ..RequestOptions::default()
            },
        };
        assert!(upload_multipart(request(None)).await.is_err());
        create.assert_async().await;
        part_1.assert_async().await;
        assert_eq!(db.upload_checksum().unwrap(), Some(None));
        part_2_fails.remove_async().await;

        // Second run asks for sha256: the pending part still goes without it.
        let part_2 = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
            .match_header("x-amz-checksum-sha256", Matcher::Missing)
            .match_header("content-md5", Matcher::Any)
            .with_header("etag", "\"p2\"")
            .expect(1)
            .create_async()
            .await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "uid".into()))
            .with_body(COMPLETED)
            .expect(1)
            .create_async()
            .await;

        let report = upload_multipart(request(Some(Checksum::new(ChecksumAlgorithm::Sha256))))
            .await
            .unwrap();

        part_2.assert_async().await;
        complete.assert_async().await;
        assert_eq!(report, "ETag: \"done-2\"");
    }
}
