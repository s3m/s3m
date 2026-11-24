use crate::{
    progressbar::Bar,
    s3::RequestOptions,
    s3::{S3, actions, checksum::Checksum},
    stream::{db::Db, iterator::PartIterator, part::Part, retry::backoff_secs},
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

    let upload_id = if let Some(uid) = request.sdb.upload_id()? {
        uid
    } else {
        // Initiate Multipart Upload - request an Upload ID
        let action = actions::CreateMultipartUpload::new(
            request.key,
            request.acl,
            request.meta,
            request.additional_checksum.clone(),
            request.globals.object_lock.clone(),
        );

        let response = action.request(request.s3).await?;

        // A new upload id starts from nothing. The part trees are shared by every upload of
        // the same content, so parts left by an interrupted upload (another key, or this key
        // before the file was touched) would otherwise be planned or completed again; a stale
        // part in CompleteMultipartUpload fails it with InvalidPart.
        db_parts.clear()?;
        db_uploaded.clear()?;
        // save the upload_id to resume if required
        request.sdb.save_upload_id(&response.upload_id)?;
        response.upload_id
    };

    log::debug!("upload_id: {upload_id}");

    // if db_parts is not empty it means that a previous upload did not finish successfully.
    // skip creating the parts again and try to re-upload the pending ones
    if db_parts.is_empty() {
        for (number, seek, chunk) in PartIterator::new(request.file_size, request.chunk_size) {
            request
                .sdb
                .create_part(number, seek, chunk, request.additional_checksum.clone())?;
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
        request.additional_checksum,
    );
    let rs = action.request(request.s3).await?;

    // cleanup uploads tree
    db_uploaded.clear()?;

    // save the returned Etag
    request.sdb.save_etag(&rs.e_tag)?;

    // upload finished
    log::info!("Upload finished, ETag: {}", rs.e_tag);

    Ok(format!("ETag: {}", rs.e_tag))
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
        s3::{Credentials, Region, RequestOptions, S3},
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
}
