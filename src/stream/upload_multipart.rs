use crate::{
    cli::{globals::GlobalArgs, progressbar::Bar},
    s3::{actions, checksum::Checksum, S3},
    stream::{db::Db, iterator::PartIterator, part::Part},
};
use anyhow::{anyhow, Result};
use bincode::{decode_from_slice, encode_to_vec};
use futures::stream::{FuturesUnordered, StreamExt};
use sled::transaction::{TransactionError, Transactional};
use std::{collections::BTreeMap, path::Path};
use tokio::time::{sleep, Duration};

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
/// # Errors
/// Will return an error if the upload fails
#[allow(clippy::too_many_arguments)]
pub async fn upload_multipart(
    s3: &S3,
    key: &str,
    file: &Path,
    file_size: u64,
    chunk_size: u64,
    sdb: &Db,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
    additional_checksum: Option<Checksum>,
    max_requests: u8,
    globals: GlobalArgs,
) -> Result<String> {
    log::debug!(
        "Starting multi part upload:
        key: {key}
        file: {}
        file_size: {file_size}
        part size: {chunk_size}
        acl: {:#?}
        meta: {:#?}
        additional checksum: {:#?}",
        file.display(),
        acl,
        meta,
        additional_checksum
    );

    // trees for keeping track of parts to upload
    let db_parts = sdb.db_parts()?;
    let db_uploaded = sdb.db_uploaded()?;

    let upload_id = if let Some(uid) = sdb.upload_id()? {
        uid
    } else {
        // Initiate Multipart Upload - request an Upload ID
        let action =
            actions::CreateMultipartUpload::new(key, acl, meta, additional_checksum.clone());

        let response = action.request(s3).await?;

        db_parts.clear()?;
        // save the upload_id to resume if required
        sdb.save_upload_id(&response.upload_id)?;
        response.upload_id
    };

    log::debug!("upload_id: {}", &upload_id);

    // if db_parts is not empty it means that a previous upload did not finish successfully.
    // skip creating the parts again and try to re-upload the pending ones
    if db_parts.is_empty() {
        for (number, seek, chunk) in PartIterator::new(file_size, chunk_size) {
            sdb.create_part(number, seek, chunk, additional_checksum.clone())?;
        }
        db_parts.flush()?;
    }

    // Upload parts progress bar
    let pb = Bar::new(file_size, Some(quiet));

    increment_progress_bar(&pb, db_uploaded.len() as u64 * chunk_size, None);

    let mut tasks = FuturesUnordered::new();

    log::info!("Max concurrent requests: {}", max_requests);

    for part in db_parts.iter().values().filter_map(Result::ok).map(|p| {
        decode_from_slice(&p[..], bincode::config::standard())
            .map(|(decoded, _)| decoded)
            .map_err(anyhow::Error::from)
    }) {
        let part: Part = part?;

        log::info!("Task push part: {}", part.get_number());

        // spawn task (upload part)
        tasks.push(upload_part(s3, key, file, &upload_id, sdb, part, &globals));

        await_tasks(&mut tasks, &pb, chunk_size, max_requests.into()).await?;
    }

    // wait for the remaining tasks
    await_remaining_tasks(&mut tasks, &pb, chunk_size).await?;

    // finish progress bar
    increment_progress_bar(&pb, 0, Some(true));

    if !db_parts.is_empty() {
        return Err(anyhow!("could not upload all parts"));
    }

    // Complete Multipart Upload
    let uploaded = sdb.uploaded_parts()?;
    let action =
        actions::CompleteMultipartUpload::new(key, &upload_id, uploaded, additional_checksum);
    let rs = action.request(s3).await?;

    // cleanup uploads tree
    db_uploaded.clear()?;

    // save the returned Etag
    sdb.save_etag(&rs.e_tag)?;

    // upload finished
    log::info!("Upload finished, ETag: {}", rs.e_tag);

    Ok(format!("ETag: {}", rs.e_tag))
}

// throttling tasks
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

    // limit to num_cpus - 2 or 1
    while tasks.len() >= max_requests {
        if let Some(r) = tasks.next().await {
            r.map_err(|e| anyhow!("{}", e))?;
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
        r.map_err(|e| anyhow!("{}", e))?;
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
    globals: &GlobalArgs,
) -> Result<usize> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    let mut additional_checksum = part.get_checksum();

    // do request to get the ETag and update the checksum if required
    let part_number = part.get_number();

    let mut etag: String = String::new();

    // Retry with exponential backoff
    for attempt in 1..=globals.retries {
        let backoff_time = 2u64.pow(attempt - 1);
        if attempt > 1 {
            log::warn!(
                "Error uploading part: {}, retrying in {} seconds",
                part_number,
                backoff_time
            );

            sleep(Duration::from_secs(backoff_time)).await;
        }

        match try_upload_part(
            s3,
            key,
            file,
            part_number,
            uid,
            part.get_seek(),
            part.get_chunk(),
            &mut additional_checksum,
            globals.clone(),
        )
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
                        .map(|c| format!(" additional_checksum: {}", c.checksum))
                        .unwrap_or_default()
                );

                break;
            }

            Err(e) => {
                log::error!(
                    "Error uploading part: {}, attempt {}/{} failed: {}",
                    part.get_number(),
                    attempt,
                    globals.retries,
                    e
                );

                // Increment attempt after an error
                if attempt == globals.retries {
                    // If it's the last attempt, return the error without incrementing attempt
                    return Err(e);
                }

                continue;
            }
        }
    }

    // update part with the etag and checksum if any
    let part = part.set_etag(etag).set_checksum(additional_checksum);

    let cbor_part = encode_to_vec(&part, bincode::config::standard())?;

    // move part to uploaded
    (&unprocessed, &processed)
        .transaction(|(unprocessed, processed)| {
            unprocessed.remove(&part_number.to_be_bytes())?;
            processed.insert(&part_number.to_be_bytes(), cbor_part.clone())?;
            Ok(())
        })
        .map_err(|err| match err {
            TransactionError::Abort(err) | TransactionError::Storage(err) => err,
        })?;

    db.flush()
}

#[allow(clippy::too_many_arguments)]
async fn try_upload_part(
    s3: &S3,
    key: &str,
    file: &Path,
    number: u16,
    uid: &str,
    seek: u64,
    chunk: u64,
    additional_checksum: &mut Option<Checksum>,
    globals: GlobalArgs,
) -> Result<String> {
    let action = actions::UploadPart::new(
        key,
        file,
        number,
        uid,
        seek,
        chunk,
        additional_checksum.as_mut(),
    );

    log::debug!("Uploading part: {}", number);

    action.request(s3, &globals).await
}
