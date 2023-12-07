use crate::{
    cli::progressbar::Bar,
    s3::{actions, checksum::Checksum, S3},
    stream::{db::Db, iterator::PartIterator, part::Part},
};
use anyhow::{anyhow, Result};
use bincode::{deserialize, serialize};
use futures::stream::{FuturesUnordered, StreamExt};
use sled::transaction::{TransactionError, Transactional};
use std::{collections::BTreeMap, path::Path};
use tokio::time::{sleep, Duration};

const MAX_RETRIES: usize = 3;

// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingRESTAPImpUpload.html
// * Initiate Multipart Upload
// * Upload Part
// * Complete Multipart Upload
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

    for part in db_parts
        .iter()
        .values()
        .filter_map(Result::ok)
        .map(|p| deserialize(&p[..]).map_err(anyhow::Error::from))
    {
        let part = part?;

        log::debug!("Task push part: {:?}", part);

        tasks.push(upload_part(s3, key, file, &upload_id, sdb, part));

        await_tasks(&mut tasks, &pb, chunk_size).await?;
    }

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

    Ok(format!("ETag: {}", rs.e_tag))
}

async fn await_tasks<T>(tasks: &mut FuturesUnordered<T>, pb: &Bar, chunk_size: u64) -> Result<()>
where
    T: std::future::Future<Output = Result<usize>> + Send,
{
    // limit to num_cpus - 2 or 1
    while tasks.len() >= (num_cpus::get_physical() - 2).max(1) {
        log::debug!("Available tasks: {}", tasks.len());

        if let Some(r) = tasks.next().await {
            match r {
                Ok(_) => {
                    log::debug!("Task completed");

                    increment_progress_bar(pb, chunk_size, None);
                }
                Err(e) => return Err(anyhow!("{}", e)),
            }
        }
    }

    log::debug!("Remaining tasks: {}", tasks.len());

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
    while let Some(r) = tasks.next().await {
        match r {
            Ok(_) => {
                log::debug!("Task completed");

                increment_progress_bar(pb, chunk_size, None);
            }
            Err(e) => return Err(anyhow!("{}", e)),
        }
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
) -> Result<usize> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    let mut additional_checksum = part.get_checksum();

    // do request to get the ETag and update the checksum if required
    let part_number = part.get_number();
    let mut retries = 0;
    let etag = loop {
        let action = actions::UploadPart::new(
            key,
            file,
            part_number,
            uid,
            part.get_seek(),
            part.get_chunk(),
            additional_checksum.as_mut(),
        );

        log::debug!("Uploading part: {}", part_number);

        // retry 3 times
        match action.request(s3).await {
            Ok(etag) => break etag,
            Err(e) => {
                if retries < MAX_RETRIES {
                    retries += 1;

                    log::warn!("Error uploading part: {}, {}", part_number, e);

                    sleep(Duration::from_secs(retries as u64)).await;
                } else {
                    return Err(e);
                }
            }
        }
    };

    // update part with the etag and checksum if any
    let part = part.set_etag(etag).set_checksum(additional_checksum);

    let cbor_part = serialize(&part)?;

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
