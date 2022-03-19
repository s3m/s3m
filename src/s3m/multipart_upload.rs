use crate::s3::{actions, S3};
use crate::s3m::{Db, Part};
use anyhow::{anyhow, Result};
use futures::stream::FuturesUnordered;
use indicatif::{ProgressBar, ProgressStyle};
use serde_cbor::{de::from_reader, to_vec};
use sled::transaction::{TransactionError, Transactional};
use std::time::Duration;
use tokio::time;
use tokio_stream::StreamExt;

fn progress_bar_parts(file_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({bytes_per_sec} - {eta})")
            // "█▉▊▋▌▍▎▏  ·"
            .progress_chars(
                "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}",
            ),
    );
    pb
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
    sdb: &Db,
) -> Result<String> {
    // trees for keeping track of parts to upload
    let db_parts = sdb.db_parts()?;
    let db_uploaded = sdb.db_uploaded()?;

    let upload_id = if let Some(uid) = sdb.upload_id()? {
        uid
    } else {
        // Initiate Multipart Upload - request an Upload ID
        let action = actions::CreateMultipartUpload::new(key);
        let response = action.request(s3).await?;
        db_parts.clear()?;
        // save the upload_id to resume if required
        sdb.save_upload_id(&response.upload_id)?;
        response.upload_id
    };

    // if db_parts is not empty it means that a previous upload did not finish successfully.
    // skip creating the parts again and try to re-upload the pending ones
    if db_parts.is_empty() {
        let mut chunk = chunk_size;
        let mut seek: u64 = 0;
        let mut number: u16 = 1;
        while seek < file_size {
            if (file_size - seek) <= chunk {
                chunk = file_size % chunk;
            }
            sdb.create_part(number, seek, chunk)?;
            seek += chunk;
            number += 1;
        }
        db_parts.flush()?;
    }

    // Upload parts progress bar
    let pb = progress_bar_parts(file_size);
    pb.inc(db_uploaded.len() as u64 * chunk_size);

    let mut tasks = FuturesUnordered::new();

    for part in db_parts.iter().values() {
        if let Ok(p) = part {
            let part: Part = from_reader(&p[..])?;
            tasks.push(upload_part(s3, key, file, &upload_id, sdb, part));
        }

        // limit to N threads
        if tasks.len() == 2 {
            if let Some(r) = tasks.next().await {
                // TODO better error handling
                if r.is_ok() {
                    pb.inc(chunk_size);
                }
            }
        }
    }

    // consume remaining tasks
    while let Some(r) = tasks.next().await {
        if r.is_ok() {
            pb.inc(chunk_size)
        }
    }
    pb.finish();

    if !db_parts.is_empty() {
        return Err(anyhow!("could not upload all parts"));
    }

    // Complete Multipart Upload
    let uploaded = sdb.uploaded_parts()?;
    let action = actions::CompleteMultipartUpload::new(key, &upload_id, uploaded);
    let rs = action.request(s3).await?;

    // cleanup uploads tree
    db_uploaded.clear()?;

    // save the returned Etag
    sdb.save_etag(&rs.e_tag)?;

    Ok(format!("ETag: {}", rs.e_tag))
}

// TODO
// Metadata cannot be specified in this context.
// x-amz-acl=public
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
async fn upload_part(
    s3: &S3,
    key: &str,
    file: &str,
    uid: &str,
    db: &Db,
    part: Part,
) -> Result<usize> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    // do request to get the ETag and update the part
    let part_number = part.get_number();
    let mut retries: u64 = 0;
    let etag = loop {
        let action = actions::UploadPart::new(
            key,
            file,
            part_number,
            uid,
            part.get_seek(),
            part.get_chunk(),
        );

        match action.request(s3).await {
            Ok(etag) => break etag,
            Err(e) => {
                if retries < 3 {
                    retries += 1;
                    // TODO backoff strategy
                    time::sleep(Duration::from_secs(retries)).await;
                } else {
                    return Err(e);
                }
            }
        }
    };

    let part = part.set_etag(etag);
    let cbor_part = to_vec(&part)?;

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
    Ok(db.flush()?)
}
