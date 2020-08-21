use crate::s3::{actions, S3};
use crate::s3m::Stream;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_cbor::{de::from_reader, to_vec};
use sled::transaction::{TransactionError, Transactional};
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Serialize, Deserialize, Default)]
struct Part {
    etag: String,
    number: u16,
    seek: u64,
    chunk: u64,
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
    threads: usize,
    sdb: &Stream,
) -> Result<String, Box<dyn error::Error>> {
    let mut upload_id = String::new();

    if let Some(u) = sdb.upload_id()? {
        upload_id = u;
    }

    // trees for keeping track of parts to upload
    let db_parts = sdb.db_parts()?;
    let db_uploaded = sdb.db_uploaded()?;

    if upload_id.is_empty() {
        // Initiate Multipart Upload - request an Upload ID
        let action = actions::CreateMultipartUpload::new(key);
        let response = action.request(s3).await?;
        upload_id = response.upload_id;
        db_parts.clear()?;
    }

    // save the upload_id to resume if required
    sdb.save_upload_id(&upload_id)?;

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
            let part = Part {
                number,
                seek,
                chunk,
                ..Default::default()
            };
            let cbor_part = to_vec(&part)?;
            db_parts.insert(format!("{}", number), cbor_part)?;
            seek += chunk;
            number += 1;
        }
        db_parts.flush()?;
    }

    // Upload parts progress bar
    let pb = ProgressBar::new(db_parts.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {pos}/{len} ({eta})")
            // "█▉▊▋▌▍▎▏  ·"
            .progress_chars(
                "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}",
            ),
    );

    let mut tasks = FuturesUnordered::new();

    let bin_parts = db_parts.iter().values();
    for bin_part in bin_parts {
        if let Ok(p) = bin_part {
            let part: Part = from_reader(&p[..])?;
            tasks.push(async { upload_part(s3, key, file, &upload_id, sdb, part).await });
            // limit to N threads
            if tasks.len() == threads {
                while let Some(r) = tasks.next().await {
                    if r.is_ok() {
                        pb.inc(1)
                    }
                }
            }
        }
    }

    // consume remaining tasks
    loop {
        if let Some(r) = tasks.next().await {
            if r.is_ok() {
                pb.inc(1)
            }
        } else {
            pb.finish();
            break;
        }
    }

    if !db_parts.is_empty() {
        return Err("could not upload all parts".into());
    }

    let uploaded = db_uploaded
        .into_iter()
        .values()
        .flat_map(|part| {
            part.map(|part| {
                from_reader(&part[..])
                    .map(|p: Part| {
                        (
                            p.number,
                            actions::Part {
                                etag: p.etag,
                                number: p.number,
                            },
                        )
                    })
                    .map_err(|e| e.into())
            })
        })
        .collect::<Result<BTreeMap<u16, actions::Part>, Box<dyn error::Error>>>()?;

    // Complete Multipart Upload
    let action = actions::CompleteMultipartUpload::new(key, &upload_id, uploaded);
    let rs = action.request(s3).await?;

    // cleanup uploads tree
    db_uploaded.clear()?;

    // save the returned Etag
    sdb.save_etag(&rs.e_tag)?;

    Ok(format!("ETag: {}", rs.e_tag))
}

async fn upload_part(
    s3: &S3,
    key: &str,
    file: &str,
    uid: &str,
    db: &Stream,
    mut part: Part,
) -> Result<usize, Box<dyn error::Error>> {
    let unprocessed = db.db_parts()?;
    let processed = db.db_uploaded()?;

    // do request to get the ETag and update the part
    let pn = format!("{}", part.number);
    let action = actions::UploadPart::new(key, file, &pn, uid, part.seek, part.chunk);

    // TODO implement retry
    let etag = action.request(s3).await?;

    part.etag = etag;
    let cbor_part = to_vec(&part)?;

    // move part to uploaded
    (&unprocessed, &processed)
        .transaction(|(unprocessed, processed)| {
            unprocessed.remove(pn.as_bytes())?;
            processed.insert(pn.as_bytes(), cbor_part.clone())?;
            Ok(())
        })
        .map_err(|err| match err {
            TransactionError::Abort(err) | TransactionError::Storage(err) => err,
        })?;
    Ok(db.flush_async().await?)
}
