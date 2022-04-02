use crate::s3::{tools, S3};
use crate::s3m::progressbar::Bar;
use crate::s3m::{multipart_upload, upload, Db};
use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::fs::metadata;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PART_SIZE: usize = 5_368_709_120;
const MAX_FILE_SIZE: usize = 5_497_558_138_880;
const MAX_PARTS_PER_UPLOAD: usize = 10_000;

#[allow(clippy::too_many_arguments)]
pub async fn put_object(
    s3: &S3,
    mut buf_size: usize,
    file: &str,
    key: &str,
    s3m_dir: PathBuf,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    quiet: bool,
) -> Result<()> {
    // Get file size and last modified time
    let (file_size, file_mtime) = metadata(&file)
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

    // <https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/>
    if file_size > MAX_FILE_SIZE as u64 {
        return Err(anyhow!("object size limit 5 TB"));
    }

    // calculate the chunk size
    let mut parts = file_size / buf_size as u64;
    while parts > MAX_PARTS_PER_UPLOAD as u64 {
        buf_size *= 2;
        parts = file_size / buf_size as u64;
    }

    if buf_size > MAX_PART_SIZE {
        return Err(anyhow!("max part size 5 GB"));
    }

    let checksum = checksum(file, quiet)?;

    let db = Db::new(s3, key, &checksum, file_mtime, &s3m_dir)
        .context("could not create stream tree, try option \"-r\"")?;

    // check if file has been uploded already
    let etag = &db
        .check()?
        .context("could not query db, try option \"-r\", to clean it");
    if let Ok(etag) = etag {
        if !quiet {
            println!("{}", etag);
        }
        return Ok(());
    };

    // upload in multipart
    if file_size > buf_size as u64 {
        let rs = multipart_upload(
            s3,
            key,
            file,
            file_size,
            buf_size as u64,
            &db,
            acl,
            meta,
            quiet,
        )
        .await
        .context("multipart upload failed")?;
        if !quiet {
            println!("{}", rs);
        }
    } else {
        let rs = upload(s3, key, file, file_size, &db, acl, meta, quiet).await?;
        if !quiet {
            println!("{}", rs);
        }
    }

    Ok(())
}

fn checksum(file: &str, quiet: bool) -> Result<String> {
    let pb = if quiet {
        Bar::default()
    } else {
        Bar::new_spinner()
    };
    // CHECKSUM BLAKE3
    let checksum = tools::blake3(file).context("could not calculate the checksum")?;
    if let Some(pb) = pb.progress.as_ref() {
        pb.finish_and_clear();
        println!("checksum: {}", &checksum);
    }
    Ok(checksum)
}
