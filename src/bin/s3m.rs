use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use s3m::options;
use s3m::s3::{actions, tools};
use s3m::s3m::{multipart_upload, stream, upload, Db};
use s3m::s3m::{start, Action};
use std::fs::metadata;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PART_SIZE: usize = 5_368_709_120;
const MAX_FILE_SIZE: usize = 5_497_558_138_880;
const MAX_PARTS_PER_UPLOAD: usize = 10_000;
const BUFFER_SIZE: usize = 536_870_912;

#[tokio::main]
async fn main() -> Result<()> {
    let (s3, action) = start()?;

    match action {
        Action::ShareObject { key, expire } => {
            let url = options::share(&s3, &key, expire)?;
            println!("{}", url);
        }

        Action::GetObject {
            key,
            get_head,
            dest,
            quiet,
        } => {
            if get_head {
                options::get_head(s3, key).await?;
            } else {
                options::get(s3, key, dest, quiet).await?;
            }
        }

        Action::ListObjects {
            bucket,
            list_multipart_uploads,
        } => {
            if bucket.is_some() {
                if list_multipart_uploads {
                    options::list_multipart_uploads(&s3).await?;
                } else {
                    options::list_objects(&s3).await?;
                }
            } else {
                options::list_buckets(&s3).await?;
            }
        }

        // Upload
        Action::PutObject {
            attr: _,
            mut buf_size,
            file,
            s3m_dir,
            key,
            pipe,
            quiet,
        } => {
            if pipe {
                let etag = stream(&s3, &key, BUFFER_SIZE).await?;
                println!("{}", etag);
            } else if let Some(file) = file {
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

                let checksum = checksum(&file)?;

                let db = Db::new(&s3, &key, &checksum, file_mtime, &s3m_dir)
                    .context("could not create stream tree, try option \"-r\"")?;

                // check if file has been uploded already
                let etag = &db
                    .check()?
                    .context("could not query db, try option \"-r\", to clean it");
                if let Ok(etag) = etag {
                    println!("{}", etag);
                    return Ok(());
                };

                // upload in multipart
                if file_size > buf_size as u64 {
                    let rs = multipart_upload(&s3, &key, &file, file_size, buf_size as u64, &db)
                        .await
                        .context("multipart upload failed")?;
                    println!("{}", rs);
                } else {
                    let rs = upload(&s3, &key, &file, file_size, &db, quiet).await?;
                    println!("{}", rs);
                }
            }
        }
        Action::DeleteObject { key, upload_id } => {
            if upload_id.is_empty() {
                let action = actions::DeleteObject::new(&key);
                action.request(&s3).await?;
            } else {
                let action = actions::AbortMultipartUpload::new(&key, &upload_id);
                action.request(&s3).await?;
            }
        }
    }
    Ok(())
}

fn checksum(file: &str) -> Result<String> {
    // progress bar for the checksum
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(200);
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&[
                "\u{2801}", "\u{2802}", "\u{2804}", "\u{2840}", "\u{2880}", "\u{2820}", "\u{2810}",
                "\u{2808}", "",
            ])
            .template("checksum: {spinner:.green}"),
    );
    // CHECKSUM BLAKE3
    let checksum = tools::blake3(file).context("could not calculate the checksum")?;
    pb.finish_and_clear();
    println!("checksum: {}", &checksum);
    Ok(checksum)
}
