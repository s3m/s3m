use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use colored::Colorize;
use http::method::Method;
use indicatif::{ProgressBar, ProgressStyle};
use s3m::s3::{actions, tools, Signature};
use s3m::s3m::{multipart_upload, stream, upload, Db};
use s3m::s3m::{start, Action};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PART_SIZE: usize = 5_368_709_120;
const MAX_FILE_SIZE: usize = 5_497_558_138_880;
const MAX_PARTS_PER_UPLOAD: usize = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    let (s3, action) = start()?;

    match action {
        Action::ShareObject { key, expire } => {
            let url = Signature::new(&s3, "s3", Method::from_bytes(b"GET").unwrap())?
                .presigned_url(&key, expire)?;
            println!("{}", url);
        }

        Action::GetObject { key, get_head } => {
            if get_head {
                println!("{}", key);
                let action = actions::HeadObject::new(&key);
                let headers = action.request(&s3).await?;
                let mut i = 0;
                for k in headers.keys() {
                    i = k.len();
                }
                i += 1;
                for (k, v) in headers {
                    println!("{:<width$} {}", format!("{}:", k).green(), v, width = i)
                }
            }
        }

        Action::ListObjects {
            bucket,
            list_multipart_uploads,
        } => {
            if bucket.is_some() {
                if list_multipart_uploads {
                    let action = actions::ListMultipartUploads::new();
                    let rs = action.request(&s3).await?;
                    if let Some(uploads) = rs.upload {
                        for upload in uploads {
                            let dt = DateTime::parse_from_rfc3339(&upload.initiated)?;
                            let initiated: DateTime<Utc> = DateTime::from(dt);
                            println!(
                                "{} {} {}",
                                format!("[{}]", initiated.format("%F %T %Z")).green(),
                                upload.upload_id.yellow(),
                                upload.key
                            );
                        }
                    }
                } else {
                    let mut action = actions::ListObjectsV2::new();
                    action.prefix = Some(String::from(""));
                    let rs = action.request(&s3).await?;
                    for object in rs.contents {
                        let dt = DateTime::parse_from_rfc3339(&object.last_modified)?;
                        let last_modified: DateTime<Utc> = DateTime::from(dt);
                        println!(
                            "{} {:>10} {:<}",
                            format!("[{}]", last_modified.format("%F %T %Z")).green(),
                            bytesize::to_string(object.size, true).yellow(),
                            object.key
                        );
                    }
                }
            } else {
                // LIST BUCKETS
                let action = actions::ListBuckets::new();
                let rs = action.request(&s3).await?;
                for bucket in rs.buckets.bucket {
                    let dt = DateTime::parse_from_rfc3339(&bucket.creation_date)?;
                    let creation_date: DateTime<Utc> = DateTime::from(dt);
                    println!(
                        "{} {:>10}/",
                        format!("[{}]", creation_date.format("%F %T %Z")).green(),
                        bucket.name.yellow()
                    );
                }
            }
        }

        // Upload
        Action::PutObject {
            mut buf_size,
            file,
            home_dir,
            key,
            stdin,
            threads,
        } => {
            // upload from stdin
            if stdin {
                let etag = stream(&s3, &key, buf_size).await?;
                println!("{}", etag);
            } else {
                // Get file size and last modified time
                let (file_size, file_mtime) = fs::metadata(&file)
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

                let db = Db::new(&s3, &key, &checksum, file_mtime, &home_dir)
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
                    let rs = multipart_upload(
                        &s3,
                        &key,
                        &file,
                        file_size,
                        buf_size as u64,
                        threads,
                        &db,
                    )
                    .await
                    .context("multipart upload failed")?;
                    println!("{}", rs);
                } else {
                    let rs = upload(&s3, &key, &file, file_size, &db).await?;
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
