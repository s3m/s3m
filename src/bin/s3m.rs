use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Local};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use s3m::s3::{actions, tools};
use s3m::s3m::{multipart_upload, prebuffer, upload, Db};
use s3m::s3m::{start, Action};
use std::fs::metadata;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PART_SIZE: u64 = 5_368_709_120;
const MAX_FILE_SIZE: u64 = 5_497_558_138_880;
const MAX_PARTS_PER_UPLOAD: u64 = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    let (s3, action) = start()?;

    match action {
        Action::ListObjects(bucket) => {
            if bucket.is_some() {
                let mut action = actions::ListObjectsV2::new();
                action.prefix = Some(String::from(""));
                let rs = action.request(&s3).await?;
                for object in rs.contents {
                    let dt = DateTime::parse_from_rfc3339(&object.last_modified)?;
                    let last_modified: DateTime<Local> = DateTime::from(dt);
                    println!(
                        "{} {:>10} {:<}",
                        format!("[{}]", last_modified.format("%F %T %Z")).green(),
                        bytesize::to_string(object.size, true).yellow(),
                        object.key
                    );
                }
            } else {
                // list buckets
                let action = actions::ListBuckets::new();
                let rs = action.request(&s3).await?;
                for bucket in rs.buckets.bucket {
                    let dt = DateTime::parse_from_rfc3339(&bucket.creation_date)?;
                    let creation_date: DateTime<Local> = DateTime::from(dt);
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
            mut buffer,
            file,
            home_dir,
            key,
            stdin,
            threads,
        } => {
            if stdin {
                return prebuffer(buffer).await;
            }

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
            if file_size > MAX_FILE_SIZE {
                return Err(anyhow!("object size limit 5 TB"));
            }

            // calculate the chunk size
            let mut parts = file_size / buffer;
            while parts > MAX_PARTS_PER_UPLOAD {
                buffer *= 2;
                parts = file_size / buffer;
            }

            if buffer > MAX_PART_SIZE {
                return Err(anyhow!("max part size 5 GB"));
            }

            let checksum = checksum(&file)?;

            let db = Db::new(&s3, &key, &checksum, file_mtime, &home_dir)
                .context("could not create stream tree")?;

            // check if file has been uploded already
            let etag = &db
                .check()?
                .context("could not query db, try option \"-r\", to clean it");
            if let Ok(etag) = etag {
                println!("{}", etag);
                return Ok(());
            };

            // upload in multipart
            if file_size > buffer {
                let rs = multipart_upload(&s3, &key, &file, file_size, buffer, threads, &db)
                    .await
                    .context("multipart upload failed")?;
                println!("{}", rs);
            } else {
                let rs = upload(&s3, &key, &file, file_size, &db).await?;
                println!("{}", rs);
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
