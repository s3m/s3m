use crate::s3::{actions, S3};
use anyhow::Result;
use chrono::{DateTime, Utc};
use colored::Colorize;

pub async fn list_multipart_uploads(s3: &S3) -> Result<()> {
    let action = actions::ListMultipartUploads::new();
    let rs = action.request(s3).await?;
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
    Ok(())
}

pub async fn list_objects(
    s3: &S3,
    prefix: Option<String>,
    start_after: Option<String>,
) -> Result<()> {
    let action = actions::ListObjectsV2::new(prefix, start_after);
    let rs = action.request(s3).await?;
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
    Ok(())
}

// LIST BUCKETS
pub async fn list_buckets(s3: &S3) -> Result<()> {
    let action = actions::ListBuckets::new();
    let rs = action.request(s3).await?;
    for bucket in rs.buckets.bucket {
        let dt = DateTime::parse_from_rfc3339(&bucket.creation_date)?;
        let creation_date: DateTime<Utc> = DateTime::from(dt);
        println!(
            "{} {}",
            format!("[{}]", creation_date.format("%F %T %Z")).green(),
            bucket.name.yellow()
        );
    }
    Ok(())
}
