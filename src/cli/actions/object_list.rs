use crate::{
    cli::actions::Action,
    s3::{
        S3, actions,
        responses::{Bucket, Object, Upload},
    },
};
use anyhow::Result;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use colored::Colorize;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::ListObjects {
        bucket,
        list_multipart_uploads,
        max_kub,
        prefix,
        start_after,
    } = action
    {
        match (bucket, list_multipart_uploads) {
            // list objects
            (Some(_), false) => {
                let action = actions::ListObjectsV2::new(prefix, start_after, max_kub);
                let rs = action.request(s3).await?;
                for object in rs.contents {
                    print_object_info(&object)?;
                }
            }

            // list multipart uploads
            (Some(_), true) => {
                let action = actions::ListMultipartUploads::new(max_kub);
                let rs = action.request(s3).await?;
                if let Some(uploads) = rs.upload {
                    for upload in uploads {
                        print_upload_info(&upload)?;
                    }
                }
            }

            // list buckets
            (None, _) => {
                let action = actions::ListBuckets::new(max_kub);
                let rs = action.request(s3).await?;
                for bucket in rs.buckets.bucket {
                    print_bucket_info(&bucket)?;
                }
            }
        }
    }

    Ok(())
}

fn print_object_info(object: &Object) -> Result<()> {
    let dt = DateTime::parse_from_rfc3339(&object.last_modified)?;
    let last_modified: DateTime<Utc> = DateTime::from(dt);
    println!(
        "{} {:>10} {:<}",
        format!("[{}]", last_modified.format("%F %T %Z")).green(),
        ByteSize(object.size).to_string().yellow(),
        object.key
    );
    Ok(())
}

fn print_upload_info(upload: &Upload) -> Result<()> {
    let dt = DateTime::parse_from_rfc3339(&upload.initiated)?;
    let initiated: DateTime<Utc> = DateTime::from(dt);
    println!(
        "{} {} {}",
        format!("[{}]", initiated.format("%F %T %Z")).green(),
        upload.upload_id.yellow(),
        upload.key
    );
    Ok(())
}

fn print_bucket_info(bucket: &Bucket) -> Result<()> {
    let dt = DateTime::parse_from_rfc3339(&bucket.creation_date)?;
    let creation_date: DateTime<Utc> = DateTime::from(dt);
    println!(
        "{} {}",
        format!("[{}]", creation_date.format("%F %T %Z")).green(),
        bucket.name.yellow()
    );
    Ok(())
}
