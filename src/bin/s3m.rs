use anyhow::Result;
use s3m::options;
use s3m::s3::actions;
use s3m::s3m::stream;
use s3m::s3m::{start, Action};

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

        Action::PutObject {
            attr: _,
            buf_size,
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
                options::put_object(&s3, buf_size, &file, &key, s3m_dir, quiet).await?;
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
