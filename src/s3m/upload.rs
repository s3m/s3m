use crate::s3::{actions, S3};
use crate::s3m::{progressbar::Bar, Db};
use anyhow::{anyhow, Result};
use std::cmp::min;
use tokio::sync::mpsc::unbounded_channel;

pub async fn upload(
    s3: &S3,
    key: &str,
    file: &str,
    file_size: u64,
    sdb: &Db,
    quiet: bool,
) -> Result<String> {
    let (sender, mut receiver) = unbounded_channel();
    let mut action = actions::PutObject::new(key, file, Some(sender));
    // TODO
    action.x_amz_acl = Some(String::from("public-read"));

    if !quiet {
        if let Some(pb) = Bar::new(file_size).progress {
            tokio::spawn(async move {
                let mut uploaded = 0;
                while let Some(i) = receiver.recv().await {
                    let new = min(uploaded + i as u64, file_size);
                    uploaded = new;
                    pb.set_position(new);
                }
                pb.finish();
            });
        }
    };

    let response = action.request(s3).await?;
    let etag = &response.get("ETag").ok_or_else(|| anyhow!("no etag"))?;
    sdb.save_etag(etag)?;
    Ok(response
        .iter()
        .map(|(k, v)| format!("{}: {}\n", k, v))
        .collect::<String>())
}
