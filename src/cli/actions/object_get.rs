use crate::{
    cli::{actions::Action, progressbar::Bar},
    s3::{actions, S3},
};
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::{cmp::min, path::Path};
use tokio::{fs::File, io::AsyncWriteExt};

pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::GetObject {
        key,
        get_head,
        dest,
        quiet,
    } = action
    {
        if get_head {
            let action = actions::HeadObject::new(&key);
            let headers = action.request(s3).await?;
            let mut i = 0;
            for k in headers.keys() {
                i = k.len();
            }
            i += 1;
            for (k, v) in headers {
                println!("{:<width$} {}", format!("{k}:").green(), v, width = i);
            }
        } else {
            let file_name = Path::new(&key)
                .file_name()
                .with_context(|| format!("Failed to get file name from: {key}"))?;

            let path = dest.map_or_else(
                || Path::new(".").join(file_name),
                |d| Path::new(&d).join(file_name),
            );

            // TODO implement  -f --force
            if path.is_file() {
                return Err(anyhow!("file {:?} already exists", path));
            }

            // do the request
            let action = actions::GetObject::new(&key);
            let mut res = action.request(s3).await?;
            let mut file = File::create(&path).await?;

            // get the file_size in bytes by using the content_length
            let file_size = res
                .content_length()
                .context("could not get content_length")?;

            let pb = if quiet {
                Bar::default()
            } else {
                Bar::new(file_size)
            };

            let mut downloaded = 0;
            while let Some(bytes) = res.chunk().await? {
                let new = min(downloaded + bytes.len() as u64, file_size);
                downloaded = new;
                if let Some(pb) = pb.progress.as_ref() {
                    pb.set_position(new);
                }
                file.write_all(&bytes).await?;
            }

            if let Some(pb) = pb.progress.as_ref() {
                pb.finish();
            }

            while let Some(bytes) = res.chunk().await? {
                file.write_all(&bytes).await?;
            }
        }
    }

    Ok(())
}