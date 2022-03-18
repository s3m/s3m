use crate::s3::{actions, S3};
use anyhow::{anyhow, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn get(s3: S3, key: String, dest: Option<String>) -> Result<()> {
    // crate a new destination
    let file_name = Path::new(&key)
        .file_name()
        .with_context(|| format!("Failed to get file name from: {}", key))?;

    let path = match dest {
        Some(d) => Path::new(&d).join(file_name),
        None => Path::new(".").join(file_name),
    };

    // TODO implement  -f --force
    if path.is_file() {
        return Err(anyhow!("file {:?} already exists", path));
    }

    // do the request
    let action = actions::GetObject::new(&key);
    let mut res = action.request(&s3).await?;
    let mut file = File::create(&path).await?;

    // get the file_size in bytes by using the content_length
    let file_size = res
        .content_length()
        .context("could not get content_length")?;

    let pb = ProgressBar::new(file_size);

    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({bytes_per_sec} - {eta})")
            // "█▉▊▋▌▍▎▏  ·"
            .progress_chars(
                "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}",
            ),
    );

    let mut downloaded = 0;

    while let Some(bytes) = res.chunk().await? {
        let new = min(downloaded + bytes.len() as u64, file_size);
        downloaded = new;
        pb.set_position(new);
        file.write_all(&bytes).await?;
    }

    pb.finish();

    while let Some(bytes) = res.chunk().await? {
        file.write_all(&bytes).await?;
    }

    Ok(())
}
