use crate::s3::{actions, S3};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

#[derive(Debug, Serialize, Deserialize, Default)]
struct Part {
    etag: String,
    number: u16,
    seek: u64,
    chunk: u64,
}

async fn progress_bar_bytes(file_size: u64, mut receiver: UnboundedReceiver<usize>) -> Result<()> {
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:50.green/blue} {bytes}/{total_bytes} ({eta})")
            // "█▉▊▋▌▍▎▏  ·"
            .progress_chars(
                "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f}  \u{b7}",
            ),
    );
    // print progress bar
    let mut uploaded = 0;
    while let Some(i) = receiver.recv().await {
        let new = min(uploaded + i as u64, file_size);
        uploaded = new;
        pb.set_position(new);
    }
    pb.finish();
    Ok(())
}

pub async fn upload(s3: &S3, key: &str, file: &str, file_size: u64) -> Result<String> {
    let (sender, receiver) = unbounded_channel();
    let action = actions::PutObject::new(key, file, Some(sender));
    let response = tokio::try_join!(progress_bar_bytes(file_size, receiver), action.request(s3))?.1;
    Ok(response)
}
