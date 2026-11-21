use super::{DownloadState, GlobalArgs};
use crate::s3::tools::throttle_download;
use anyhow::Result;

pub(super) async fn download_response(
    res: &mut reqwest::Response,
    state: &mut DownloadState,
    globals: &GlobalArgs,
) -> Result<()> {
    while let Some(chunk) = res.chunk().await? {
        let chunk_len = chunk.len();
        state.process_chunk(chunk).await?;

        if let Some(bandwidth_kb) = globals.throttle {
            throttle_download(bandwidth_kb, chunk_len).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::progressbar::Bar;
    use anyhow::Context;
    use bytes::Bytes;
    use futures::{TryStreamExt, poll, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{io::AsyncWriteExt, time::Duration};

    #[tokio::test(start_paused = true)]
    async fn download_releases_next_chunk_only_after_current_chunk_budget() -> Result<()> {
        for (bandwidth, interval) in [
            (Some(1), Duration::from_secs(1)),
            (Some(2), Duration::from_millis(500)),
            (None, Duration::ZERO),
        ] {
            let first = Bytes::from(vec![0x37; 1024]);
            let second = Bytes::from(vec![0x81; 512]);
            let expected = [first.as_ref(), second.as_ref()].concat();
            let released = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&released);
            let body = stream::iter([Ok::<_, std::io::Error>(first), Ok(second)]).inspect_ok(
                move |chunk| {
                    observed.fetch_add(chunk.len(), Ordering::SeqCst);
                },
            );
            let mut response =
                reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(body)));
            let destination = tempfile::NamedTempFile::new()?;
            let mut state = DownloadState::new(
                tokio::fs::File::from_std(destination.reopen()?),
                Bar::default(),
                1536,
                false,
                false,
                None,
            );
            let globals = GlobalArgs {
                throttle: bandwidth,
                ..GlobalArgs::default()
            };
            let mut download = Box::pin(download_response(&mut response, &mut state, &globals));

            if bandwidth.is_some() {
                assert!(poll!(download.as_mut()).is_pending());
                assert_eq!(released.load(Ordering::SeqCst), 1024);
                tokio::time::advance(
                    interval
                        .checked_sub(Duration::from_millis(1))
                        .context("enabled download interval exceeds one millisecond")?,
                )
                .await;
                assert!(poll!(download.as_mut()).is_pending());
                assert_eq!(
                    released.load(Ordering::SeqCst),
                    1024,
                    "next bytes escaped before the configured download deadline"
                );
                tokio::time::advance(Duration::from_millis(1)).await;
                assert!(poll!(download.as_mut()).is_pending());
                assert_eq!(released.load(Ordering::SeqCst), 1536);
            }

            download.await?;
            state.file.flush().await?;
            assert_eq!(state.downloaded, 1536);
            assert_eq!(std::fs::read(destination.path())?, expected);
        }
        Ok(())
    }
}
