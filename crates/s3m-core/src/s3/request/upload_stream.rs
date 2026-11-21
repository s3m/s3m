use super::DEFAULT_FRAMED_CHUNK_SIZE_BYTES;
use futures::future::Either;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};

pub(super) fn calculate_duration_per_chunk(
    bandwidth_kb_per_sec: usize,
    chunk_size: usize,
) -> Duration {
    let bandwidth_bytes_per_sec = u128::from(bandwidth_kb_per_sec as u64) * 1024;
    let nanos = (u128::from(chunk_size as u64) * 1_000_000_000) / bandwidth_bytes_per_sec;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// The request-body scheduling boundary shared by file and part uploads.
pub(super) fn paced<S: Stream>(stream: S, throttle: Option<usize>) -> impl Stream<Item = S::Item> {
    match throttle {
        Some(bandwidth_kb) => {
            let duration_per_chunk =
                calculate_duration_per_chunk(bandwidth_kb, DEFAULT_FRAMED_CHUNK_SIZE_BYTES);
            log::info!(
                "Throttling enabled: {} KB/s (duration per chunk: {:.3}s)",
                bandwidth_kb,
                duration_per_chunk.as_secs_f64()
            );
            Either::Left(stream.throttle(duration_per_chunk))
        }
        None => Either::Right(stream),
    }
}

#[cfg(test)]
mod tests {
    use super::paced;
    use futures::{FutureExt, StreamExt, stream};
    use std::pin::pin;
    use tokio::time::{Duration, Instant, advance};

    #[tokio::test(start_paused = true)]
    async fn upload_scheduler_holds_chunks_until_selected_delay_expires() {
        for (rate, delay) in [
            (128, Duration::from_secs(1)),
            (256, Duration::from_millis(500)),
        ] {
            let mut body = pin!(paced(stream::iter([1, 2, 3]), Some(rate)));
            assert_eq!(body.next().now_or_never(), Some(Some(1)));

            for chunk in [2, 3] {
                assert!(body.next().now_or_never().is_none(), "rate={rate}");
                let released_at = Instant::now() + delay;
                advance(delay.saturating_sub(Duration::from_millis(1))).await;
                assert!(body.next().now_or_never().is_none(), "rate={rate}");
                // Only Tokio's paused clock advances here, never wall time.
                assert_eq!(body.next().await, Some(chunk));
                assert_eq!(Instant::now(), released_at);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn upload_scheduler_without_a_rate_releases_chunks_without_delay() {
        let mut body = pin!(paced(stream::iter([1, 2, 3]), None));
        for chunk in [1, 2, 3] {
            assert_eq!(body.next().now_or_never(), Some(Some(chunk)));
        }
        assert_eq!(body.next().now_or_never(), Some(None));
    }
}
