use super::{Stream, StreamProgressEvent, emit_stream_progress, stream_sending_progress_callback};
use crate::s3::{RequestOptions, actions};
use anyhow::Result;
use rand::{RngExt, rng};
use tokio::time::{Duration, sleep};

/// Maximum backoff before jitter.
const MAX_BACKOFF_SECS: u64 = 30;

/// Seconds to wait before `attempt` (1-based): 1, 2, 4, 8, 16, then 30 for
/// every later attempt, however large the attempt budget is.
pub(super) fn backoff_secs(attempt: u32) -> u64 {
    2u64.saturating_pow(attempt.saturating_sub(1))
        .min(MAX_BACKOFF_SECS)
}

pub(super) fn retry_delay(attempt: u32) -> Duration {
    let jitter_ms = rng().random_range(0_u64..1000);
    Duration::from_secs(backoff_secs(attempt)) + Duration::from_millis(jitter_ms)
}

pub(super) async fn try_stream_part(part: &Stream<'_>) -> Result<String> {
    let attempts = part.retries.max(1);
    let mut etag = String::new();

    let digest_sha = part.sha.clone().finish();
    let digest_md5 = part.md5.clone().finalize();

    // Create globals only to pass the throttle
    let globals = RequestOptions {
        throttle: part.throttle,
        retries: attempts,
        compress: false,
        encrypt: false,
        enc_key: None,
        object_lock: None,
    };

    for attempt in 1..=attempts {
        if attempt > 1 {
            let total_backoff = retry_delay(attempt);

            log::warn!(
                "Error streaming part number {}, retrying in {:.1} seconds",
                part.part_number,
                total_backoff.as_secs_f64()
            );

            sleep(total_backoff).await;
        }

        emit_stream_progress(
            part.channel.as_ref(),
            StreamProgressEvent::SendingStarted {
                part_number: part.part_number,
                total_bytes: part.count as u64,
            },
        );

        let action = actions::StreamPart::new(
            part.key,
            part.tmp_file.path(),
            part.part_number,
            part.upload_id,
            part.count,
            (digest_sha.as_ref(), digest_md5.as_ref()),
            stream_sending_progress_callback(part.channel.as_ref()),
        );

        match action.request(part.s3, &globals).await {
            Ok(e) => {
                etag = e;
                emit_stream_progress(
                    part.channel.as_ref(),
                    StreamProgressEvent::Confirmed(part.count as u64),
                );

                log::info!("Uploaded part: {}, etag: {}", part.part_number, etag);

                break;
            }

            Err(e) => {
                emit_stream_progress(part.channel.as_ref(), StreamProgressEvent::SendingStopped);

                log::error!(
                    "Error uploading part number {}, attempt {}/{} failed: {}",
                    part.part_number,
                    attempt,
                    attempts,
                    e
                );

                if attempt == attempts {
                    return Err(anyhow::anyhow!(
                        "Error uploading part number {}, {}",
                        part.part_number,
                        e
                    ));
                }
            }
        }
    }

    Ok(etag)
}

#[cfg(test)]
mod tests {
    use super::backoff_secs;

    #[test]
    fn backoff_doubles_then_stays_at_the_cap() {
        for (attempt, expected) in [
            (0, 1),
            (1, 1),
            (2, 2),
            (5, 16),
            (6, 30),
            (64, 30),
            (65, 30),
            (u32::MAX, 30),
        ] {
            assert_eq!(backoff_secs(attempt), expected, "attempt {attempt}");
        }
    }
}
