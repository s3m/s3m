use crate::{
    progressbar::Bar,
    s3::{
        Error, RequestOptions, S3, actions,
        checksum::Checksum,
        request::{is_transient_send_error, source_changed},
    },
    stream::{
        db::Db,
        retry::retry_delay,
        source_file::{SourceFile, unverified_file},
        track_progress,
    },
};
use anyhow::{Result, anyhow};
use indicatif::ProgressBar;
use std::{collections::BTreeMap, fmt::Write, path::Path, pin::pin};
use tokio::{sync::mpsc::unbounded_channel, time::sleep};

pub struct UploadRequest<'a> {
    pub s3: &'a S3,
    pub key: &'a str,
    pub file: &'a Path,
    pub file_size: u64,
    /// The modification time in milliseconds since the Unix epoch, read with
    /// `file_size` before hashing. `None` skips comparison with a pre-hash mtime.
    pub file_mtime: Option<u128>,
    /// The lowercase hexadecimal BLAKE3 checksum used to open `sdb`.
    /// The contents must match this checksum and `file_size` on every attempt.
    pub file_blake3: &'a str,
    pub sdb: &'a Db,
    pub acl: Option<String>,
    pub meta: Option<BTreeMap<String, String>>,
    pub quiet: bool,
    /// The requested algorithm. Each attempt computes the value from the
    /// BLAKE3-verified file read.
    pub additional_checksum: Option<Checksum>,
    pub globals: RequestOptions,
}

/// # Errors
/// Will return an error if the upload fails
pub async fn upload(request: UploadRequest<'_>) -> Result<String> {
    let progress = if request.quiet {
        None
    } else {
        Bar::new(request.file_size).progress
    };
    let attempts = request.globals.retries.max(1);
    let mut attempt = 1;
    let (response, checksum) = loop {
        let source = source_before_attempt(&request).await?;
        match put_object(&request, progress.as_ref()).await {
            Ok(response) => {
                if let Some(change) = source.change(request.file).await {
                    return Err(source_changed(&change).into());
                }
                break response;
            }
            Err(error) if attempt == attempts || !is_retryable(&error) => {
                return Err(error.into());
            }
            Err(error) => {
                attempt += 1;
                let delay = retry_delay(attempt);
                log::warn!(
                    "PutObject failed: {error}. Retrying (attempt {attempt}/{attempts}) in {:.1} seconds",
                    delay.as_secs_f64()
                );
                sleep(delay).await;
            }
        }

        if let Some(pb) = &progress {
            pb.reset();
        }
    };
    if let Some(pb) = &progress {
        pb.finish();
    }

    let etag = &response.get("ETag").ok_or_else(|| anyhow!("no etag"))?;
    request.sdb.save_etag(etag)?;

    let mut report = response.iter().fold(String::new(), |mut output, (k, v)| {
        let _ = writeln!(output, "{k}: {v}");
        output
    });

    if let Some(checksum) = checksum {
        let checksum = checksum.report();
        log::info!("{checksum}");
        let _ = writeln!(report, "{checksum}");
    }

    Ok(report)
}

async fn source_before_attempt(request: &UploadRequest<'_>) -> Result<SourceFile> {
    let source = SourceFile::read(request.file)
        .await
        .map_err(|error| source_changed(&unverified_file(&error)))?;
    if let Some(change) = source.change_since_hash(request.file_size, request.file_mtime) {
        return Err(source_changed(&change).into());
    }
    Ok(source)
}

fn is_retryable(error: &Error) -> bool {
    match error {
        Error::Api(error) if error.status != 501 => {
            // AWS transient and throttling responses, including timeout and gateway failures:
            // <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>
            // <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-retries.html>
            // <https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-troubleshooting.html#http-408-issues>
            // S3 error codes such as RequestTimeout, OperationAborted and InternalError:
            // <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>
            matches!(error.status, 408 | 429 | 500 | 502 | 503 | 504)
                || matches!(
                    error.code.as_deref(),
                    Some(
                        "RequestTimeout"
                            | "RequestTimeoutException"
                            | "Throttling"
                            | "ThrottlingException"
                            | "SlowDown"
                            | "OperationAborted"
                            | "InternalError"
                    )
                )
                // #93: R2 also reports a disconnected client as an S3 error.
                // <https://developers.cloudflare.com/r2/api/s3/api/>
                // <https://developers.cloudflare.com/support/troubleshooting/http-status-codes/4xx-client-error/error-499/>
                || error.code.as_deref() == Some("ClientDisconnect")
                // #93: R2 can return Cloudflare's origin/gateway errors with no S3 XML.
                // <https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-5xx-errors/>
                || matches!(error.status, 520..=524)
        }
        Error::Http(error) => is_transient_send_error(error),
        _ => false,
    }
}

async fn put_object(
    request: &UploadRequest<'_>,
    progress: Option<&ProgressBar>,
) -> crate::s3::error::Result<(BTreeMap<&'static str, String>, Option<Checksum>)> {
    let (sender, receiver) = progress.map(|_| unbounded_channel()).unzip();
    let send = actions::PutObject::new(
        request.key,
        request.file,
        request.acl.clone(),
        request.meta.clone(),
        sender,
        request.additional_checksum.clone(),
        request.globals.object_lock.clone(),
    )
    .request_checked(
        request.s3,
        request.globals.clone(),
        request.file_size,
        request.file_blake3,
    );

    // Dropping this attempt's receiver keeps any late body updates off the next attempt's bar.
    if let (Some(pb), Some(receiver)) = (progress, receiver) {
        let mut send = pin!(send);
        tokio::select! {
            result = &mut send => result,
            _ = track_progress(receiver, pb, request.file_size) => send.await,
        }
    } else {
        send.await
    }
}

#[cfg(test)]
#[path = "upload_default_tests.rs"]
mod tests;
