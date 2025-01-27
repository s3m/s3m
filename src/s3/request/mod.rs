//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use anyhow::Result;
use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::stream::TryStreamExt;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client, Response,
};
use std::{collections::BTreeMap, path::Path};
use tokio::time::Duration;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

const DEFAULT_FRAMED_CHUNK_SIZE_BYTES: usize = 1024 * 128;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// Calculate the duration per chunk to achieve the desired bandwidth
///
/// # Parameters
/// - `bandwidth_kb_per_sec`: Throttle limit in KB/s
/// - `chunk_size`: Size of the data chunk in bytes
///
/// # Returns
/// Duration to wait after processing each chunk
fn calculate_duration_per_chunk(bandwidth_kb_per_sec: usize, chunk_size: usize) -> Duration {
    let bandwidth_bytes_per_sec = bandwidth_kb_per_sec * 1024;
    let duration_secs = chunk_size as f64 / bandwidth_bytes_per_sec as f64;
    Duration::from_secs_f64(duration_secs)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    sender: Option<Sender<usize>>,
    throttle: Option<usize>,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!("HTTP method: {method}, Headers: {headers:#?}");

    let client = Client::builder().user_agent(APP_USER_AGENT).build()?;

    let request = if let Some(file_path) = file {
        let file = File::open(file_path).await?;

        let stream =
            FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES)
                .inspect_ok(move |chunk| {
                    if let Some(tx) = &sender {
                        log::debug!("Sending {} bytes", chunk.len());

                        if let Err(e) = tx.send(chunk.len()) {
                            eprintln!("{} - {}", e, chunk.len());
                        }
                    }
                });

        if let Some(bandwidth_kb) = throttle {
            let duration_per_chunk =
                calculate_duration_per_chunk(bandwidth_kb, DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

            log::info!(
                "Throttling enabled: {} KB/s (duration per chunk: {:.3}s)",
                bandwidth_kb,
                duration_per_chunk.as_secs_f64()
            );

            let rate_limited_stream = stream.throttle(duration_per_chunk);

            let body = Body::wrap_stream(rate_limited_stream);

            client.request(method, url).headers(headers).body(body)
        } else {
            let body = Body::wrap_stream(stream);

            client.request(method, url).headers(headers).body(body)
        }
    } else {
        client.request(method, url).headers(headers)
    };

    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn multipart_upload(
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: &Path,
    seek: u64,
    chunk: u64,
    throttle: Option<usize>,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!("HTTP method: {method}, Headers: {headers:#?}");

    let client = Client::builder().user_agent(APP_USER_AGENT).build()?;

    // async read
    let mut file = File::open(&file).await?;

    file.seek(SeekFrom::Start(seek)).await?;

    let file = file.take(chunk);

    log::debug!("Chunk size: {}", chunk);

    let stream =
        FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

    let request = if let Some(bandwidth_kb) = throttle {
        let duration_per_chunk =
            calculate_duration_per_chunk(bandwidth_kb, DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

        log::info!(
            "Throttling enabled: {} KB/s (duration per chunk: {:.3}s)",
            bandwidth_kb,
            duration_per_chunk.as_secs_f64()
        );

        let rate_limited_stream = stream.throttle(duration_per_chunk);

        let body = Body::wrap_stream(rate_limited_stream);

        client.request(method, url).headers(headers).body(body)
    } else {
        let body = Body::wrap_stream(stream);

        client.request(method, url).headers(headers).body(body)
    };

    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn upload(
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    body: Bytes,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!("HTTP method: {method}, Headers: {headers:#?}");

    let client = Client::builder().user_agent(APP_USER_AGENT).build()?;

    let request = client.request(method, url).headers(headers).body(body);

    Ok(request.send().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_duration_per_chunk() {
        let bandwidth_kb_per_sec = 1024;
        let chunk_size = 1024 * 128;

        let duration = calculate_duration_per_chunk(bandwidth_kb_per_sec, chunk_size);

        assert_eq!(duration.as_secs_f64(), 0.125);
    }
}
