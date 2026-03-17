//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use anyhow::Result;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use reqwest::{
    Body, Client, Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{collections::BTreeMap, path::Path};
use tokio::time::Duration;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::mpsc::UnboundedSender,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

const DEFAULT_FRAMED_CHUNK_SIZE_BYTES: usize = 1024 * 128;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

pub struct MultipartRequest<'a> {
    pub client: &'a Client,
    pub url: Url,
    pub method: reqwest::Method,
    pub headers: &'a BTreeMap<String, String>,
    pub file: &'a Path,
    pub seek: u64,
    pub chunk: u64,
    pub throttle: Option<usize>,
}

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
    // Calculate duration in nanoseconds using integer arithmetic
    // duration = (chunk_size * 1_000_000_000) / bandwidth_bytes_per_sec
    let nanos = (u128::from(chunk_size as u64) * 1_000_000_000)
        / u128::from(bandwidth_bytes_per_sec as u64);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    client: &Client,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    sender: Option<UnboundedSender<usize>>,
    throttle: Option<usize>,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!("HTTP method: {method}, Headers: {headers:#?}");

    let request = if let Some(file_path) = file {
        let file = File::open(file_path).await?;

        let stream = {
            log::debug!("Sender(channel) is_some: {}", sender.is_some());

            FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES)
                .inspect_ok(move |chunk| {
                    if let Some(tx) = &sender {
                        log::trace!("Sending {} bytes", chunk.len());

                        if tx.send(chunk.len()).is_err() {
                            log::trace!("Progress receiver dropped");
                        }
                    }
                })
        };

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

            client
                .request(method, url)
                .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
                .headers(headers)
                .body(body)
        } else {
            let body = Body::wrap_stream(stream);

            client
                .request(method, url)
                .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
                .headers(headers)
                .body(body)
        }
    } else {
        client
            .request(method, url)
            .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
            .headers(headers)
    };

    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn multipart_upload(request: MultipartRequest<'_>) -> Result<Response> {
    let headers = request
        .headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    log::debug!("HTTP method: {}, Headers: {headers:#?}", request.method);

    // async read
    let mut file = File::open(request.file).await?;

    file.seek(SeekFrom::Start(request.seek)).await?;

    let file = file.take(request.chunk);

    log::debug!("Chunk size: {}", request.chunk);

    let stream =
        FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

    let request = if let Some(bandwidth_kb) = request.throttle {
        let duration_per_chunk =
            calculate_duration_per_chunk(bandwidth_kb, DEFAULT_FRAMED_CHUNK_SIZE_BYTES);

        log::info!(
            "Throttling enabled: {} KB/s (duration per chunk: {:.3}s)",
            bandwidth_kb,
            duration_per_chunk.as_secs_f64()
        );

        let rate_limited_stream = stream.throttle(duration_per_chunk);

        let body = Body::wrap_stream(rate_limited_stream);

        request
            .client
            .request(request.method, request.url)
            .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
            .headers(headers)
            .body(body)
    } else {
        let body = Body::wrap_stream(stream);

        request
            .client
            .request(request.method, request.url)
            .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
            .headers(headers)
            .body(body)
    };

    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn upload(
    client: &Client,
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

    let request = client
        .request(method, url)
        .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
        .headers(headers)
        .body(body);

    Ok(request.send().await?)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    clippy::cast_precision_loss,
    clippy::large_stack_arrays,
    clippy::missing_errors_doc,
    clippy::float_cmp
)]
mod tests {
    use super::*;
    use mockito::Server;
    use reqwest::StatusCode;

    #[test]
    fn test_calculate_duration_per_chunk() {
        let bandwidth_kb_per_sec = 1024;
        let chunk_size = 1024 * 128;

        let duration = calculate_duration_per_chunk(bandwidth_kb_per_sec, chunk_size);

        assert_eq!(duration.as_secs_f64(), 0.125);
    }

    #[tokio::test]
    async fn test_upload_get() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/test")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("test data")
            .create_async()
            .await;

        let headers = BTreeMap::new();
        let body = Bytes::from("test data");
        let client = Client::new();

        let url = format!("{}/test", server.url()).parse::<Url>().unwrap();

        let response = upload(&client, url, reqwest::Method::GET, &headers, body).await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upload_post() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("POST", "/test")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("test data")
            .create_async()
            .await;

        let headers = BTreeMap::new();
        let body = Bytes::from("test data");
        let client = Client::new();

        let url = format!("{}/test", server.url()).parse::<Url>().unwrap();

        let response = upload(&client, url, reqwest::Method::POST, &headers, body).await;

        println!("Response: {response:?}");

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_upload_reuses_same_client_for_multiple_requests() {
        let mut server = Server::new_async().await;
        let _first = server
            .mock("POST", "/first")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("first")
            .create_async()
            .await;
        let _second = server
            .mock("POST", "/second")
            .with_status(200)
            .match_header("User-Agent", APP_USER_AGENT)
            .match_body("second")
            .create_async()
            .await;

        let client = Client::new();
        let headers = BTreeMap::new();

        let first_url = format!("{}/first", server.url()).parse::<Url>().unwrap();
        let second_url = format!("{}/second", server.url()).parse::<Url>().unwrap();

        let first = upload(
            &client,
            first_url,
            reqwest::Method::POST,
            &headers,
            Bytes::from("first"),
        )
        .await
        .unwrap();
        let second = upload(
            &client,
            second_url,
            reqwest::Method::POST,
            &headers,
            Bytes::from("second"),
        )
        .await
        .unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
    }
}
