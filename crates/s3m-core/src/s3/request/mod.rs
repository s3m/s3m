//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::s3::error::Result;
use bytes::Bytes;
use futures::stream::TryStreamExt;
use reqwest::{
    Body, Client, Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::codec::{BytesCodec, FramedRead};
use upload_stream::paced;
use url::Url;

mod upload_stream;

const DEFAULT_FRAMED_CHUNK_SIZE_BYTES: usize = 1024 * 128;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

pub type ProgressCallback = Arc<dyn Fn(usize) + Send + Sync + 'static>;

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

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    client: &Client,
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    progress: Option<ProgressCallback>,
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
            log::debug!("Progress callback is_some: {}", progress.is_some());

            FramedRead::with_capacity(file, BytesCodec::new(), DEFAULT_FRAMED_CHUNK_SIZE_BYTES)
                .inspect_ok(move |chunk| {
                    if let Some(callback) = &progress {
                        log::trace!("Sending {} bytes", chunk.len());
                        callback(chunk.len());
                    }
                })
        };

        let body = Body::wrap_stream(paced(stream, throttle));
        client
            .request(method, url)
            .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
            .headers(headers)
            .body(body)
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

    let body = Body::wrap_stream(paced(stream, request.throttle));
    let request = request
        .client
        .request(request.method, request.url)
        .header(reqwest::header::USER_AGENT, APP_USER_AGENT)
        .headers(headers)
        .body(body);

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
    use super::upload_stream::calculate_duration_per_chunk;
    use super::*;
    use mockito::Server;
    use reqwest::StatusCode;
    use tokio::time::Duration;

    #[test]
    fn test_calculate_duration_per_chunk() {
        let chunk_size = 1024 * 128;
        // (KB/s, expected delay between 128 KiB chunks)
        let cases = [
            (1, Duration::from_secs(128)),
            (3, Duration::from_nanos(42_666_666_666)),
            (1024, Duration::from_millis(125)),
            (102_400, Duration::from_micros(1_250)),
            // KB/s values whose byte rate overflows usize mean "no delay"
            (usize::MAX / 1000, Duration::ZERO),
            (usize::MAX, Duration::ZERO),
        ];

        for (bandwidth_kb_per_sec, expected) in cases {
            assert_eq!(
                calculate_duration_per_chunk(bandwidth_kb_per_sec, chunk_size),
                expected,
                "{bandwidth_kb_per_sec} KB/s"
            );
        }
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
