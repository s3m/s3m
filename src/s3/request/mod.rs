//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

// https://stackoverflow.com/a/63374116/1135424
use anyhow::Result;
use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::stream::TryStreamExt;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client, Response,
};
use std::{collections::BTreeMap, io::SeekFrom, path::Path};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    url: Url,
    method: reqwest::Method,
    headers: &BTreeMap<String, String>,
    file: Option<&Path>,
    sender: Option<Sender<usize>>,
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    let client = Client::new();

    let request = if let Some(file_path) = file {
        let file = File::open(file_path).await?;
        let stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 128).inspect_ok(
            move |chunk| {
                if let Some(tx) = &sender {
                    if let Err(e) = tx.send(chunk.len()) {
                        eprintln!("{} - {}", e, chunk.len());
                    }
                }
            },
        );
        let body = Body::wrap_stream(stream);
        client.request(method, url).headers(headers).body(body)
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
) -> Result<Response> {
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap>>()?;

    let client = Client::new();

    // async read
    let mut file = File::open(&file).await?;
    file.seek(SeekFrom::Start(seek)).await?;
    let file = file.take(chunk);
    let stream = FramedRead::with_capacity(file, BytesCodec::new(), 1024 * 128);
    let body = Body::wrap_stream(stream);
    let request = client.request(method, url).headers(headers).body(body);
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

    let client = Client::new();
    let request = client.request(method, url).headers(headers).body(body);
    Ok(request.send().await?)
}
