//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

// https://stackoverflow.com/a/63374116/1135424
// use futures::future::Either;
// use futures::stream::TryStreamExt;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client, Method, Response,
};
use std::collections::BTreeMap;
use std::error;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    url: Url,
    method: &'static str,
    headers: &BTreeMap<String, String>,
    file: Option<String>,
    sender: Option<UnboundedSender<usize>>,
) -> Result<Response, Box<dyn error::Error>> {
    let method = Method::from_bytes(method.as_bytes())?;
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()?;

    let client = Client::new();

    let request = if let Some(file_path) = file {
        let file = File::open(file_path).await?;
        let mut stream = FramedRead::new(file, BytesCodec::new());
        let stream = async_stream::stream! {
            if let Some(tx) = sender {
                while let Some(bytes) = stream.next().await {
                    if let Ok(bytes) = &bytes {
                        // TODO
                        tx.send(bytes.len()).unwrap();
                    }
                    yield bytes;
                }
            } else {
                while let Some(bytes) = stream.next().await {
                    yield bytes;
                }
            }
        };
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
pub async fn multipart(
    url: Url,
    method: &'static str,
    headers: &BTreeMap<String, String>,
    file: String,
    seek: u64,
    chunk: u64,
) -> Result<Response, Box<dyn error::Error>> {
    let method = Method::from_bytes(method.as_bytes())?;
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()?;

    let client = Client::new();

    // async read
    let mut file = File::open(&file).await?;
    file.seek(SeekFrom::Start(seek)).await?;
    let file = file.take(chunk);
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    let request = client.request(method, url).headers(headers).body(body);
    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn body(
    url: Url,
    method: &'static str,
    headers: &BTreeMap<String, String>,
    body: String,
) -> Result<Response, Box<dyn error::Error>> {
    let method = Method::from_bytes(method.as_bytes())?;
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()?;

    let client = Client::new();
    let request = client.request(method, url).headers(headers).body(body);
    Ok(request.send().await?)
}
