//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client, Method, Response,
};
use std::collections::BTreeMap;
use std::error;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::prelude::*;
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
) -> Result<Response, Box<dyn error::Error>> {
    let method = Method::from_bytes(method.as_bytes())?;
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()?;

    let client = Client::new();

    let request = if let Some(file_path) = file {
        let file = File::open(file_path).await?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let body = Body::wrap_stream(stream);
        client.request(method, url).headers(headers).body(body)
    } else {
        client.request(method, url).headers(headers)
    };

    //  println!("request: {:#?}", request);

    Ok(request.send().await?)
}

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request_multipart(
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
pub async fn request_body(
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
