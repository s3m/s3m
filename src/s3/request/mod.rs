//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body, Client, Method, Response,
};
use std::collections::BTreeMap;
use std::error;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    url: Url,
    method: &'static str,
    headers: &BTreeMap<String, String>,
    body: Option<String>,
) -> Result<Response, Box<dyn error::Error>> {
    let method = Method::from_bytes(method.as_bytes())?;
    let headers = headers
        .iter()
        .map(|(k, v)| Ok((k.parse::<HeaderName>()?, v.parse::<HeaderValue>()?)))
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()?;

    let client = Client::new();

    let request = if let Some(file) = body {
        let async_read = File::open(file).await?;
        let stream = FramedRead::new(async_read, BytesCodec::new());
        let body = Body::wrap_stream(stream);
        client.request(method, url).headers(headers).body(body)
    } else {
        client.request(method, url).headers(headers)
    };

    //  println!("request: {:#?}", request);

    match request.send().await {
        Ok(r) => Ok(r),
        Err(e) => Err(Box::new(e)),
    }
}
