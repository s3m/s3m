//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, Error, Method, Response,
};
use std::collections::BTreeMap;
use std::error;
use url::Url;

/// # Errors
///
/// Will return `Err` if can not make the request
pub async fn request(
    url: Url,
    method: &'static str,
    headers: &BTreeMap<String, String>,
) -> Result<Response, Error> {
    let method = Method::from_bytes(method.as_bytes()).unwrap();
    let headers = headers
        .iter()
        .map(|(k, v)| {
            Ok((
                k.parse::<HeaderName>().unwrap(),
                v.parse::<HeaderValue>().unwrap(),
            ))
        })
        .collect::<Result<HeaderMap, Box<dyn error::Error>>>()
        .unwrap();
    let client = Client::new();
    let request = client.request(method, url).headers(headers);
    //let request = client.request(method, url).headers(headers).body("");
    request.send().await
}
