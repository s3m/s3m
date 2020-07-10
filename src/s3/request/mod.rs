//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, Error, Response,
};
use std::collections::BTreeMap;
use std::error;
use url::Url;

pub async fn request(
    url: Url,
    method: http::method::Method,
    headers: &BTreeMap<String, String>,
) -> Result<Response, Error> {
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
