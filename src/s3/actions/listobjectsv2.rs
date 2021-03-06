use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::responses::ListBucketResult;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use http::method::Method;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListObjectsV2 {
    pub continuation_token: Option<String>,
    pub delimiter: Option<String>,
    pub fetch_owner: Option<bool>,
    pub prefix: Option<String>,
    pub start_after: Option<String>,
}

impl ListObjectsV2 {
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListBucketResult> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response =
            request::request(url.clone(), self.http_method(), headers, None, None).await?;
        if response.status().is_success() {
            let objects: ListBucketResult = from_str(&response.text().await?)?;
            Ok(objects)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
impl Action for ListObjectsV2 {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"GET").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        None
    }

    // URL query pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // list-type parameter that indicates version 2 of the API
        map.insert("list-type", "2");

        if let Some(token) = &self.continuation_token {
            map.insert("continuation-token", token);
        }

        if let Some(delimiter) = &self.delimiter {
            map.insert("delimiter", delimiter);
        }

        if self.fetch_owner.is_some() {
            map.insert("fetch-owner", "true");
        }

        if let Some(prefix) = &self.prefix {
            map.insert("prefix", prefix);
        }

        if let Some(sa) = &self.start_after {
            map.insert("start-after", sa);
        }

        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = ListObjectsV2::new();
        assert_eq!(Method::GET, action.http_method());
    }
}
