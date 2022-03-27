use crate::{
    s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256},
    s3::{request, S3},
};
use anyhow::{anyhow, Result};
use http::method::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct GetObjectAcl<'a> {
    key: &'a str,
}

impl<'a> GetObjectAcl<'a> {
    #[must_use]
    pub const fn new(key: &'a str) -> Self {
        Self { key }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<reqwest::Response> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response =
            request::request(url.clone(), self.http_method(), headers, None, None).await?;

        if response.status().is_success() {
            Ok(response)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl<'a> Action for GetObjectAcl<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"GET").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("acl", "");

        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        // remove leading / or //
        let clean_path = self
            .key
            .split('/')
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();
        Some(clean_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = GetObjectAcl::new("key");
        assert_eq!(Method::PUT, action.http_method());
    }
}
