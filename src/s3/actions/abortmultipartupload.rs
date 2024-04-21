//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct AbortMultipartUpload<'a> {
    key: &'a str,
    upload_id: &'a str,
}

impl<'a> AbortMultipartUpload<'a> {
    #[must_use]
    pub const fn new(key: &'a str, upload_id: &'a str) -> Self {
        Self { key, upload_id }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

        if response.status().is_success() {
            Ok(response.text().await?)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
impl<'a> Action for AbortMultipartUpload<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"DELETE")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // uploadId - Upload ID that identifies the multipart upload.
        map.insert("uploadId", self.upload_id);

        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use secrecy::Secret;

    #[test]
    fn test_method() {
        let action = AbortMultipartUpload::new("key", "uid");
        assert_eq!(Method::DELETE, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs() {
        let action = AbortMultipartUpload::new("key", "uid");
        let mut map = BTreeMap::new();
        map.insert("uploadId", "uid");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = AbortMultipartUpload::new("key", "uid");
        assert_eq!(Some(vec!["key"]), action.path());
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let action = AbortMultipartUpload::new("key", "uid");
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?uploadId=uid",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
