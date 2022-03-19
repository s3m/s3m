//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::{
    s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256},
    s3::{request, S3},
};
use anyhow::{anyhow, Result};
use http::method::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct DeleteObject<'a> {
    key: &'a str,
    version_id: Option<String>,
}

impl<'a> DeleteObject<'a> {
    #[must_use]
    pub fn new(key: &'a str) -> Self {
        Self {
            key,
            ..Self::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response =
            request::request(url.clone(), self.http_method(), headers, None, None).await?;

        if response.status().is_success() {
            Ok(response.text().await?)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
impl<'a> Action for DeleteObject<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"DELETE").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        if let Some(vid) = &self.version_id {
            map.insert("versionId", vid);
        }

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
        let action = DeleteObject::new("key");
        assert_eq!(Method::DELETE, action.http_method());
    }
}
