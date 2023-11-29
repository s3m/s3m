//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::{
    s3::actions::{response_error, Action},
    s3::responses::InitiateMultipartUploadResult,
    s3::{checksum::Checksum, request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct CreateMultipartUpload<'a> {
    key: &'a str,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    additional_checksum: Option<Checksum>,
}

impl<'a> CreateMultipartUpload<'a> {
    #[must_use]
    pub const fn new(
        key: &'a str,
        acl: Option<String>,
        meta: Option<BTreeMap<String, String>>,
        additional_checksum: Option<Checksum>,
    ) -> Self {
        Self {
            key,
            acl,
            meta,
            additional_checksum,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<InitiateMultipartUploadResult> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None).await?;

        if response.status().is_success() {
            let upload_req: InitiateMultipartUploadResult = from_str(&response.text().await?)?;
            Ok(upload_req)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>
impl<'a> Action for CreateMultipartUpload<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"POST")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        if let Some(acl) = &self.acl {
            map.insert("x-amz-acl", acl);
        }

        if let Some(meta) = &self.meta {
            for (k, v) in meta {
                map.insert(k, v);
            }
        }

        // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>
        if let Some(additional_checksum) = &self.additional_checksum {
            map.insert(
                "x-amz-checksum-algorithm",
                additional_checksum.algorithm.as_algorithm(),
            );
        }

        Some(map)
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // uploads
        map.insert("uploads", "");

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
        let action = CreateMultipartUpload::new("key", None, None, None);
        assert_eq!(Method::POST, action.http_method().unwrap());
    }
}
