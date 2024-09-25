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
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

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
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{
        checksum::{Checksum, ChecksumAlgorithm},
        Credentials, Region, S3,
    };
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = CreateMultipartUpload::new("key", None, None, None);
        assert_eq!(Method::POST, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = CreateMultipartUpload::new("key", None, None, None);
        let headers = action.headers().unwrap();
        assert_eq!(None, headers.get("x-amz-acl"));
        assert_eq!(None, headers.get("x-amz-meta-"));
        assert_eq!(None, headers.get("x-amz-checksum-algorithm"));
    }

    #[test]
    fn test_headers_acl() {
        let test = vec![
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ];
        for acl in test {
            let action = CreateMultipartUpload::new("key", Some(acl.to_string()), None, None);
            let headers = action.headers().unwrap();
            assert_eq!(Some(acl), headers.get("x-amz-acl").copied());
        }
    }

    #[test]
    fn test_headers_with_checksum() {
        let test = vec![
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ];
        for algorithm in test {
            let action = CreateMultipartUpload::new(
                "key",
                None,
                None,
                Some(Checksum::new(algorithm.clone())),
            );
            let headers = action.headers().unwrap();
            assert_eq!(None, headers.get("x-amz-acl"));
            assert_eq!(
                headers.get("x-amz-checksum-algorithm").unwrap(),
                &algorithm.as_algorithm()
            );
        }
    }

    #[test]
    fn test_query_pairs() {
        let action = CreateMultipartUpload::new("key", None, None, None);
        let mut map = BTreeMap::new();
        map.insert("uploads", "");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = CreateMultipartUpload::new("key", None, None, None);
        assert_eq!(Some(vec!["key"]), action.path());
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let action = CreateMultipartUpload::new("key", None, None, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?uploads=",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential="));
    }
}
