use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct PutObjectAcl<'a> {
    key: &'a str,
    acl: &'a str,
}

impl<'a> PutObjectAcl<'a> {
    #[must_use]
    pub const fn new(key: &'a str, acl: &'a str) -> Self {
        Self { key, acl }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<BTreeMap<&str, String>> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;
        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None).await?;

        if response.status().is_success() {
            let mut h: BTreeMap<&str, String> = BTreeMap::new();
            if let Some(etag) = response.headers().get("ETag") {
                h.insert("ETag", etag.to_str()?.to_string());
            }
            if let Some(vid) = response.headers().get("x-amz-version-id") {
                h.insert("Version ID", vid.to_str()?.to_string());
            }
            if let Some(sse) = response.headers().get("x-amz-server-side-encryption") {
                h.insert("Server-side encryption", sse.to_str()?.to_string());
            }
            if let Some(exp) = response.headers().get("x-amz-expiration") {
                h.insert("Expiration", exp.to_str()?.to_string());
            }
            if let Some(pos) = response.headers().get("x-emc-previous-object-size") {
                h.insert("Previous object size", pos.to_str()?.to_string());
            }
            Ok(h)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html>
impl<'a> Action for PutObjectAcl<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("x-amz-acl", self.acl);

        Some(map)
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
    use crate::s3::{Credentials, Region, S3};

    #[test]
    fn test_method() {
        let action = PutObjectAcl::new("key", "public-read");
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = PutObjectAcl::new("key", "public-read");
        let mut map = BTreeMap::new();
        map.insert("x-amz-acl", "public-read");
        assert_eq!(Some(map), action.headers());
    }

    #[test]
    fn test_headers_acl() {
        let test = vec![
            ("private", "private"),
            ("public-read", "public-read"),
            ("public-read-write", "public-read-write"),
            ("authenticated-read", "authenticated-read"),
            ("aws-exec-read", "aws-exec-read"),
            ("bucket-owner-read", "bucket-owner-read"),
            ("bucket-owner-full-control", "bucket-owner-full-control"),
        ];

        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
        );

        for (acl, expected) in test {
            let action = PutObjectAcl::new("key", acl);
            let (_, headers) = action
                .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
                .unwrap();
            assert_eq!(expected, headers.get("x-amz-acl").unwrap());
        }
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
        );

        let action = PutObjectAcl::new("key", "public-read");
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?acl=",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
