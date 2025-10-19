use crate::{
    cli::globals::GlobalArgs,
    s3::actions::{Action, response_error},
    s3::{
        S3,
        checksum::{Checksum, sha256_md5_digest},
        request,
    },
};
use anyhow::{Result, anyhow};
use crossbeam::channel::Sender;
use reqwest::Method;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug)]
pub struct PutObject<'a> {
    key: &'a str,
    file: &'a Path,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    sender: Option<Sender<usize>>,
    additional_checksum: Option<Checksum>,
}

impl<'a> PutObject<'a> {
    #[must_use]
    pub const fn new(
        key: &'a str,
        file: &'a Path,
        acl: Option<String>,
        meta: Option<BTreeMap<String, String>>,
        sender: Option<Sender<usize>>,
        additional_checksum: Option<Checksum>,
    ) -> Self {
        Self {
            key,
            file,
            acl,
            meta,
            sender,
            additional_checksum,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3, globals: GlobalArgs) -> Result<BTreeMap<&str, String>> {
        let (sha, md5, length) = sha256_md5_digest(self.file).await?;

        let (url, headers) = &self.sign(s3, sha.as_ref(), Some(md5.as_ref()), Some(length))?;

        let response = request::request(
            url.clone(),
            self.http_method()?,
            headers,
            Some(Path::new(self.file)),
            self.sender,
            globals.throttle,
        )
        .await?;

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

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl Action for PutObject<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
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
                additional_checksum.algorithm.as_amz(),
                &additional_checksum.checksum,
            );
        }

        Some(map)
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{
        tools, {Credentials, Region, S3},
    };
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = PutObject::new("key", Path::new("/"), None, None, None, None);
        assert_eq!(Method::PUT, action.http_method().unwrap());
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
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        for (acl, expected) in test {
            let action = PutObject::new(
                "key",
                Path::new("/"),
                Some(acl.to_string()),
                None,
                None,
                None,
            );
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
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = PutObject::new("key", Path::new("/"), None, None, None, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key",
            url.as_str()
        );
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
