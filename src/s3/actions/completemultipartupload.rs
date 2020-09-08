//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::responses::CompleteMultipartUploadResult;
use crate::s3::tools;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::method::Method;
use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};
use serde_xml_rs::{from_str, to_string};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct CompleteMultipartUpload<'a> {
    key: &'a str,
    upload_id: &'a str,
    pub x_amz_request_payer: Option<String>,
    parts: BTreeMap<u16, Part>,
}

impl<'a> Serialize for CompleteMultipartUpload<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let len = 1 + self.parts.len();
        let mut map = serializer.serialize_struct("CompleteMultipartUpload", len)?;
        for part in self.parts.values() {
            map.serialize_field("Part", part)?;
        }
        map.end()
    }
}

#[derive(Debug, Default, Clone)]
pub struct Part {
    pub etag: String,
    pub number: u16,
}

impl Serialize for Part {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("ETag", &self.etag)?;
        map.serialize_entry("PartNumber", &self.number)?;
        map.end()
    }
}

impl<'a> CompleteMultipartUpload<'a> {
    #[must_use]
    pub fn new(key: &'a str, upload_id: &'a str, parts: BTreeMap<u16, Part>) -> Self {
        Self {
            key,
            upload_id,
            parts,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<CompleteMultipartUploadResult> {
        let parts = CompleteMultipartUpload {
            parts: self.parts.clone(),
            ..Default::default()
        };
        let body = to_string(&parts)?;
        let digest = tools::sha256_digest(&body);
        let (url, headers) = &self.sign(s3, &digest, None, Some(body.len()))?;
        let response =
            request::upload(url.clone(), self.http_method(), headers, Bytes::from(body)).await?;

        if response.status().is_success() {
            let rs: CompleteMultipartUploadResult = from_str(&response.text().await?)?;
            Ok(rs)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>
impl<'a> Action for CompleteMultipartUpload<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"POST").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    // URL query_pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("uploadId", self.upload_id);
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
        let parts: BTreeMap<u16, Part> = BTreeMap::new();
        let action = CompleteMultipartUpload::new("key", "uid", parts);
        assert_eq!(Method::POST, action.http_method());
    }
}
