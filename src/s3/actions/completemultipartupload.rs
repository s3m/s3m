//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::responses::CompleteMultipartUploadResult;
use crate::s3::tools;
use crate::s3::S3;
use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};
use serde_xml_rs::{from_str, to_string};
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Default, Clone)]
pub struct CompleteMultipartUpload {
    key: String,
    upload_id: String,
    pub x_amz_request_payer: Option<String>,
    parts: BTreeMap<u16, Part>,
}

impl Serialize for CompleteMultipartUpload {
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
    pub seek: u64,
    pub chunk: u64,
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

impl CompleteMultipartUpload {
    #[must_use]
    pub fn new(key: String, upload_id: String, parts: BTreeMap<u16, Part>) -> Self {
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
    pub async fn request(
        &self,
        s3: S3,
    ) -> Result<CompleteMultipartUploadResult, Box<dyn error::Error>> {
        let parts = CompleteMultipartUpload {
            parts: self.parts.clone(),
            ..Default::default()
        };
        let body = to_string(&parts).unwrap();
        let digest = tools::sha256_digest_string(&body);
        let (url, headers) = &self.sign(s3, &digest, Some(body.len()))?;
        let response = request::request_body(url.clone(), self.http_verb(), headers, body).await?;

        if response.status().is_success() {
            let rs: CompleteMultipartUploadResult = from_str(&response.text().await?)?;
            Ok(rs)
        } else {
            Err(response_error(response).await?.into())
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>
impl Action for CompleteMultipartUpload {
    fn http_verb(&self) -> &'static str {
        "POST"
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    // URL query_pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("uploadId", &self.upload_id);
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
