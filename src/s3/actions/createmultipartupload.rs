//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::responses::InitiateMultipartUploadResult;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct CreateMultipartUpload<'a> {
    key: &'a str,
    pub x_amz_acl: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_length: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<String>,
    pub x_amz_grant_full_control: Option<String>,
    pub x_amz_grant_read: Option<String>,
    pub x_amz_grant_read_acp: Option<String>,
    pub x_amz_grant_write_acp: Option<String>,
    pub x_amz_server_side_encryption: Option<String>,
    pub x_amz_storage_class: Option<String>,
    pub x_amz_website_redirect_location: Option<String>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<String>,
    pub x_amz_server_side_encryption_customer_key: Option<String>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<String>,
    pub x_amz_server_side_encryption_aws_kms_key_id: Option<String>,
    pub x_amz_server_side_encryption_context: Option<String>,
    pub x_amz_request_payer: Option<String>,
    pub x_amz_tagging: Option<String>,
    pub x_amz_object_lock_mode: Option<String>,
    pub x_amz_object_lock_retain_until_date: Option<String>,
    pub x_amz_object_lock_legal_hold: Option<String>,
}

impl<'a> CreateMultipartUpload<'a> {
    #[must_use]
    pub fn new(key: &'a str) -> Self {
        Self {
            key,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<InitiateMultipartUploadResult> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response = request::request(url.clone(), self.http_verb(), headers, None, None).await?;

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
    fn http_verb(&self) -> &'static str {
        "POST"
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
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
