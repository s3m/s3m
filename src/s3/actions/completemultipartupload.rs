//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::responses::CompleteMultipartUploadResult;
use crate::s3::S3;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Default)]
pub struct CompleteMultipartUpload {
    key: String,
    upload_id: String,
    pub x_amz_request_payer: Option<String>,
}

impl CompleteMultipartUpload {
    #[must_use]
    pub fn new(key: String, upload_id: String) -> Self {
        Self {
            key,
            upload_id,
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
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None)?;
        let response = request::request(url.clone(), self.http_verb(), headers, None).await?;

        if response.status().is_success() {
            let upload_req: CompleteMultipartUploadResult = from_str(&response.text().await?)?;
            Ok(upload_req)
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
