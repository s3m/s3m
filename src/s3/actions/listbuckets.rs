use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::responses::ListAllMyBucketsResult;
use crate::s3::S3;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Default)]
pub struct ListBuckets {}

impl ListBuckets {
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: S3) -> Result<ListAllMyBucketsResult, Box<dyn error::Error>> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None)?;
        let response = request::request(url.clone(), self.http_verb(), headers, None).await?;
        if response.status().is_success() {
            let buckets: ListAllMyBucketsResult = from_str(&response.text().await?)?;
            Ok(buckets)
        } else {
            Err(response_error(response).await?.into())
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
impl Action for ListBuckets {
    fn http_verb(&self) -> &'static str {
        "GET"
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        None
    }

    // URL query pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }
}
