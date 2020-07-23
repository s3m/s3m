use crate::s3::actions::Action;
use crate::s3::actions::EMPTY_PAYLOAD_SHA256;
use crate::s3::request;
use crate::s3::responses::{ErrorResponse, ListAllMyBucketsResult};
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
        //let rs = response.text().await?;
        if response.status().is_success() {
            let buckets: ListAllMyBucketsResult = from_str(&response.text().await?)?;
            Ok(buckets)
        } else {
            let mut error: BTreeMap<&str, String> = BTreeMap::new();
            error.insert("StatusCode", response.status().as_str().to_string());
            if let Some(x_amz_id_2) = response.headers().get("x-amz-id-2") {
                error.insert("x-amz-id-2", x_amz_id_2.to_str()?.to_string());
            }

            if let Some(rid) = response.headers().get("x-amz-request-id") {
                error.insert("Request ID", rid.to_str()?.to_string());
            }

            let body = response.text().await?;

            if let Ok(e) = from_str::<ErrorResponse>(&body) {
                error.insert("Code", e.code);
                error.insert("Message", e.message);
            } else {
                error.insert("Response", body);
            }

            Err(error
                .iter()
                .map(|(k, v)| format!("{}: {}\n", k, v))
                .collect::<String>())?
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
