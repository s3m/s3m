use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::responses::ListMultipartUploadsResult;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use http::method::Method;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListMultipartUploads {}

impl ListMultipartUploads {
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListMultipartUploadsResult> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response =
            request::request(url.clone(), self.http_method(), headers, None, None).await?;
        if response.status().is_success() {
            let uploads: ListMultipartUploadsResult = from_str(&response.text().await?)?;
            Ok(uploads)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
impl Action for ListMultipartUploads {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"GET").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        None
    }

    // URL query pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // uploads
        map.insert("uploads", "");

        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = ListMultipartUploads::new();
        assert_eq!(Method::GET, action.http_method());
    }
}
