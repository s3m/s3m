use crate::s3::actions::{response_error, Action, EMPTY_PAYLOAD_SHA256};
use crate::s3::request;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use http::method::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct HeadObject<'a> {
    key: &'a str,
    pub part_number: Option<String>,
    pub version_id: Option<String>,
}

impl<'a> HeadObject<'a> {
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
    pub async fn request(&self, s3: &S3) -> Result<BTreeMap<String, String>> {
        let (url, headers) = &self.sign(s3, EMPTY_PAYLOAD_SHA256, None, None)?;
        let response =
            request::request(url.clone(), self.http_method(), headers, None, None).await?;
        if response.status().is_success() {
            let mut h: BTreeMap<String, String> = BTreeMap::new();
            for (key, value) in response.headers() {
                if !value.is_empty() {
                    h.insert(key.as_str().to_string(), value.to_str()?.to_string());
                }
            }
            Ok(h)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
impl<'a> Action for HeadObject<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"HEAD").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
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

    // URL query pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        if let Some(pn) = &self.part_number {
            map.insert("partNumber", pn);
        }

        if let Some(vid) = &self.version_id {
            map.insert("versionId", vid);
        }

        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = HeadObject::new("key");
        assert_eq!(Method::HEAD, action.http_method());
    }
}
