use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::method::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct CreateBucket<'a> {
    acl: &'a str,
}

impl<'a> CreateBucket<'a> {
    #[must_use]
    pub const fn new(acl: &'a str) -> Self {
        Self { acl }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<BTreeMap<&str, String>> {
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>{}</LocationConstraint>
</CreateBucketConfiguration>"#,
            s3.region.name()
        );

        let (url, headers) = &self.sign(s3, tools::sha256_digest(&xml).as_ref(), None, None)?;
        let response =
            request::upload(url.clone(), self.http_method(), headers, Bytes::from(xml)).await?;

        if response.status().is_success() {
            let mut h: BTreeMap<&str, String> = BTreeMap::new();
            if let Some(location) = response.headers().get("location") {
                h.insert("location", location.to_str()?.to_string());
            }
            Ok(h)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
impl<'a> Action for CreateBucket<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"PUT").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("x-amz-acl", self.acl);

        Some(map)
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = CreateBucket::new("private");
        assert_eq!(Method::PUT, action.http_method());
    }
}
