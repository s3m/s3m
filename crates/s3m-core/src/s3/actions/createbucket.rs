use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::{S3, request, tools},
};
use bytes::Bytes;
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct CreateBucket<'a> {
    acl: &'a str,
    object_lock_enabled: bool,
}

impl<'a> CreateBucket<'a> {
    #[must_use]
    pub const fn new(acl: &'a str, object_lock_enabled: bool) -> Self {
        Self {
            acl,
            object_lock_enabled,
        }
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
        let response = request::upload(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            Bytes::from(xml),
        )
        .await?;

        if response.status().is_success() {
            let mut h: BTreeMap<&str, String> = BTreeMap::new();
            if let Some(location) = response.headers().get("location") {
                h.insert("location", location.to_str()?.to_string());
            }
            Ok(h)
        } else {
            Err(response_error(response).await)
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
impl Action for CreateBucket<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("x-amz-acl", self.acl);

        // Object Lock can only be enabled at bucket creation; it also enables
        // versioning on the bucket.
        // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html>
        if self.object_lock_enabled {
            map.insert("x-amz-bucket-object-lock-enabled", "true");
        }

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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = CreateBucket::new("private", false);
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = CreateBucket::new("private", false);
        let mut map = BTreeMap::new();
        map.insert("x-amz-acl", "private");
        assert_eq!(Some(map), action.headers());
    }

    #[test]
    fn test_headers_object_lock() {
        let action = CreateBucket::new("private", true);
        let headers = action.headers().unwrap();
        assert_eq!(
            headers.get("x-amz-bucket-object-lock-enabled"),
            Some(&"true")
        );

        // header absent when not enabled
        let action = CreateBucket::new("private", false);
        assert_eq!(
            action
                .headers()
                .unwrap()
                .get("x-amz-bucket-object-lock-enabled"),
            None
        );
    }

    #[test]
    fn test_query_pairs() {
        let action = CreateBucket::new("private", false);
        assert_eq!(None, action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = CreateBucket::new("private", false);
        assert_eq!(None, action.path());
    }
}
