use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::responses::ObjectLockConfiguration,
    s3::{S3, request, tools},
};
use quick_xml::de::from_str;
use reqwest::Method;
use std::collections::BTreeMap;

/// `GetObjectLockConfiguration` — read the bucket's Object Lock configuration
/// (including the default retention rule, if any).
#[derive(Debug, Default)]
pub struct GetObjectLockConfiguration;

impl GetObjectLockConfiguration {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ObjectLockConfiguration> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;
        let response = request::request(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            None,
            None,
            None,
        )
        .await?;

        if response.status().is_success() {
            Ok(from_str(&response.text().await?)?)
        } else {
            Err(response_error(response).await)
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html>
impl Action for GetObjectLockConfiguration {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"GET")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("object-lock", "");
        Some(map)
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
    use crate::s3::{Credentials, Region, S3};
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = GetObjectLockConfiguration::new();
        assert_eq!(Method::GET, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs() {
        let action = GetObjectLockConfiguration::new();
        let mut map = BTreeMap::new();
        map.insert("object-lock", "");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = GetObjectLockConfiguration::new();
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?object-lock=",
            url.as_str()
        );
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
