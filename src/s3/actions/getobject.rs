use crate::{
    cli::globals::GlobalArgs,
    s3::actions::{Action, response_error},
    s3::{S3, request, tools},
};
use anyhow::{Result, anyhow};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct GetObject<'a> {
    key: &'a str,
    pub part_number: Option<String>,
    pub version_id: Option<String>,
    pub response_cache_control: Option<String>,
}

impl<'a> GetObject<'a> {
    #[must_use]
    pub fn new(key: &'a str, version_id: Option<String>) -> Self {
        Self {
            key,
            version_id,
            ..Self::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3, globals: &GlobalArgs) -> Result<reqwest::Response> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response = request::request(
            url.clone(),
            self.http_method()?,
            headers,
            None,
            None,
            globals.throttle,
        )
        .await?;

        if response.status().is_success() {
            Ok(response)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
impl Action for GetObject<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"GET")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
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

        if let Some(response_cache_control) = &self.response_cache_control {
            map.insert("response-cache-control", response_cache_control);
        }

        Some(map)
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
    use crate::s3::{
        tools, {Credentials, Region, S3},
    };
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = GetObject::new("key", None);
        assert_eq!(Method::GET, action.http_method().unwrap());
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

        let action = GetObject::new("key", None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key",
            url.as_str()
        );

        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }

    #[test]
    fn test_version_id() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = GetObject::new("key", Some("123".to_string()));
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?versionId=123",
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
