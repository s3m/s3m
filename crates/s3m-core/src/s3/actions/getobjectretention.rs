use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::responses::ObjectRetention,
    s3::{S3, request, tools},
};
use quick_xml::de::from_str;
use reqwest::Method;
use std::collections::BTreeMap;

/// `GetObjectRetention` — read the retention mode/date of an object version.
#[derive(Debug)]
pub struct GetObjectRetention<'a> {
    key: &'a str,
    version_id: Option<String>,
}

impl<'a> GetObjectRetention<'a> {
    #[must_use]
    pub const fn new(key: &'a str, version_id: Option<String>) -> Self {
        Self { key, version_id }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ObjectRetention> {
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

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html>
impl Action for GetObjectRetention<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"GET")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("retention", "");
        if let Some(version_id) = &self.version_id {
            map.insert("versionId", version_id);
        }
        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
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
        let action = GetObjectRetention::new("key", None);
        assert_eq!(Method::GET, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs_versioned() {
        let action = GetObjectRetention::new("key", Some("v2".to_string()));
        let pairs = action.query_pairs().unwrap();
        assert_eq!(pairs.get("retention"), Some(&""));
        assert_eq!(pairs.get("versionId"), Some(&"v2"));
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

        let action = GetObjectRetention::new("key", None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?retention=",
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
