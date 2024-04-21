use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct DeleteObject<'a> {
    key: &'a str,
    version_id: Option<String>,
}

impl<'a> DeleteObject<'a> {
    #[must_use]
    pub fn new(key: &'a str) -> Self {
        Self {
            key,
            ..Self::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

        if response.status().is_success() {
            Ok(response.text().await?)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
impl<'a> Action for DeleteObject<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"DELETE")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        if let Some(vid) = &self.version_id {
            map.insert("versionId", vid);
        }

        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use secrecy::Secret;

    #[test]
    fn test_method() {
        let action = DeleteObject::new("key");
        assert_eq!(Method::DELETE, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = DeleteObject::new("key");
        assert_eq!(None, action.headers());
    }

    #[test]
    fn test_query_pairs() {
        let action = DeleteObject::new("key");
        assert!(action.query_pairs().is_some());
    }

    #[test]
    fn test_path() {
        let action = DeleteObject::new("key");
        assert_eq!(Some(vec!["key"]), action.path());
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let action = DeleteObject::new("key");
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
