use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct DeleteBucket {}

impl DeleteBucket {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
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

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
impl Action for DeleteBucket {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"DELETE")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
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
    use crate::s3::{Credentials, Region, S3};
    use secrecy::Secret;

    #[test]
    fn test_method() {
        let action = DeleteBucket::new();
        assert_eq!(Method::DELETE, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = DeleteBucket::new();
        assert_eq!(None, action.headers());
    }

    #[test]
    fn test_query_pairs() {
        let action = DeleteBucket::new();
        assert_eq!(None, action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = DeleteBucket::new();
        assert_eq!(None, action.path());
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
        let action = DeleteBucket::new();
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1",
            url.as_str()
        );

        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
