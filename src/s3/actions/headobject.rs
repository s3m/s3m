use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
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
            ..Self::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<BTreeMap<String, String>> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;
        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;
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
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"HEAD")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("x-amz-checksum-mode", "ENABLED");

        Some(map)
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

        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{
        tools, {Credentials, Region, S3},
    };
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = HeadObject::new("key");
        assert_eq!(Method::HEAD, action.http_method().unwrap());
    }

    #[test]
    fn test_headers() {
        let action = HeadObject::new("key");
        let mut headers = BTreeMap::new();
        headers.insert("x-amz-checksum-mode", "ENABLED");
        assert_eq!(headers, action.headers().unwrap());
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

        let action = HeadObject::new("key");
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
