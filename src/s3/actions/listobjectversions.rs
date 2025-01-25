use crate::{
    s3::actions::{response_error, Action},
    s3::responses::ListVersionsResult,
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct ListObjectVersions<'a> {
    prefix: &'a str,
}

impl<'a> ListObjectVersions<'a> {
    #[must_use]
    pub const fn new(prefix: &'a str) -> Self {
        Self { prefix }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListVersionsResult> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

        if response.status().is_success() {
            let objects: ListVersionsResult = from_str(&response.text().await?)?;

            Ok(objects)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
impl Action for ListObjectVersions<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"GET")?)
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

        // list-type parameter that indicates version 2 of the API
        map.insert("versions", "");

        // prefix parameter
        map.insert("prefix", self.prefix);

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
        let action = ListObjectVersions::new("key");
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

        let action = ListObjectVersions::new("key");
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?prefix=key&versions=",
            url.as_str()
        );

        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
