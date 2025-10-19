use crate::{
    s3::actions::{Action, response_error},
    s3::responses::ListBucketResult,
    s3::{S3, request, tools},
};
use anyhow::{Result, anyhow};
use quick_xml::de::from_str;
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListObjectsV2 {
    pub continuation_token: Option<String>,
    pub delimiter: Option<String>,
    pub fetch_owner: Option<bool>,
    pub prefix: Option<String>,
    pub start_after: Option<String>,
    pub max_keys: Option<String>,
}

impl ListObjectsV2 {
    #[must_use]
    pub fn new(
        prefix: Option<String>,
        start_after: Option<String>,
        max_keys: Option<String>,
    ) -> Self {
        Self {
            prefix,
            start_after,
            max_keys,
            ..Self::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListBucketResult> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

        if response.status().is_success() {
            let objects: ListBucketResult = from_str(&response.text().await?)?;
            Ok(objects)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
impl Action for ListObjectsV2 {
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
        map.insert("list-type", "2");

        if let Some(max_keys) = &self.max_keys {
            map.insert("max-keys", max_keys);
        }

        if let Some(token) = &self.continuation_token {
            map.insert("continuation-token", token);
        }

        if let Some(delimiter) = &self.delimiter {
            map.insert("delimiter", delimiter);
        }

        if self.fetch_owner.is_some() {
            map.insert("fetch-owner", "true");
        }

        if let Some(prefix) = &self.prefix {
            map.insert("prefix", prefix);
        }

        if let Some(sa) = &self.start_after {
            map.insert("start-after", sa);
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
        let action = ListObjectsV2::new(
            Some(String::from("prefix")),
            Some(String::from("start-after")),
            None,
        );
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

        let action = ListObjectsV2::new(None, None, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?list-type=2",
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
    fn test_max_keys() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = ListObjectsV2::new(None, None, Some("10".to_string()));
        println!("{:?}", action.query_pairs());
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?list-type=2&max-keys=10",
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
