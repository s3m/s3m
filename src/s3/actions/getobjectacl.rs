use crate::{
    s3::actions::{Action, response_error},
    s3::{S3, request, tools},
};
use anyhow::{Result, anyhow};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct GetObjectAcl<'a> {
    key: &'a str,
}

impl<'a> GetObjectAcl<'a> {
    #[must_use]
    pub const fn new(key: &'a str) -> Self {
        Self { key }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<reqwest::Response> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;
        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;

        if response.status().is_success() {
            Ok(response)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl Action for GetObjectAcl<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"GET")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        // URL query_pairs
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        map.insert("acl", "");

        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = GetObjectAcl::new("key");
        assert_eq!(Method::GET, action.http_method().unwrap());
    }
}
