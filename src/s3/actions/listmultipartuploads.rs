use crate::{
    s3::actions::{response_error, Action},
    s3::responses::ListMultipartUploadsResult,
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListMultipartUploads {
    pub max_uploads: Option<String>,
}

impl ListMultipartUploads {
    #[must_use]
    pub const fn new(max_uploads: Option<String>) -> Self {
        Self { max_uploads }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListMultipartUploadsResult> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response =
            request::request(url.clone(), self.http_method()?, headers, None, None, None).await?;
        if response.status().is_success() {
            let uploads: ListMultipartUploadsResult = from_str(&response.text().await?)?;
            Ok(uploads)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
impl Action for ListMultipartUploads {
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

        // uploads
        map.insert("uploads", "");

        if let Some(max_uploads) = &self.max_uploads {
            map.insert("max-uploads", max_uploads);
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
        let action = ListMultipartUploads::new(None);
        assert_eq!(Method::GET, action.http_method().unwrap());
    }

    #[test]
    fn test_max_uploads() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = ListMultipartUploads::new(Some("10".to_string()));
        println!("{:?}", action.query_pairs());
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?max-uploads=10&uploads=",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
