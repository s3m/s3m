use crate::s3::error::Error;
use crate::{
    s3::actions::{Action, response_error},
    s3::{S3, request, tools},
};
use bytes::Bytes;
use quick_xml::se::to_string;
use reqwest::Method;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
#[serde(rename = "LegalHold")]
struct LegalHoldPayload {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Status")]
    status: &'static str,
}

/// `PutObjectLegalHold` — turn an object version's legal hold on or off.
#[derive(Debug)]
pub struct PutObjectLegalHold<'a> {
    key: &'a str,
    enabled: bool,
    version_id: Option<String>,
}

impl<'a> PutObjectLegalHold<'a> {
    #[must_use]
    pub const fn new(key: &'a str, enabled: bool, version_id: Option<String>) -> Self {
        Self {
            key,
            enabled,
            version_id,
        }
    }

    // Explicit `Result<T, Error>`, not the `s3::error::Result` alias: importing
    // that alias would shadow `std::result::Result` and break `#[derive(Serialize)]`.
    fn body(&self) -> Result<String, Error> {
        Ok(to_string(&LegalHoldPayload {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
            status: if self.enabled { "ON" } else { "OFF" },
        })?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<(), Error> {
        let body = self.body()?;
        let sha256 = tools::sha256_digest(&body);
        let md5 = md5::compute(body.as_bytes());

        let (url, headers) =
            &self.sign(s3, sha256.as_ref(), Some(md5.as_ref()), Some(body.len()))?;

        let response = request::upload(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            Bytes::from(body),
        )
        .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html>
impl Action for PutObjectLegalHold<'_> {
    fn http_method(&self) -> Result<Method, Error> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("legal-hold", "");
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
    fn test_body_on_off() {
        let on = PutObjectLegalHold::new("key", true, None).body().unwrap();
        assert_eq!(
            on,
            r#"<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>"#
        );
        let off = PutObjectLegalHold::new("key", false, None).body().unwrap();
        assert!(off.contains("<Status>OFF</Status>"));
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

        let action = PutObjectLegalHold::new("key", true, None);
        let body = action.body().unwrap();
        let sha256 = tools::sha256_digest(&body);
        let md5 = md5::compute(body.as_bytes());
        let (url, headers) = action
            .sign(&s3, sha256.as_ref(), Some(md5.as_ref()), Some(body.len()))
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?legal-hold=",
            url.as_str()
        );
        assert_eq!(
            headers.get("content-md5"),
            Some(&crate::s3::tools::base64_md5(body.as_bytes()))
        );
    }
}
