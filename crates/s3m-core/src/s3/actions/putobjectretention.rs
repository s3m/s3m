use crate::s3::error::Error;
use crate::s3::object_lock::ObjectLockMode;
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
#[serde(rename = "Retention")]
struct RetentionPayload {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Mode")]
    mode: &'static str,
    #[serde(rename = "RetainUntilDate")]
    retain_until_date: String,
}

/// `PutObjectRetention` — set/extend the retention on an existing object version.
///
/// Shortening or removing a `GOVERNANCE` retention requires `bypass_governance`
/// (the `x-amz-bypass-governance-retention` header).
#[derive(Debug)]
pub struct PutObjectRetention<'a> {
    key: &'a str,
    mode: ObjectLockMode,
    retain_until_date: String,
    version_id: Option<String>,
    bypass_governance: bool,
}

impl<'a> PutObjectRetention<'a> {
    #[must_use]
    pub const fn new(
        key: &'a str,
        mode: ObjectLockMode,
        retain_until_date: String,
        version_id: Option<String>,
        bypass_governance: bool,
    ) -> Self {
        Self {
            key,
            mode,
            retain_until_date,
            version_id,
            bypass_governance,
        }
    }

    // Explicit `Result<T, Error>`, not the `s3::error::Result` alias: importing
    // that alias would shadow `std::result::Result` and break `#[derive(Serialize)]`.
    fn body(&self) -> Result<String, Error> {
        Ok(to_string(&RetentionPayload {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
            mode: self.mode.as_amz(),
            retain_until_date: self.retain_until_date.clone(),
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

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html>
impl Action for PutObjectRetention<'_> {
    fn http_method(&self) -> Result<Method, Error> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        if self.bypass_governance {
            let mut map: BTreeMap<&str, &str> = BTreeMap::new();
            map.insert("x-amz-bypass-governance-retention", "true");
            Some(map)
        } else {
            None
        }
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
    fn test_body() {
        let action = PutObjectRetention::new(
            "key",
            ObjectLockMode::Compliance,
            "2027-01-01T00:00:00Z".to_string(),
            None,
            false,
        );
        let xml = action.body().unwrap();
        assert_eq!(
            xml,
            r#"<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>COMPLIANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>"#
        );
    }

    #[test]
    fn test_headers_bypass() {
        let action = PutObjectRetention::new(
            "key",
            ObjectLockMode::Governance,
            "2027-01-01T00:00:00Z".to_string(),
            None,
            true,
        );
        assert_eq!(
            action
                .headers()
                .unwrap()
                .get("x-amz-bypass-governance-retention"),
            Some(&"true")
        );

        let action = PutObjectRetention::new(
            "key",
            ObjectLockMode::Governance,
            "2027-01-01T00:00:00Z".to_string(),
            None,
            false,
        );
        assert!(action.headers().is_none());
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

        let action = PutObjectRetention::new(
            "key",
            ObjectLockMode::Compliance,
            "2027-01-01T00:00:00Z".to_string(),
            None,
            false,
        );
        let body = action.body().unwrap();
        let sha256 = tools::sha256_digest(&body);
        let md5 = md5::compute(body.as_bytes());
        let (url, headers) = action
            .sign(&s3, sha256.as_ref(), Some(md5.as_ref()), Some(body.len()))
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?retention=",
            url.as_str()
        );
        assert_eq!(
            headers.get("content-md5"),
            Some(&crate::s3::tools::base64_md5(body.as_bytes()))
        );
    }
}
