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
#[serde(rename = "DefaultRetention")]
struct DefaultRetentionPayload {
    #[serde(rename = "Mode")]
    mode: &'static str,
    #[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
    days: Option<u32>,
    #[serde(rename = "Years", skip_serializing_if = "Option::is_none")]
    years: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename = "Rule")]
struct RulePayload {
    #[serde(rename = "DefaultRetention")]
    default_retention: DefaultRetentionPayload,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename = "ObjectLockConfiguration")]
struct ConfigurationPayload {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "ObjectLockEnabled")]
    object_lock_enabled: &'static str,
    #[serde(rename = "Rule")]
    rule: RulePayload,
}

/// `PutObjectLockConfiguration` — set the bucket's default retention rule so
/// every new object inherits it without per-upload flags.
#[derive(Debug)]
pub struct PutObjectLockConfiguration {
    mode: ObjectLockMode,
    days: Option<u32>,
    years: Option<u32>,
}

impl PutObjectLockConfiguration {
    /// `days` and `years` are mutually exclusive; exactly one should be `Some`.
    #[must_use]
    pub const fn new(mode: ObjectLockMode, days: Option<u32>, years: Option<u32>) -> Self {
        Self { mode, days, years }
    }

    // Explicit `Result<T, Error>`, not the `s3::error::Result` alias: importing
    // that alias would shadow `std::result::Result` and break `#[derive(Serialize)]`.
    fn body(&self) -> Result<String, Error> {
        if self.days.is_some() == self.years.is_some() {
            return Err(Error::Other(
                "default retention requires exactly one of days or years".to_string(),
            ));
        }

        Ok(to_string(&ConfigurationPayload {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
            object_lock_enabled: "Enabled",
            rule: RulePayload {
                default_retention: DefaultRetentionPayload {
                    mode: self.mode.as_amz(),
                    days: self.days,
                    years: self.years,
                },
            },
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

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html>
impl Action for PutObjectLockConfiguration {
    fn http_method(&self) -> Result<Method, Error> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("object-lock", "");
        Some(map)
    }

    fn path(&self) -> Option<Vec<&str>> {
        None
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
    fn test_body_days() {
        let action = PutObjectLockConfiguration::new(ObjectLockMode::Compliance, Some(30), None);
        let xml = action.body().unwrap();
        assert_eq!(
            xml,
            r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#
        );
    }

    #[test]
    fn test_body_years() {
        let action = PutObjectLockConfiguration::new(ObjectLockMode::Governance, None, Some(1));
        let xml = action.body().unwrap();
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<Years>1</Years>"));
        assert!(!xml.contains("<Days>"));
    }

    #[test]
    fn test_body_requires_exactly_one_duration() {
        assert!(
            PutObjectLockConfiguration::new(ObjectLockMode::Governance, None, None)
                .body()
                .is_err()
        );
        assert!(
            PutObjectLockConfiguration::new(ObjectLockMode::Governance, Some(1), Some(1))
                .body()
                .is_err()
        );
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

        let action = PutObjectLockConfiguration::new(ObjectLockMode::Compliance, Some(30), None);
        let body = action.body().unwrap();
        let sha256 = tools::sha256_digest(&body);
        let md5 = md5::compute(body.as_bytes());
        let (url, headers) = action
            .sign(&s3, sha256.as_ref(), Some(md5.as_ref()), Some(body.len()))
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?object-lock=",
            url.as_str()
        );
        assert_eq!(
            headers.get("content-md5"),
            Some(&crate::s3::tools::base64_md5(body.as_bytes()))
        );
    }
}
