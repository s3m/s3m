use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::{S3, request, tools},
};
use reqwest::Method;
use std::collections::BTreeMap;

/// Outcome of a [`DeleteObject`] request.
///
/// On a versioning-enabled bucket, deleting by key (no version id) does not
/// remove data — S3 inserts a **delete marker** and returns
/// `x-amz-delete-marker: true` plus the new marker's `x-amz-version-id`. This
/// struct surfaces that so callers can tell "masked with a delete marker" apart
/// from "version permanently removed".
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeleteObjectOutput {
    pub delete_marker: bool,
    pub version_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct DeleteObject<'a> {
    key: &'a str,
    version_id: Option<String>,
    bypass_governance: bool,
}

impl<'a> DeleteObject<'a> {
    #[must_use]
    pub fn new(key: &'a str) -> Self {
        Self {
            key,
            ..Self::default()
        }
    }

    /// Target a specific object version.
    #[must_use]
    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    /// Send `x-amz-bypass-governance-retention` so a `GOVERNANCE`-locked version
    /// can be deleted (requires the `s3:BypassGovernanceRetention` permission).
    #[must_use]
    pub const fn bypass_governance(mut self, bypass: bool) -> Self {
        self.bypass_governance = bypass;
        self
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<DeleteObjectOutput> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response = request::request(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            None,
            None,
            None,
        )
        .await?;

        if response.status().is_success() {
            let delete_marker = response
                .headers()
                .get("x-amz-delete-marker")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.eq_ignore_ascii_case("true"));
            let version_id = response
                .headers()
                .get("x-amz-version-id")
                .and_then(|v| v.to_str().ok())
                .map(ToString::to_string);
            Ok(DeleteObjectOutput {
                delete_marker,
                version_id,
            })
        } else {
            Err(response_error(response).await)
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
impl Action for DeleteObject<'_> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"DELETE")?)
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
    fn test_headers_bypass_governance() {
        let action = DeleteObject::new("key").bypass_governance(true);
        assert_eq!(
            action
                .headers()
                .unwrap()
                .get("x-amz-bypass-governance-retention"),
            Some(&"true")
        );
    }

    #[test]
    fn test_query_pairs() {
        let action = DeleteObject::new("key");
        assert!(action.query_pairs().is_some());
    }

    #[test]
    fn test_query_pairs_version_id() {
        let action = DeleteObject::new("key").version_id(Some("v9".to_string()));
        assert_eq!(action.query_pairs().unwrap().get("versionId"), Some(&"v9"));
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
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
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
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
