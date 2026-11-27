use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::responses::{ListVersionsResult, from_xml_root},
    s3::{S3, request, tools},
};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct ListObjectVersions<'a> {
    prefix: &'a str,
    /// List versions after this key (`NextKeyMarker` of a truncated page).
    pub key_marker: Option<String>,
    /// With `key_marker`, list versions after this version id
    /// (`NextVersionIdMarker`).
    pub version_id_marker: Option<String>,
}

impl<'a> ListObjectVersions<'a> {
    #[must_use]
    pub const fn new(prefix: &'a str) -> Self {
        Self {
            prefix,
            key_marker: None,
            version_id_marker: None,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListVersionsResult> {
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
            let objects: ListVersionsResult =
                from_xml_root(&response.text().await?, "ListVersionsResult")?;

            Ok(objects)
        } else {
            Err(response_error(response).await)
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

        if let Some(key_marker) = &self.key_marker {
            map.insert("key-marker", key_marker);
        }

        if let Some(version_id_marker) = &self.version_id_marker {
            map.insert("version-id-marker", version_id_marker);
        }

        Some(map)
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
    use crate::s3::{
        tools, {Credentials, Region, S3},
    };
    use secrecy::SecretString;

    #[test]
    fn test_method() {
        let action = ListObjectVersions::new("key");
        assert_eq!(Method::GET, action.http_method().unwrap());
    }

    // <https://github.com/s3m/s3m/issues/90>: the markers of the next page.
    #[test]
    fn test_query_pairs_with_markers() {
        let mut action = ListObjectVersions::new("key");
        assert_eq!(
            action.query_pairs(),
            Some(BTreeMap::from([("prefix", "key"), ("versions", "")]))
        );

        action.key_marker = Some("key".to_string());
        action.version_id_marker = Some("v2".to_string());
        assert_eq!(
            action.query_pairs(),
            Some(BTreeMap::from([
                ("key-marker", "key"),
                ("prefix", "key"),
                ("version-id-marker", "v2"),
                ("versions", "")
            ]))
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

        let action = ListObjectVersions::new("key");
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?prefix=key&versions=",
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
