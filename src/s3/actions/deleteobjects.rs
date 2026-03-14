use crate::{
    s3::actions::{Action, response_error},
    s3::responses::DeleteObjectsResult,
    s3::{S3, request, tools},
};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use quick_xml::{de::from_str, se::to_string};
use reqwest::Method;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObjectIdentifier {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename = "Delete")]
struct DeletePayload<'a> {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "Object")]
    objects: &'a [ObjectIdentifier],
    #[serde(rename = "Quiet")]
    quiet: bool,
}

#[derive(Debug, Clone)]
pub struct DeleteObjects {
    objects: Vec<ObjectIdentifier>,
    quiet: bool,
}

impl DeleteObjects {
    pub const MAX_OBJECTS: usize = 1_000;

    #[must_use]
    pub fn new(objects: Vec<ObjectIdentifier>, quiet: bool) -> Self {
        Self { objects, quiet }
    }

    fn body(&self) -> Result<String> {
        if self.objects.len() > Self::MAX_OBJECTS {
            return Err(anyhow!(
                "DeleteObjects supports up to {} objects per request",
                Self::MAX_OBJECTS
            ));
        }

        Ok(to_string(&DeletePayload {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
            objects: &self.objects,
            quiet: self.quiet,
        })?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<DeleteObjectsResult> {
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
            Ok(from_str(&response.text().await?)?)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
impl Action for DeleteObjects {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"POST")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map = BTreeMap::new();
        map.insert("delete", "");
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
    use crate::s3::{
        Credentials, Region, S3,
        responses::{DeleteError, DeletedObject},
    };
    use secrecy::SecretString;

    fn object(key: &str, version_id: Option<&str>) -> ObjectIdentifier {
        ObjectIdentifier {
            key: key.to_string(),
            version_id: version_id.map(str::to_string),
        }
    }

    #[test]
    fn test_method() {
        let action = DeleteObjects::new(vec![object("key", None)], false);
        assert_eq!(Method::POST, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs() {
        let action = DeleteObjects::new(vec![object("key", None)], false);
        let mut map = BTreeMap::new();
        map.insert("delete", "");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = DeleteObjects::new(vec![object("key", None)], false);
        assert_eq!(None, action.path());
    }

    #[test]
    fn test_serialize_body() {
        let action = DeleteObjects::new(
            vec![object("one", None), object("two", Some("version-2"))],
            false,
        );

        let xml = action.body().unwrap();

        assert_eq!(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>one</Key></Object><Object><Key>two</Key><VersionId>version-2</VersionId></Object><Quiet>false</Quiet></Delete>"#,
            xml
        );
    }

    #[test]
    fn test_serialize_body_quiet() {
        let action = DeleteObjects::new(vec![object("key", None)], true);

        let xml = action.body().unwrap();

        assert!(xml.contains("<Quiet>true</Quiet>"));
    }

    #[test]
    fn test_serialize_body_too_many_objects() {
        let objects = vec![object("key", None); DeleteObjects::MAX_OBJECTS + 1];
        let action = DeleteObjects::new(objects, true);

        let err = action.body().unwrap_err().to_string();

        assert!(err.contains("DeleteObjects supports up to 1000 objects per request"));
    }

    #[test]
    fn test_parse_response_success() {
        let xml = r#"
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
    <Key>one</Key>
  </Deleted>
  <Deleted>
    <Key>two</Key>
    <VersionId>version-2</VersionId>
  </Deleted>
</DeleteResult>
"#;

        let parsed: DeleteObjectsResult = from_str(xml).unwrap();

        assert_eq!(
            parsed.deleted,
            vec![
                DeletedObject {
                    key: "one".to_string(),
                    version_id: None,
                },
                DeletedObject {
                    key: "two".to_string(),
                    version_id: Some("version-2".to_string()),
                },
            ]
        );
        assert!(parsed.errors.is_empty());
    }

    #[test]
    fn test_parse_response_with_errors() {
        let xml = r#"
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
    <Key>ok</Key>
  </Deleted>
  <Error>
    <Key>denied</Key>
    <VersionId>version-3</VersionId>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
</DeleteResult>
"#;

        let parsed: DeleteObjectsResult = from_str(xml).unwrap();

        assert_eq!(parsed.deleted.len(), 1);
        assert_eq!(
            parsed.errors,
            vec![DeleteError {
                key: "denied".to_string(),
                version_id: Some("version-3".to_string()),
                code: "AccessDenied".to_string(),
                message: "Access Denied".to_string(),
            }]
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

        let action = DeleteObjects::new(vec![object("key", None)], true);
        let body = action.body().unwrap();
        let sha256 = tools::sha256_digest(&body);
        let md5 = md5::compute(body.as_bytes());

        let (url, headers) = action
            .sign(&s3, sha256.as_ref(), Some(md5.as_ref()), Some(body.len()))
            .unwrap();

        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1?delete=",
            url.as_str()
        );
        assert_eq!(
            headers.get("content-md5"),
            Some(&crate::s3::tools::base64_md5(body.as_bytes()))
        );
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
