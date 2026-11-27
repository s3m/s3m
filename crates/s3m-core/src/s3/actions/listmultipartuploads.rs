use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::responses::{ListMultipartUploadsResult, from_xml_root},
    s3::{S3, request, tools},
};
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListMultipartUploads {
    /// Maximum number of uploads returned by this single request (page size).
    pub max_uploads: Option<String>,
    /// Only list uploads whose key begins with this prefix.
    pub prefix: Option<String>,
    /// List uploads after this key (`NextKeyMarker` of the previous page).
    pub key_marker: Option<String>,
    /// With `key_marker`, list uploads after this upload ID (`NextUploadIdMarker`).
    pub upload_id_marker: Option<String>,
}

impl ListMultipartUploads {
    #[must_use]
    pub const fn new(max_uploads: Option<String>) -> Self {
        Self {
            max_uploads,
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListMultipartUploadsResult> {
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
            let uploads: ListMultipartUploadsResult =
                from_xml_root(&response.text().await?, "ListMultipartUploadsResult")?;
            Ok(uploads)
        } else {
            Err(response_error(response).await)
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

        if let Some(prefix) = &self.prefix {
            map.insert("prefix", prefix);
        }

        if let Some(key_marker) = &self.key_marker {
            map.insert("key-marker", key_marker);
        }

        if let Some(upload_id_marker) = &self.upload_id_marker {
            map.insert("upload-id-marker", upload_id_marker);
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
        error::Error,
        signature::canonical_query_string,
        tools, {Credentials, Region, S3},
    };
    use mockito::{Matcher, Server};
    use secrecy::SecretString;

    const KEY_MARKER: &str = "logs/\u{fc}+a=b.txt";
    const UPLOAD_ID_MARKER: &str = "VXBsb2Fk+/=Id";

    fn mock_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new("minioadmin", &SecretString::new("minioadmin".into())),
            &Region::custom("us-east-1", endpoint),
            Some("bucket".to_string()),
            false,
        )
    }

    fn marker_page() -> ListMultipartUploads {
        let mut action = ListMultipartUploads::new(Some("1000".to_string()));
        action.prefix = Some("logs/".to_string());
        action.key_marker = Some(KEY_MARKER.to_string());
        action.upload_id_marker = Some(UPLOAD_ID_MARKER.to_string());
        action
    }

    fn marker_query() -> Matcher {
        Matcher::AllOf(vec![
            Matcher::UrlEncoded("uploads".into(), String::new()),
            Matcher::UrlEncoded("max-uploads".into(), "1000".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            Matcher::UrlEncoded("key-marker".into(), KEY_MARKER.into()),
            Matcher::UrlEncoded("upload-id-marker".into(), UPLOAD_ID_MARKER.into()),
        ])
    }

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
        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }

    #[test]
    fn test_prefix_and_markers_query() {
        let s3 = mock_s3("https://s3.example.com".to_string());

        let (url, _) = marker_page()
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        let pairs: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("prefix").unwrap(), "logs/");
        assert_eq!(pairs.get("key-marker").unwrap(), KEY_MARKER);
        assert_eq!(pairs.get("upload-id-marker").unwrap(), UPLOAD_ID_MARKER);
        assert_eq!(pairs.get("max-uploads").unwrap(), "1000");
        assert_eq!(pairs.get("uploads").unwrap(), "");
        assert_eq!(
            canonical_query_string(&url),
            "key-marker=logs%2F%C3%BC%2Ba%3Db.txt&max-uploads=1000&prefix=logs%2F&upload-id-marker=VXBsb2Fk%2B%2F%3DId&uploads="
        );
    }

    #[tokio::test]
    async fn test_request_markers_single_page() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/bucket")
            .match_query(marker_query())
            .with_status(200)
            .with_body("<ListMultipartUploadsResult><Bucket>bucket</Bucket><KeyMarker>logs/\u{fc}+a=b.txt</KeyMarker><UploadIdMarker>VXBsb2Fk+/=Id</UploadIdMarker><NextKeyMarker>logs/z.txt</NextKeyMarker><NextUploadIdMarker>next+/=</NextUploadIdMarker><Prefix>logs/</Prefix><MaxUploads>1000</MaxUploads><IsTruncated>true</IsTruncated><Upload><Key>logs/z.txt</Key><UploadId>u-1</UploadId><Initiated>2026-03-14T00:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass></Upload></ListMultipartUploadsResult>")
            .expect(1)
            .create_async()
            .await;

        let result = marker_page().request(&mock_s3(server.url())).await.unwrap();

        mock.assert_async().await;
        assert!(result.is_truncated);
        assert_eq!(result.key_marker.as_deref(), Some(KEY_MARKER));
        assert_eq!(result.next_key_marker.as_deref(), Some("logs/z.txt"));
        assert_eq!(result.next_upload_id_marker.as_deref(), Some("next+/="));
        assert_eq!(result.upload.unwrap()[0].upload_id, "u-1");
    }

    #[tokio::test]
    async fn test_request_access_denied() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("GET", "/bucket")
            .match_query(marker_query())
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
            .create_async()
            .await;

        let result = marker_page().request(&mock_s3(server.url())).await;

        let Err(Error::Api(api)) = result else {
            panic!("expected API error, got {result:?}");
        };
        assert_eq!(api.status, 403);
        assert_eq!(api.code.as_deref(), Some("AccessDenied"));
    }

    #[tokio::test]
    async fn test_request_malformed_xml() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("GET", "/bucket")
            .match_query(marker_query())
            .with_status(200)
            .with_body("<ListMultipartUploadsResult><Bucket>bucket</Bucket><IsTruncated>maybe</IsTruncated>")
            .create_async()
            .await;

        let result = marker_page().request(&mock_s3(server.url())).await;

        assert!(
            matches!(result, Err(Error::Xml(_))),
            "expected XML error, got {result:?}"
        );
    }
}
