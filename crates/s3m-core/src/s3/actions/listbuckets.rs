use crate::s3::error::Result;
use crate::{
    s3::actions::{Action, response_error},
    s3::responses::ListAllMyBucketsResult,
    s3::{S3, request, tools},
};
use quick_xml::de::from_str;
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct ListBuckets {
    /// Maximum number of buckets returned by this single request (page size).
    pub max_buckets: Option<String>,
    /// Opaque `ContinuationToken` from the previous page, sent unchanged.
    pub continuation_token: Option<String>,
}

impl ListBuckets {
    #[must_use]
    pub const fn new(max_buckets: Option<String>) -> Self {
        Self {
            max_buckets,
            continuation_token: None,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<ListAllMyBucketsResult> {
        let (url, headers) = &self.sign(s3, tools::sha256_digest("").as_ref(), None, None)?;

        let response = request::send_request(
            s3.into(),
            url.clone(),
            self.http_method()?,
            headers,
            None,
            None,
            None,
        )
        .await?;
        if response.status().is_success() {
            let buckets: ListAllMyBucketsResult = from_str(&response.text().await?)?;
            Ok(buckets)
        } else {
            Err(response_error(response).await)
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
impl Action for ListBuckets {
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
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        if let Some(max_buckets) = &self.max_buckets {
            map.insert("max-buckets", max_buckets);
        }

        if let Some(token) = &self.continuation_token {
            map.insert("continuation-token", token);
        }

        (!map.is_empty()).then_some(map)
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

    const TOKEN: &str = "1/AbC+dEf==\u{e9}";

    fn mock_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new("minioadmin", &SecretString::new("minioadmin".into())),
            &Region::custom("us-east-1", endpoint),
            None,
            false,
        )
    }

    fn token_query() -> Matcher {
        Matcher::AllOf(vec![
            Matcher::UrlEncoded("continuation-token".into(), TOKEN.into()),
            Matcher::UrlEncoded("max-buckets".into(), "10000".into()),
        ])
    }

    fn token_page() -> ListBuckets {
        let mut action = ListBuckets::new(Some("10000".to_string()));
        action.continuation_token = Some(TOKEN.to_string());
        action
    }

    #[test]
    fn test_method() {
        let action = ListBuckets::new(None);
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

        let action = ListBuckets::new(None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1",
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
    fn test_max_buckets() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            None,
            false,
        );

        let action = ListBuckets::new(Some("10".to_string()));
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/?max-buckets=10",
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
    fn test_continuation_token_query() {
        let s3 = mock_s3("https://s3.example.com".to_string());

        let (url, _) = token_page()
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();

        let pairs: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("continuation-token").unwrap(), TOKEN);
        assert_eq!(pairs.get("max-buckets").unwrap(), "10000");
        assert_eq!(
            canonical_query_string(&url),
            "continuation-token=1%2FAbC%2BdEf%3D%3D%C3%A9&max-buckets=10000"
        );
    }

    #[tokio::test]
    async fn test_request_continuation_token_single_page() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/")
            .match_query(token_query())
            .with_status(200)
            .with_body("<ListAllMyBucketsResult><Buckets><Bucket><Name>b1</Name><CreationDate>2026-03-14T00:00:00.000Z</CreationDate></Bucket></Buckets><ContinuationToken>next/+=</ContinuationToken></ListAllMyBucketsResult>")
            .expect(1)
            .create_async()
            .await;

        let result = token_page().request(&mock_s3(server.url())).await.unwrap();

        mock.assert_async().await;
        assert_eq!(result.buckets.bucket.len(), 1);
        assert_eq!(result.buckets.bucket[0].name, "b1");
        assert_eq!(result.continuation_token.as_deref(), Some("next/+="));
    }

    #[tokio::test]
    async fn test_request_empty_last_page() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("GET", "/")
            .match_query(token_query())
            .with_status(200)
            .with_body("<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>")
            .create_async()
            .await;

        let result = token_page().request(&mock_s3(server.url())).await.unwrap();

        assert!(result.buckets.bucket.is_empty());
        assert_eq!(result.continuation_token, None);
    }

    #[tokio::test]
    async fn test_request_access_denied() {
        let mut server = Server::new_async().await;
        let _mock = server
            .mock("GET", "/")
            .match_query(token_query())
            .with_status(403)
            .with_body("<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
            .create_async()
            .await;

        let result = token_page().request(&mock_s3(server.url())).await;

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
            .mock("GET", "/")
            .match_query(token_query())
            .with_status(200)
            .with_body("<ListAllMyBucketsResult><Buckets><Bucket><Name>b1</Name>")
            .create_async()
            .await;

        let result = token_page().request(&mock_s3(server.url())).await;

        assert!(
            matches!(result, Err(Error::Xml(_))),
            "expected XML error, got {result:?}"
        );
    }
}
