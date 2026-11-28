pub mod actions;
pub mod checksum;
pub mod credentials;
pub mod error;
pub mod limits;
pub mod object_lock;
pub mod options;
pub mod region;
pub mod request;
pub mod responses;
pub mod signature;
pub mod tools;
pub use self::{
    credentials::Credentials,
    error::{ApiError, Error},
    object_lock::{ObjectLock, ObjectLockMode},
    options::RequestOptions,
    region::Region,
    signature::Signature,
};

use crate::s3::error::Result;
use crate::s3::tools::{sha256_digest, write_hex_bytes};
use reqwest::Client;
use std::fmt;
use url::Url;

#[derive(Debug, Clone)]
pub struct S3 {
    // AWS Credentials
    credentials: Credentials,
    // AWS Region
    region: Region,
    // bucket name
    bucket: Option<String>,
    // sign request
    no_sign_request: bool,
    // shared HTTP client for connection pooling and TLS session reuse
    client: Client,
}

impl fmt::Display for S3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write the formatted representation to the provided formatter
        write!(
            f,
            "credentials: {:#?}\nregion: {:#?}\nbucket: {:#?}\nno_sign_request: {}",
            self.credentials, self.region, self.bucket, self.no_sign_request
        )
    }
}

// Amazon S3 API Reference
// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>
impl S3 {
    #[must_use]
    pub fn new(
        credentials: &Credentials,
        region: &Region,
        bucket: Option<String>,
        no_sign_request: bool,
    ) -> Self {
        Self {
            credentials: credentials.clone(),
            region: region.clone(),
            bucket,
            no_sign_request,
            client: Client::new(),
        }
    }

    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }

    #[cfg(test)]
    pub(crate) fn set_test_client(&mut self, client: Client) {
        self.client = client;
    }

    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    #[must_use]
    pub fn with_bucket(&self, bucket: Option<String>) -> Self {
        let mut s3 = self.clone();
        s3.bucket = bucket;
        s3
    }

    // use it to identify the connection and keep track of the uploaded files so that the same file
    // could be uploaded into multiple provider/buckets
    #[must_use]
    pub fn hash(&self) -> String {
        let mut hash = String::new();

        hash.push_str(self.credentials.aws_access_key_id());
        hash.push_str(self.credentials.aws_secret_access_key());
        hash.push_str(&self.region.endpoint());

        if let Some(bucket) = &self.bucket {
            hash.push_str(bucket);
        }

        write_hex_bytes(sha256_digest(&hash).as_ref())
    }

    /// Store the endpoint and bucket as one space-free field in the state key.
    /// An endpoint containing userinfo or query parameters may itself carry a
    /// secret. In that case only its fingerprint goes on disk.
    pub(crate) fn upload_host(&self) -> String {
        Self::encode_upload_host(
            &self.region.endpoint(),
            self.bucket().unwrap_or_default(),
            true,
        )
    }

    /// Early v2 directories hashed the configured spelling directly.
    pub(crate) fn previous_upload_host(&self) -> String {
        Self::encode_upload_host(
            &self.region.endpoint(),
            self.bucket().unwrap_or_default(),
            false,
        )
    }

    pub(crate) fn matches_upload_host(&self, saved: &str) -> bool {
        let current = self.upload_host();
        if current == saved {
            return true;
        }
        let Some(fields) = saved.strip_prefix("v2-") else {
            return false;
        };
        let mut pairs = url::form_urlencoded::parse(fields.as_bytes());
        matches!((pairs.next(), pairs.next(), pairs.next()),
            (Some((name, endpoint)), Some((field, bucket)), None)
                if name == "endpoint" && field == "bucket"
                && Self::encode_upload_host(&endpoint, &bucket, true) == current)
    }

    fn encode_upload_host(raw: &str, bucket: &str, normalize: bool) -> String {
        let endpoint = if raw.contains(['@', '?', '#']) {
            format!("blake3:{}", blake3::hash(raw.as_bytes()))
        } else if normalize {
            Self::base_url(raw).map_or_else(
                |_| raw.to_owned(),
                |url| url.as_str().trim_end_matches('/').to_owned(),
            )
        } else {
            raw.to_owned()
        };
        let host = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("endpoint", &endpoint)
            .append_pair("bucket", bucket)
            .finish();
        format!("v2-{host}")
    }

    /// # Errors
    /// Will return an error if the endpoint is invalid
    pub fn endpoint(&self) -> Result<Url> {
        let mut base = Self::base_url(&self.region.endpoint())?;
        if let Some(bucket) = &self.bucket {
            base.set_path(&format!("{}/{bucket}", base.path().trim_end_matches('/')));
        }
        Ok(base)
    }

    fn base_url(endpoint: &str) -> Result<Url> {
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            Url::parse(endpoint)?
        } else {
            Url::parse(&format!("https://{endpoint}"))?
        };
        Ok(url)
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
    use secrecy::SecretString;

    #[test]
    fn test_s3() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        assert_eq!(
            s3.hash(),
            "bc8cea3065fd34a0b9cda467e3ace6bae7737d046e0dd3d449381a33508c24ae"
        );
    }

    #[test]
    fn test_s3_endpoint() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        assert_eq!(
            s3.endpoint().unwrap().as_str(),
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1"
        );
    }
}
