pub mod actions;
pub mod checksum;
pub mod credentials;
pub mod region;
pub mod request;
pub mod responses;
pub mod signature;
pub mod tools;
pub use self::{credentials::Credentials, region::Region, signature::Signature};

use crate::s3::tools::{sha256_digest, write_hex_bytes};
use anyhow::Result;
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
        }
    }

    // use it to identify the connection and keep track of the uploaded files so that the same file
    // could be uploaded into multiple provider/buckets
    #[must_use]
    pub fn hash(&self) -> String {
        let mut hash = String::new();

        hash.push_str(self.credentials.aws_access_key_id());
        hash.push_str(self.credentials.aws_secret_access_key());
        hash.push_str(self.region.endpoint());

        if let Some(bucket) = &self.bucket {
            hash.push_str(bucket);
        }

        write_hex_bytes(sha256_digest(&hash).as_ref())
    }

    /// # Errors
    /// Will return an error if the endpoint is invalid
    pub fn endpoint(&self) -> Result<Url> {
        let url = if let Some(bucket) = &self.bucket {
            Url::parse(&format!("https://{}/{}", &self.region.endpoint(), bucket))?
        } else {
            Url::parse(&format!("https://{}", &self.region.endpoint()))?
        };

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::Secret;

    #[test]
    fn test_s3() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
