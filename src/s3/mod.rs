pub mod actions;
pub mod credentials;
pub mod region;
pub mod request;
pub mod responses;
pub mod signature;
pub mod tools;
pub use self::{credentials::Credentials, region::Region, signature::Signature};

use crate::s3::tools::sha256_digest_string;

#[derive(Debug, Clone)]
pub struct S3 {
    // AWS Credentials
    credentials: Credentials,
    // AWS Region
    region: Region,
    // bucket name
    bucket: Option<String>,
}

// Amazon S3 API Reference
// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>
impl S3 {
    #[must_use]
    pub fn new(credentials: &Credentials, region: &Region, bucket: Option<String>) -> Self {
        Self {
            credentials: credentials.clone(),
            region: region.clone(),
            bucket,
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
        sha256_digest_string(&hash)
    }
}
