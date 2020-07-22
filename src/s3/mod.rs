pub mod actions;
pub mod credentials;
pub mod region;
pub mod request;
pub mod responses;
pub mod signature;
pub mod tools;
pub use self::{credentials::Credentials, region::Region, signature::Signature};

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
            bucket: bucket,
        }
    }
}
