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
    // bucket name
    bucket: String,
    // AWS Credentials
    credentials: Credentials,
    // AWS Region
    region: Region,
}

// Amazon S3 API Reference
// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>
impl S3 {
    #[must_use]
    pub fn new<B: ToString>(bucket: &B, credentials: &Credentials, region: &Region) -> Self {
        Self {
            bucket: bucket.to_string(),
            credentials: credentials.clone(),
            region: region.clone(),
        }
    }
}
