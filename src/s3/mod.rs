pub mod actions;
pub mod credentials;
pub mod region;
pub mod signature;
pub use self::{actions::Actions, credentials::Credentials, region::Region, signature::Signature};

#[derive(Debug)]
pub struct S3 {
    // bucket name
    pub bucket: String,
    // AWS Credentials
    pub credentials: Credentials,
    // AWS Region
    pub region: Region,
}

// Amazon S3 API Reference
// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>
impl S3 {
    #[must_use]
    pub fn new<B: ToString>(bucket: B, credentials: &Credentials, region: &Region) -> Self {
        Self {
            bucket: bucket.to_string(),
            credentials: credentials.clone(),
            region: region.clone(),
        }
    }

    // ListObjectsV2
    // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    pub fn list_objects(self, action: Actions) {
        let method = action.http_verb();
        let mut signature = Signature::new(self, method.as_str(), "/", "list-type=2");
        signature.sign()
    }
}
