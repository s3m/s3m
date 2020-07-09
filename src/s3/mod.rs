pub mod actions;
pub mod credentials;
pub mod region;
pub mod request;
pub mod signature;
pub use self::{actions::Actions, credentials::Credentials, region::Region, signature::Signature};

use reqwest::Response;
use std::error;

#[derive(Debug)]
pub struct S3 {
    // bucket name
    pub bucket: String,
    // AWS Credentials
    pub credentials: Credentials,
    // AWS Region
    pub region: Region,
    // Host
    pub host: String,
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
            host: format!("s3.{}.amazonaws.com", region.name()),
        }
    }

    // ListObjectsV2
    // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    pub async fn list_objects(&self, action: Actions) -> Result<Response, Box<dyn error::Error>> {
        //pub async fn list_objects(&self, action: Actions) {
        let method = action.http_verb();
        let url = format!("https://{}/{}?list-type=2", self.host, self.bucket);
        let mut signature = Signature::new(&self, method.as_str(), &url)?;
        let headers = signature.sign("")?;
        Ok(request::request(&url, action.http_verb(), headers).await?)
    }
}
