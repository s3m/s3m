pub mod actions;
pub mod credentials;
pub mod region;
pub mod request;
pub mod responses;
pub mod signature;
pub use self::{actions::Actions, credentials::Credentials, region::Region, signature::Signature};

use responses::ListBucketResult;
use serde_xml_rs::from_str;
use std::error;
use url::Url;

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
    pub fn new<B: ToString>(bucket: &B, credentials: &Credentials, region: &Region) -> Self {
        Self {
            bucket: bucket.to_string(),
            credentials: credentials.clone(),
            region: region.clone(),
            host: format!("s3.{}.amazonaws.com", region.name()),
        }
    }

    // ListObjectsV2
    // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    pub async fn list_objects(
        &self,
        action: Actions,
    ) -> Result<ListBucketResult, Box<dyn error::Error>> {
        let mut url = Url::parse(&format!("https://{}/{}", self.host, self.bucket))?;
        url.query_pairs_mut().append_pair("list-type", "2");

        if let Actions::ListObjectsV2 {
            continuation_token,
            delimiter,
            fetch_owner,
            prefix,
            start_after,
        } = action.clone()
        {
            if let Some(token) = continuation_token {
                url.query_pairs_mut()
                    .append_pair("continuation-token", &token);
            }
            if let Some(delimiter) = delimiter {
                url.query_pairs_mut().append_pair("delimiter", &delimiter);
            }
            if let Some(_) = fetch_owner {
                url.query_pairs_mut().append_pair("fetch-owner", "true");
            }
            if let Some(prefix) = prefix {
                url.query_pairs_mut().append_pair("prefix", &prefix);
            }
            if let Some(sa) = start_after {
                url.query_pairs_mut().append_pair("start-after", &sa);
            }
        }

        let method = action.http_verb();
        let mut signature = Signature::new(self, method.as_str(), &url)?;
        let headers = signature.sign("")?;
        let response = match request::request(url.clone(), action.http_verb(), headers).await {
            Ok(r) => r,
            Err(e) => return Err(Box::new(e)),
        };
        //        if rs.status() == 200 {
        let options: ListBucketResult = from_str(&response.text().await?)?;
        Ok(options)
    }
}
