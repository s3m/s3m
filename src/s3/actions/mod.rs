//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::signature::Signature;
use crate::s3::S3;
use std::collections::BTreeMap;
use std::error;
use url::Url;

// ListObjectsV2
// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
mod listobjectsv2;
pub use self::listobjectsv2::ListObjectsV2;

pub trait Action {
    fn http_verb(&self) -> String;

    fn query_pairs(&self) -> BTreeMap<&str, &str>;

    fn sign(&self, s3: S3) -> Result<(Url, BTreeMap<String, String>), Box<dyn error::Error>> {
        let mut url = Url::parse(&format!("https://{}/{}", s3.host, s3.bucket))?;
        url.query_pairs_mut().append_pair("list-type", "2");
        for (k, v) in self.query_pairs().iter() {
            url.query_pairs_mut().append_pair(k, v);
        }
        println!("url: {}", url);
        //        Err("sopas")?
        let mut signature = Signature::new(s3, "GET".to_string(), &url)?;
        Ok((url, signature.sign("")?))
    }
}
