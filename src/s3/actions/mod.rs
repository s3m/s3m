//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::signature::Signature;
use crate::s3::S3;
use std::collections::BTreeMap;
use std::error;
use url::Url;

const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
mod listobjectsv2;
pub use self::listobjectsv2::ListObjectsV2;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
mod putobject;
pub use self::putobject::PutObject;

pub trait Action {
    fn http_verb(&self) -> &'static str;

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>>;

    fn headers(&self) -> Option<BTreeMap<&str, &str>>;

    /// # Errors
    ///
    /// Will return `Err` if the signature can not be created
    fn sign(
        &self,
        s3: S3,
        hash_payload: &str,
    ) -> Result<(Url, BTreeMap<String, String>), Box<dyn error::Error>> {
        let mut url = Url::parse(&format!("https://{}/{}", s3.host, s3.bucket))?;

        if self.http_verb() != "GET" {
            url.path_segments_mut()
                .map_err(|_| "cannot be base")?
                .push("a.txt");
        }

        // GET - query pairs
        if let Some(pairs) = &self.query_pairs() {
            for (k, v) in pairs {
                url.query_pairs_mut().append_pair(k, v);
            }
        }
        // PUT/POST
        let _headers: BTreeMap<&str, &str> = if let Some(headers) = self.headers() {
            headers
        } else {
            BTreeMap::new()
        };
        let mut signature = Signature::new(s3, self.http_verb(), &url)?;
        let headers = signature.sign(hash_payload);
        Ok((url, headers))
    }
}
