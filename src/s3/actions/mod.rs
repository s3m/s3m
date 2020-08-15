//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::responses::ErrorResponse;
use crate::s3::signature::Signature;
use crate::s3::S3;
use reqwest::Response;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;
use url::Url;

const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html>
mod listbuckets;
pub use self::listbuckets::ListBuckets;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
mod listobjectsv2;
pub use self::listobjectsv2::ListObjectsV2;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
mod putobject;
pub use self::putobject::PutObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>
mod createmultipartupload;
pub use self::createmultipartupload::CreateMultipartUpload;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html>
mod uploadpart;
pub use self::uploadpart::UploadPart;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
mod completemultipartupload;
pub use self::completemultipartupload::{CompleteMultipartUpload, Part};

pub trait Action {
    // headers to send in the request
    fn headers(&self) -> Option<BTreeMap<&str, &str>>;

    // method to use GET/PUT...
    fn http_verb(&self) -> &'static str;

    // URL query pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>>;

    // URL path
    fn path(&self) -> Option<Vec<&str>>;

    /// # Errors
    ///
    /// Will return `Err` if the signature can not be created
    fn sign(
        &self,
        s3: &S3,
        hash_payload: &str,
        md5: Option<&str>,
        content_length: Option<usize>,
    ) -> Result<(Url, BTreeMap<String, String>), Box<dyn error::Error>> {
        let mut url = match &s3.bucket {
            Some(bucket) => Url::parse(&format!("https://{}/{}", s3.region.endpoint(), bucket))?,
            None => Url::parse(&format!("https://{}", s3.region.endpoint()))?,
        };

        // mainly for PUT when uploading an object
        if let Some(path) = self.path() {
            for p in path {
                url.path_segments_mut()
                    .map_err(|_| "cannot be base")?
                    .push(p);
            }
        }

        // GET - query pairs
        if let Some(pairs) = &self.query_pairs() {
            for (k, v) in pairs {
                url.query_pairs_mut().append_pair(k, v);
            }
        }

        // headers to be sent
        let _headers: BTreeMap<&str, &str> = if let Some(headers) = self.headers() {
            headers
        } else {
            BTreeMap::new()
        };

        let mut signature = Signature::new(s3, self.http_verb(), &url)?;
        let headers = signature.sign(hash_payload, md5, content_length);
        Ok((url, headers))
    }
}

pub async fn response_error(response: Response) -> Result<String, Box<dyn error::Error>> {
    let mut error: BTreeMap<&str, String> = BTreeMap::new();
    error.insert("HTTP Status Code", response.status().to_string());
    if let Some(x_amz_id_2) = response.headers().get("x-amz-id-2") {
        error.insert("x-amz-id-2", x_amz_id_2.to_str()?.to_string());
    }

    if let Some(rid) = response.headers().get("x-amz-request-id") {
        error.insert("Request ID", rid.to_str()?.to_string());
    }

    let body = response.text().await?;

    if let Ok(e) = from_str::<ErrorResponse>(&body) {
        error.insert("Code", e.code);
        error.insert("Message", e.message);
    } else {
        error.insert("Response", body);
    }
    Ok(error
        .iter()
        .map(|(k, v)| format!("{}: {}\n", k, v))
        .collect::<String>())
}
