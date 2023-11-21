//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::responses::ErrorResponse;
use crate::s3::signature::Signature;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use reqwest::Response;
use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use url::Url;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html>
mod listbuckets;
pub use self::listbuckets::ListBuckets;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
mod listobjectsv2;
pub use self::listobjectsv2::ListObjectsV2;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html>
mod headobject;
pub use self::headobject::HeadObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
mod getobject;
pub use self::getobject::GetObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html>
mod getobjectacl;
pub use self::getobjectacl::GetObjectAcl;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
mod putobject;
pub use self::putobject::PutObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html>
mod putobjectacl;
pub use self::putobjectacl::PutObjectAcl;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>
mod createmultipartupload;
pub use self::createmultipartupload::CreateMultipartUpload;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html>
mod uploadpart;
pub use self::uploadpart::UploadPart;

mod streampart;
pub use self::streampart::StreamPart;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
mod completemultipartupload;
pub use self::completemultipartupload::{CompleteMultipartUpload, Part};

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html>
mod listmultipartuploads;
pub use self::listmultipartuploads::ListMultipartUploads;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html>
mod abortmultipartupload;
pub use self::abortmultipartupload::AbortMultipartUpload;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
mod deleteobject;
pub use self::deleteobject::DeleteObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
mod createbucket;
pub use self::createbucket::CreateBucket;

pub trait Action {
    // headers to send in the request
    fn headers(&self) -> Option<BTreeMap<&str, &str>>;

    // method to use GET/PUT...
    fn http_method(&self) -> reqwest::Method;

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
        hash_payload: &[u8],
        md5: Option<&[u8]>,
        content_length: Option<usize>,
    ) -> Result<(Url, BTreeMap<String, String>)> {
        let mut url = s3.endpoint()?;

        // mainly for PUT when uploading an object
        if let Some(path) = self.path() {
            for p in path {
                url.path_segments_mut()
                    .map_err(|e| anyhow!("cannot be base: {:#?}", e))?
                    .push(p);
            }
        }

        // GET - query pairs
        if let Some(pairs) = &self.query_pairs() {
            for (k, v) in pairs {
                url.query_pairs_mut().append_pair(k, v);
            }
        }

        let mut signature = Signature::new(s3, "s3", self.http_method())?;
        let headers = signature.sign(&url, hash_payload, md5, content_length, self.headers());
        Ok((url, headers))
    }
}

pub async fn response_error(response: Response) -> Result<String> {
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
