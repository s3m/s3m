//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::{
    S3,
    checksum::ChecksumAlgorithm,
    error::{ApiError, Error, Result},
    responses::ErrorResponse,
    signature::Signature,
};
use base64ct::{Base64, Encoding};
use quick_xml::de::from_str;
use reqwest::{Method, Response, StatusCode, header::HeaderMap};
use std::{collections::BTreeMap, fmt::Write};
use url::Url;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html>
mod listbuckets;
pub use self::listbuckets::ListBuckets;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
mod listobjectsv2;
pub use self::listobjectsv2::ListObjectsV2;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html>
mod listobjectversions;
pub use self::listobjectversions::ListObjectVersions;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html>
mod headobject;
pub use self::headobject::HeadObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
mod getobject;
pub use self::getobject::GetObject;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html>
mod getobjectacl;
pub use self::getobjectacl::GetObjectAcl;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html>
mod getobjectattributes;
pub use self::getobjectattributes::GetObjectAttributes;

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
pub use self::deleteobject::{DeleteObject, DeleteObjectOutput};

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
mod deleteobjects;
pub use self::deleteobjects::{DeleteObjects, ObjectIdentifier};

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
mod createbucket;
pub use self::createbucket::CreateBucket;

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
mod deletebucket;
pub use self::deletebucket::DeleteBucket;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html>
mod getobjectlockconfiguration;
pub use self::getobjectlockconfiguration::GetObjectLockConfiguration;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html>
mod putobjectlockconfiguration;
pub use self::putobjectlockconfiguration::PutObjectLockConfiguration;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html>
mod getobjectretention;
pub use self::getobjectretention::GetObjectRetention;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html>
mod putobjectretention;
pub use self::putobjectretention::PutObjectRetention;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html>
mod getobjectlegalhold;
pub use self::getobjectlegalhold::GetObjectLegalHold;

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html>
mod putobjectlegalhold;
pub use self::putobjectlegalhold::PutObjectLegalHold;

pub trait Action {
    // headers to send in the request
    fn headers(&self) -> Option<BTreeMap<&str, &str>>;

    // HTTP method to use
    /// # Errors
    /// Will return an error if the HTTP method is not supported
    fn http_method(&self) -> Result<Method>;

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
                    .map_err(|e| Error::Other(format!("cannot be base: {e:#?}")))?
                    .push(p);
            }
        }

        // GET - query pairs
        if let Some(pairs) = &self.query_pairs() {
            for (k, v) in pairs {
                url.query_pairs_mut().append_pair(k, v);
            }
        }

        let custom_headers = self.headers();

        // An additional checksum replaces Content-MD5, as in the AWS SDKs:
        // Cloudflare R2 rejects a request that carries both.
        let md5 = if custom_headers.as_ref().is_some_and(has_additional_checksum) {
            None
        } else {
            md5
        };

        // --no-sign-request: no authorization, but the request still needs what
        // describes its body and the action's own headers (ACL, metadata, checksum).
        // Without Content-Length an upload is sent chunked, which S3 rejects.
        // Names are lowercased and values trimmed as for signed requests
        // (`Signature::add_header`), so names differing only in case stay one header.
        if s3.no_sign_request {
            let mut headers = BTreeMap::new();
            if let Some(length) = content_length {
                headers.insert("content-length".to_string(), length.to_string());
            }
            if let Some(md5) = md5 {
                headers.insert("content-md5".to_string(), Base64::encode_string(md5));
            }
            for (key, value) in custom_headers.into_iter().flatten() {
                headers.insert(key.to_ascii_lowercase(), value.trim().to_string());
            }
            return Ok((url, headers));
        }

        log::info!("URL to sign: {url}");

        let mut signature = Signature::new(s3, "s3", self.http_method()?)?;

        let headers = signature.sign(&url, hash_payload, md5, content_length, custom_headers);

        Ok((url, headers?))
    }
}

/// Whether the headers carry an `x-amz-checksum-<algorithm>` value, for any
/// algorithm `ChecksumAlgorithm` parses (so not `-algorithm`, `-mode` or `-type`).
fn has_additional_checksum(headers: &BTreeMap<&str, &str>) -> bool {
    headers.keys().any(|key| {
        key.to_ascii_lowercase()
            .strip_prefix("x-amz-checksum-")
            .is_some_and(|algorithm| algorithm.parse::<ChecksumAlgorithm>().is_ok())
    })
}

/// The response headers of an upload (`PutObject`, `CompleteMultipartUpload`) that
/// s3m reports, labelled as it prints them.
///
/// # Errors
///
/// Will return `Err` if a header value is not visible ASCII
pub(crate) fn upload_response_headers(
    headers: &HeaderMap,
) -> Result<BTreeMap<&'static str, String>> {
    let mut report = BTreeMap::new();
    for (header, label) in [
        ("x-amz-version-id", "Version ID"),
        ("x-amz-server-side-encryption", "Server-side encryption"),
        ("x-amz-expiration", "Expiration"),
        ("x-emc-previous-object-size", "Previous object size"),
    ] {
        if let Some(value) = headers.get(header) {
            report.insert(label, value.to_str()?.to_string());
        }
    }
    Ok(report)
}

/// Build a structured [`Error::Api`] from a non-2xx S3 response.
///
/// Header reads are best-effort and never fail the conversion; the populated
/// `details` string preserves the original human-readable layout while the
/// `code`/`message`/`status` fields expose the S3 error for programmatic
/// matching.
pub async fn response_error(response: Response) -> Error {
    let status = response.status();
    let headers = response.headers().clone();

    // Try to read the response body, but don't fail if we can't
    let body = response
        .text()
        .await
        .inspect_err(|e| log::warn!("Failed to read error response body: {e}"))
        .ok();

    api_error(status, &headers, body)
}

/// Build a structured [`Error::Api`] from a response's status, headers and body
/// (`None` when the body could not be read). See [`response_error`].
pub(crate) fn api_error(status: StatusCode, headers: &HeaderMap, body: Option<String>) -> Error {
    let status_code = status.as_u16();

    let mut detail: BTreeMap<&str, String> = BTreeMap::new();
    detail.insert("HTTP Status Code", status.to_string());

    if let Some(v) = headers.get("x-amz-id-2").and_then(|h| h.to_str().ok()) {
        detail.insert("x-amz-id-2", v.to_string());
    }

    if let Some(v) = headers
        .get("x-amz-request-id")
        .and_then(|h| h.to_str().ok())
    {
        detail.insert("Request ID", v.to_string());
    }

    let mut code = None;
    let mut message = None;

    match body {
        Some(body) => {
            if let Ok(e) = from_str::<ErrorResponse>(&body) {
                code = Some(e.code.clone());
                message = Some(e.message.clone());
                detail.insert("Code", e.code);
                detail.insert("Message", e.message);
            } else if !body.is_empty() {
                detail.insert("Response", body);
            }
        }
        None => {
            // If we can't read the body, still report what we know
            detail.insert(
                "Note",
                "Connection closed before response body could be read".to_string(),
            );
        }
    }

    let details = detail.iter().fold(String::new(), |mut output, (k, v)| {
        let _ = writeln!(output, "{k}: {v}");
        output
    });

    Error::Api(ApiError {
        status: status_code,
        code,
        message,
        details,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_additional_checksum() {
        for header in [
            "x-amz-checksum-crc32",
            "x-amz-checksum-crc32c",
            "x-amz-checksum-sha1",
            "x-amz-checksum-sha256",
            "X-Amz-Checksum-SHA256",
        ] {
            let headers = BTreeMap::from([(header, "value")]);
            assert!(has_additional_checksum(&headers), "{header}");
        }

        // Settings about checksums, not checksum values, keep Content-MD5.
        for header in [
            "x-amz-checksum-algorithm",
            "x-amz-checksum-mode",
            "x-amz-checksum-type",
            "x-amz-acl",
        ] {
            let headers = BTreeMap::from([(header, "value")]);
            assert!(!has_additional_checksum(&headers), "{header}");
        }
    }
}
