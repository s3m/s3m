//! Actions
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html>

use crate::s3::{
    S3,
    checksum::ChecksumAlgorithm,
    error::{ApiError, Error, Result},
    responses::{ErrorResponse, from_xml_root},
    signature::Signature,
};
use base64ct::{Base64, Encoding};
use reqwest::{
    Method, Response, StatusCode,
    header::{HeaderMap, LOCATION},
};
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

/// A response header value as text to show: bytes that aren't UTF-8 become U+FFFD,
/// and control characters and the Unicode line separators are escaped (`\t`,
/// `\u{2028}`), so a value can't start a line of its own in s3m's output.
pub(crate) fn header_text(value: &reqwest::header::HeaderValue) -> String {
    let text = String::from_utf8_lossy(value.as_bytes());
    display_text(&text)
}

fn display_text(text: &str) -> String {
    let mut shown = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_control() || matches!(character, '\u{2028}' | '\u{2029}') {
            shown.extend(character.escape_default());
        } else {
            shown.push(character);
        }
    }
    shown
}

/// The response headers of an upload (`PutObject`, `CompleteMultipartUpload`) that
/// s3m reports, labelled as it prints them.
///
/// They are only shown: the upload is already stored when they arrive, so a value
/// that isn't visible ASCII is shown as far as it can be read (see [`header_text`]),
/// and never fails the upload.
pub(crate) fn upload_response_headers(headers: &HeaderMap) -> BTreeMap<&'static str, String> {
    let mut report = BTreeMap::new();
    for (header, label) in [
        ("x-amz-version-id", "Version ID"),
        ("x-amz-server-side-encryption", "Server-side encryption"),
        ("x-amz-expiration", "Expiration"),
        ("x-emc-previous-object-size", "Previous object size"),
    ] {
        if let Some(value) = headers.get(header) {
            report.insert(label, header_text(value));
        }
    }
    report
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

#[derive(serde::Deserialize)]
struct RedirectEndpoint {
    #[serde(rename = "Endpoint")]
    endpoint: String,
}

fn redirect_destination(value: &str, location: bool) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(|c| c.is_control() || c == '\\') {
        return None;
    }

    let (url, show_origin, show_path) =
        if let Some((scheme, _)) = value.split_once("://").filter(|_| !value.starts_with('/')) {
            if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
                return None;
            }
            (Url::parse(value).ok()?, true, true)
        } else if let Some(authority) = value.strip_prefix("//") {
            (
                Url::parse(&format!("https://{authority}")).ok()?,
                false,
                true,
            )
        } else if location {
            let path = value.split(['?', '#']).next()?;
            if path.is_empty() || path.contains([':', '@']) {
                return None;
            }
            return Some(display_text(path));
        } else {
            let url = Url::parse(&format!("https://{value}")).ok()?;
            (url, false, false)
        };
    let host = url.host_str()?;
    let mut shown = if show_origin {
        format!("{}://{host}", url.scheme())
    } else if show_path {
        format!("//{host}")
    } else {
        host.to_string()
    };
    if let Some(port) = url.port() {
        let _ = write!(shown, ":{port}");
    }
    if show_path {
        shown.push_str(url.path());
    }
    Some(shown)
}

fn redirect_hint(status: StatusCode, headers: &HeaderMap, body: Option<&str>) -> String {
    // S3 can send an XML Endpoint or Location, and a temporary destination only
    // applies to the request. See the S3 routing documentation.
    // <https://docs.aws.amazon.com/AmazonS3/latest/developerguide/UsingRouting.html>
    let destination = body
        .and_then(|body| from_xml_root::<RedirectEndpoint>(body, "Error").ok())
        .and_then(|body| redirect_destination(&body.endpoint, false))
        .or_else(|| {
            headers
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| redirect_destination(value, true))
        });
    let mut hint = match (status, destination) {
        (StatusCode::MOVED_PERMANENTLY, Some(endpoint)) => {
            format!("destination is {endpoint}. Update the host's endpoint or region")
        }
        (StatusCode::MOVED_PERMANENTLY, None) => "Update the host's endpoint or region".to_string(),
        (StatusCode::TEMPORARY_REDIRECT, Some(endpoint)) => {
            format!("the destination for this request is {endpoint}")
        }
        (StatusCode::TEMPORARY_REDIRECT, None) => "temporary redirect for this request".to_string(),
        (_, Some(endpoint)) => format!("destination is {endpoint}"),
        (_, None) => "service returned a redirect".to_string(),
    };
    if let Some(region) = headers
        .get("x-amz-bucket-region")
        .and_then(|value| value.to_str().ok())
        .filter(|region| {
            !region.is_empty()
                && region
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
    {
        let _ = write!(hint, ". The bucket is in region {region}");
    }
    hint
}

/// Build a structured [`Error::Api`] from a response's status, headers and body
/// (`None` when the body could not be read). See [`response_error`].
pub(crate) fn api_error(status: StatusCode, headers: &HeaderMap, body: Option<String>) -> Error {
    let status_code = status.as_u16();

    let mut detail: BTreeMap<&str, String> = BTreeMap::new();
    detail.insert("HTTP Status Code", status.to_string());

    if status.is_redirection() {
        detail.insert("Redirect", redirect_hint(status, headers, body.as_deref()));
    }

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
            // Only an <Error> document gives the error its code: s3m acts on some
            // codes (NoSuchUpload forgets a saved upload), and another document that
            // happens to have a <Code> must not trigger that.
            if let Ok(e) = from_xml_root::<ErrorResponse>(&body, "Error") {
                if !status.is_redirection() || e.code.bytes().all(|b| b.is_ascii_alphanumeric()) {
                    code = Some(e.code.clone());
                    detail.insert("Code", e.code);
                }
                // Redirects may echo the raw destination in Message or a non-XML body.
                if !status.is_redirection() {
                    message = Some(e.message.clone());
                    detail.insert("Message", e.message);
                }
            } else if !status.is_redirection() && !body.is_empty() {
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

    async fn reject_read_redirect(method: &str, path: &str) -> anyhow::Result<()> {
        let mut server = mockito::Server::new_async().await;
        let s3 = S3::new(
            &crate::s3::Credentials::new("", &secrecy::SecretString::new("".into())),
            &crate::s3::Region::custom("us-east-1", server.url()),
            Some("bucket".into()),
            true,
        );
        let redirect = server
            .mock(method, path)
            .with_status(301)
            .with_header("location", "/destination")
            .create_async()
            .await;
        let destination = server
            .mock(method, "/destination")
            .with_status(200)
            .with_body("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
            .expect(0)
            .create_async()
            .await;

        let result = match (method, path) {
            ("HEAD", _) => HeadObject::new("key", None).request(&s3).await.map(drop),
            (_, "/bucket/key") => GetObject::new("key", None)
                .request(&s3, &crate::s3::RequestOptions::default())
                .await
                .map(drop),
            _ => ListObjectsV2::default().request(&s3).await.map(drop),
        };
        assert_eq!(
            result.as_ref().err().and_then(Error::status),
            Some(301),
            "{method} {path} must fail at the redirect: {result:?}"
        );
        let message = result.err().map_or_else(String::new, |e| e.to_string());
        assert!(message.contains("destination is /destination"), "{message}");
        redirect.assert_async().await;
        destination.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn redirects_are_not_followed_by_get() -> anyhow::Result<()> {
        reject_read_redirect("GET", "/bucket/key").await
    }

    #[tokio::test]
    async fn redirects_are_not_followed_by_head() -> anyhow::Result<()> {
        reject_read_redirect("HEAD", "/bucket/key").await
    }

    #[tokio::test]
    async fn redirects_are_not_followed_by_listing() -> anyhow::Result<()> {
        reject_read_redirect("GET", "/bucket?list-type=2").await
    }

    // A server may put bytes outside ASCII in a header s3m only shows (a lifecycle
    // rule id in x-amz-expiration, for example). The upload is stored by then.
    #[test]
    fn test_upload_response_headers_shows_values_that_are_not_ascii()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use reqwest::header::HeaderValue;

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-version-id", HeaderValue::from_static("7e5dee92"));
        headers.insert(
            "x-amz-expiration",
            HeaderValue::from_bytes("rule-id=\"règle\"".as_bytes())?,
        );
        headers.insert(
            "x-amz-server-side-encryption",
            HeaderValue::from_bytes(b"AES\xff256")?,
        );

        let report = upload_response_headers(&headers);

        assert_eq!(
            report.get("Version ID").map(String::as_str),
            Some("7e5dee92")
        );
        assert_eq!(
            report.get("Expiration").map(String::as_str),
            Some("rule-id=\"règle\"")
        );
        assert_eq!(
            report.get("Server-side encryption").map(String::as_str),
            Some("AES\u{fffd}256")
        );

        // A tab or a Unicode line separator would split the line for whoever parses
        // the report: they are shown escaped.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-expiration",
            HeaderValue::from_bytes("rule\tone\u{2028}ETag: \"forged\"".as_bytes())?,
        );
        let report = upload_response_headers(&headers);
        assert_eq!(
            report.get("Expiration").map(String::as_str),
            Some("rule\\tone\\u{2028}ETag: \"forged\"")
        );
        Ok(())
    }

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
