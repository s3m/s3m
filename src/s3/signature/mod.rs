//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::{
    s3::S3,
    s3::tools::{sha256_digest, sha256_hmac, write_hex_bytes},
};
use anyhow::{Result, anyhow};
use base64ct::{Base64, Encoding};
use chrono::prelude::{DateTime, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode, utf8_percent_encode};
use reqwest::Method;
use ring::hmac;
use std::collections::BTreeMap;
use url::Url;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Debug)]
pub struct Signature<'a> {
    // S3
    auth: &'a S3,
    // AWS-service
    aws_service: &'a str,
    // The HTTPRequestMethod
    http_method: Method,
    // The HTTP request headers
    pub headers: BTreeMap<String, String>,
    // current date & time
    datetime: DateTime<Utc>,
}

impl<'a> Signature<'a> {
    /// # Errors
    ///
    /// Will return `Err` if can't parse the url
    pub fn new(s3: &'a S3, aws_service: &'a str, http_method: Method) -> Result<Self> {
        Ok(Self {
            auth: s3,
            aws_service,
            http_method,
            datetime: Utc::now(),
            headers: BTreeMap::new(),
        })
    }

    /// Need the the HexEncode(Hash(RequestPayload))
    ///
    /// # Errors
    /// Will return `Err` if can not make the request
    pub fn sign(
        &mut self,
        url: &Url,
        digest_sha256: &[u8],
        digest_md5: Option<&[u8]>,
        length: Option<usize>,
        custom_headers: Option<BTreeMap<&str, &str>>,
    ) -> Result<BTreeMap<String, String>> {
        let current_date = self.datetime.format("%Y%m%d").to_string();
        let current_datetime = self.datetime.format("%Y%m%dT%H%M%SZ").to_string();

        self.add_header("host", &self.auth.region.endpoint());
        self.add_header("x-amz-date", &current_datetime);
        self.add_header("User-Agent", APP_USER_AGENT);
        self.add_header("x-amz-content-sha256", &write_hex_bytes(digest_sha256));

        if let Some(length) = length {
            self.add_header("content-length", format!("{length}").as_ref());
        }

        if let Some(md5) = digest_md5 {
            self.add_header("Content-MD5", &Base64::encode_string(md5));
        }

        if let Some(headers) = custom_headers {
            for (k, v) in &headers {
                self.add_header(k, v);
            }
        }

        // let canonical_headers = canonical_headers(&self.headers);
        let signed_headers = signed_headers(&self.headers);

        // https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
        // 1. Create a canonical request for Signature Version 4
        //
        //     CanonicalRequest =
        //         HTTPRequestMethod + '\n' +
        //         CanonicalURI + '\n' +
        //         CanonicalQueryString + '\n' +
        //         CanonicalHeaders + '\n' +
        //         SignedHeaders + '\n' +
        //         HexEncode(Hash(RequestPayload))
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            &self.http_method,
            canonical_uri(url)?,
            canonical_query_string(url),
            canonical_headers(&self.headers),
            signed_headers,
            write_hex_bytes(digest_sha256)
        );

        // println!("canonical request: \n---\n{}\n---\n", canonical_request);

        // 2. Create a string to sign for Signature Version 4
        //
        //     StringToSign =
        //         Algorithm + \n +
        //         RequestDateTime + \n +
        //         CredentialScope + \n +
        //         HashedCanonicalRequest
        //
        let scope = format!(
            "{}/{}/{}/aws4_request",
            current_date,
            self.auth.region.name(),
            self.aws_service
        );
        let canonical_request_hash = sha256_digest(canonical_request);
        let string_to_sign =
            string_to_sign(&current_datetime, &scope, canonical_request_hash.as_ref());

        // 3. Calculate the signature for AWS Signature Version 4
        let signing_key = signature_key(
            self.auth.credentials.aws_secret_access_key(),
            &current_date,
            self.auth.region.name(),
            self.aws_service,
        );
        let signature = sha256_hmac(signing_key.as_ref(), string_to_sign.as_bytes());

        // 4. Add the signature to the HTTP request
        let authorization_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.auth.credentials.aws_access_key_id(),
            scope,
            signed_headers,
            write_hex_bytes(signature.as_ref())
        );

        self.add_header("Authorization", &authorization_header);

        Ok(self.headers.clone())
    }

    /// # Errors
    /// Will return `Err` if can not make the URL
    pub fn presigned_url(&mut self, key: &'a str, expire: usize) -> Result<String> {
        let current_date = self.datetime.format("%Y%m%d").to_string();
        let current_datetime = self.datetime.format("%Y%m%dT%H%M%SZ").to_string();

        let mut url = self.auth.endpoint()?;

        let clean_path = key.split('/').collect::<Vec<&str>>();

        for p in clean_path {
            url.path_segments_mut()
                .map_err(|e| anyhow!("cannot be base: {:#?}", e))?
                .push(p);
        }

        let scope = format!(
            "{}/{}/{}/aws4_request",
            current_date,
            self.auth.region.name(),
            self.aws_service,
        );

        for pair in &[
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            (
                "X-Amz-Credential",
                &format!("{}/{}", self.auth.credentials.aws_access_key_id(), scope),
            ),
            ("X-Amz-Date", &current_datetime),
            ("X-Amz-Expires", &expire.to_string()),
            ("X-Amz-SignedHeaders", "host"),
        ] {
            url.query_pairs_mut().append_pair(pair.0, pair.1);
        }

        self.add_header("host", &self.auth.region.endpoint());

        let signed_headers = signed_headers(&self.headers);

        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        // 1. Create a canonical request for Signature Version 4
        //
        //     CanonicalRequest =
        //         HTTPRequestMethod + '\n' +
        //         CanonicalURI + '\n' +
        //         CanonicalQueryString + '\n' +
        //         CanonicalHeaders + '\n' +
        //         SignedHeaders + '\n' +
        //         UNSIGNED-PAYLOAD
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            &self.http_method,
            canonical_uri(&url)?,
            canonical_query_string(&url),
            canonical_headers(&self.headers),
            signed_headers,
        );

        // 2. Create a string to sign for Signature Version 4
        //
        //     StringToSign =
        //         Algorithm + \n +
        //         RequestDateTime + \n +
        //         CredentialScope + \n +
        //         HashedCanonicalRequest
        //
        let canonical_request_hash = sha256_digest(canonical_request);
        let string_to_sign =
            string_to_sign(&current_datetime, &scope, canonical_request_hash.as_ref());

        // 3. Calculate the signature for AWS Signature Version 4
        let signing_key = signature_key(
            self.auth.credentials.aws_secret_access_key(),
            &current_date,
            self.auth.region.name(),
            self.aws_service,
        );

        let signature =
            write_hex_bytes(sha256_hmac(signing_key.as_ref(), string_to_sign.as_bytes()).as_ref());

        url.query_pairs_mut()
            .append_pair("X-Amz-Signature", &signature);

        Ok(url.to_string())
    }

    pub fn add_header(&mut self, key: &str, value: &str) {
        let key = key.to_string().to_ascii_lowercase();
        self.headers.insert(key, value.trim().to_string());
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
//
// The standard UriEncode functions provided by your development platform may
// not work because of differences in implementation and related ambiguity in the
// underlying RFCs. We recommend that you write your own custom UriEncode function
// to ensure that your encoding will work.
//
/// # Errors
/// Will return Err if can't `percent_decode` the path
pub fn canonical_uri(uri: &Url) -> Result<String> {
    const URLENCODE_PATH_FRAGMENT: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'/')
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');

    let decode_url = percent_decode(uri.path().as_bytes()).decode_utf8()?;

    Ok(utf8_percent_encode(&decode_url, URLENCODE_PATH_FRAGMENT).to_string())
}

// CanonicalQueryString specifies the URI-encoded query string parameters.
#[must_use]
pub fn canonical_query_string(uri: &Url) -> String {
    const URLENCODE_QUERY_FRAGMENT: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');

    // You URI-encode name and values individually.
    let mut pairs = uri
        .query_pairs()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(&key, URLENCODE_QUERY_FRAGMENT),
                utf8_percent_encode(&value, URLENCODE_QUERY_FRAGMENT)
            )
        })
        .collect::<Vec<String>>();

    // You must also sort the parameters in the canonical query string
    // alphabetically by key name. The sorting occurs after encoding.
    pairs.sort();

    pairs.join("&")
}

fn canonical_headers(headers: &BTreeMap<String, String>) -> String {
    let mut canonical = String::new();
    for (key, value) in headers {
        canonical.push_str(format!("{key}:{value}\n").as_ref());
    }
    canonical
}

// Create a string to sign for Signature Version 4
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
#[must_use]
pub fn string_to_sign(timestamp: &str, scope: &str, hashed_canonical_request: &[u8]) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp,
        scope,
        write_hex_bytes(hashed_canonical_request)
    )
}

fn signed_headers(headers: &BTreeMap<String, String>) -> String {
    let mut signed = String::new();
    for key in headers.keys() {
        if !signed.is_empty() {
            signed.push(';');
        }
        signed.push_str(key);
    }
    signed
}

fn signature_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_date = sha256_hmac(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let k_region = sha256_hmac(k_date.as_ref(), region.as_bytes());
    let k_service = sha256_hmac(k_region.as_ref(), service.as_bytes());
    sha256_hmac(k_service.as_ref(), b"aws4_request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region};
    use secrecy::SecretString;

    #[test]
    fn test_presigned_url() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        let credentials = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
        );
        let region = Region::Custom {
            name: "us-east-1".to_string(),
            endpoint: "examplebucket.s3.amazonaws.com".to_string(),
        };
        let date = DateTime::parse_from_rfc2822("Fri, 24 May 2013 00:00:00 GMT").unwrap();
        let date_utc: DateTime<Utc> = date.with_timezone(&Utc);

        let s3 = S3::new(&credentials, &region, None, false);
        let mut sign = Signature::new(&s3, "s3", Method::from_bytes(b"GET").unwrap()).unwrap();
        sign.datetime = date_utc;
        let rs = sign.presigned_url("/test.txt", 86400).unwrap();
        assert_eq!(
            "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
            &rs
        );
    }

    #[test]
    fn test_signature_key() {
        let rs = write_hex_bytes(
            signature_key(
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "20150830",
                "us-east-1",
                "iam",
            )
            .as_ref(),
        );
        assert_eq!(
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
            rs
        );
    }
}
