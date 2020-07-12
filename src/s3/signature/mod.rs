//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::s3::S3;
use chrono::prelude::{DateTime, Utc};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use ring::{digest, hmac};
use std::collections::BTreeMap;
use std::error;
use std::fmt::Write;
use std::str;
use url::Url;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Debug)]
pub struct Signature {
    // S3
    auth: S3,
    // The HTTPRequestMethod
    pub http_method: String,
    // The CanonicalURI
    pub canonical_uri: String,
    // The CanonicalQueryString
    pub canonical_query_string: String,
    // The HTTP request headers
    pub headers: BTreeMap<String, String>,
    // current date & time
    datetime: DateTime<Utc>,
}

impl Signature {
    // #[must_use]
    pub fn new(s3: S3, method: String, url: &Url) -> Result<Self, Box<dyn error::Error>> {
        Ok(Self {
            auth: s3,
            http_method: method,
            canonical_uri: canonical_uri(url),
            canonical_query_string: canonical_query_string(url),
            datetime: Utc::now(),
            headers: BTreeMap::new(),
        })
    }

    // The HexEncode(Hash(RequestPayload))
    pub fn sign(
        &mut self,
        payload: &str,
    ) -> Result<BTreeMap<String, String>, Box<dyn error::Error>> {
        let current_date = self.datetime.format("%Y%m%d");
        let current_datetime = self.datetime.format("%Y%m%dT%H%M%SZ");

        self.add_header("host", &self.auth.host.to_string());
        self.add_header("x-amz-date", &current_datetime.to_string());
        self.add_header("User-Agent", &APP_USER_AGENT.to_string());

        // TODO (pass digest after reading file maybe)
        let digest = sha256_digest(payload);
        self.add_header("x-amz-content-sha256", &digest);

        //        let canonical_headers = canonical_headers(&self.headers);
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
            &self.canonical_uri,
            &self.canonical_query_string,
            canonical_headers(&self.headers),
            signed_headers,
            digest
        );

        //println!("canonical request: \n---\n{}\n---\n", canonical_request);

        // 2. Create a string to sign for Signature Version 4
        //
        //     StringToSign =
        //         Algorithm + \n +
        //         RequestDateTime + \n +
        //         CredentialScope + \n +
        //         HashedCanonicalRequest
        //
        let scope = format!(
            "{}/{}/s3/aws4_request",
            &current_date,
            self.auth.region.name()
        );
        let canonical_request_hash = sha256_digest(&canonical_request);
        let string_to_sign = string_to_sign(
            &current_datetime.to_string(),
            &scope,
            &canonical_request_hash,
        );

        // 3. Calculate the signature for AWS Signature Version 4
        let signing_key = signature_key(
            self.auth.credentials.aws_secret_access_key(),
            &current_date.to_string(),
            self.auth.region.name(),
            "s3",
        );
        let s_key = hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref());
        let signature = hmac::sign(&s_key, string_to_sign.as_bytes());

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

    pub fn add_header(&mut self, key: &str, value: &str) {
        let key = key.to_string().to_ascii_lowercase();
        self.headers.insert(key, value.trim().to_string());
    }
}

// CanonicalURI is the URI-encoded version of the absolute path component of the URI—everything
// starting with the "/" that follows the domain name and up to the end of the string or to the
// question mark character ('?') if you have query string parameters. The URI in the following
// example, /examplebucket/myphoto.jpg, is the absolute path and you don't encode the "/" in the
// absolute path:
// http://s3.amazonaws.com/examplebucket/myphoto.jpg
//
// URI encode every byte except the unreserved characters:
// 'A'-'Z', 'a'-'z', '0'-'9', '-', '.', '_', and '~'.
#[must_use]
pub fn canonical_uri(uri: &Url) -> String {
    const FRAGMENT: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'/')
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    utf8_percent_encode(uri.path(), FRAGMENT).to_string()
}

// CanonicalQueryString specifies the URI-encoded query string parameters. You URI-encode name and
// values individually. You must also sort the parameters in the canonical query string
// alphabetically by key name. The sorting occurs after encoding.
#[must_use]
pub fn canonical_query_string(uri: &Url) -> String {
    const FRAGMENT: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    let mut pairs = uri
        .query_pairs()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(&key, FRAGMENT).to_string(),
                utf8_percent_encode(&value, FRAGMENT).to_string()
            )
        })
        .collect::<Vec<String>>();
    pairs.sort();
    pairs.join("&")
}

fn canonical_headers(headers: &BTreeMap<String, String>) -> String {
    let mut canonical = String::new();
    for (key, value) in headers.iter() {
        canonical.push_str(format!("{}:{}\n", key, value).as_ref());
    }
    canonical
}

// Create a string to sign for Signature Version 4
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
#[must_use]
pub fn string_to_sign(timestamp: &str, scope: &str, hashed_canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp, scope, hashed_canonical_request
    )
}

fn signed_headers(headers: &BTreeMap<String, String>) -> String {
    let mut signed = String::new();
    headers.iter().for_each(|(key, _)| {
        if !signed.is_empty() {
            signed.push(';');
        }
        signed.push_str(key);
    });
    signed
}

// TODO for empty string or full payload
fn sha256_digest(string: &str) -> String {
    write_hex_bytes(digest::digest(&digest::SHA256, string.as_bytes()).as_ref())
}

fn hmac(key: &[u8], msg: &[u8]) -> hmac::Tag {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&s_key, msg)
}

fn signature_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_date = hmac(
        format!("AWS4{}", secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(k_date.as_ref(), region.as_bytes());
    let k_service = hmac(k_region.as_ref(), service.as_bytes());
    hmac(k_service.as_ref(), b"aws4_request")
}

fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
