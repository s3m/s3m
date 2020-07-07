//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::s3::S3;
use chrono::prelude::{DateTime, Utc};
use http::Method;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client,
};
use ring::{digest, hmac};
use std::collections::{btree_map::Entry, BTreeMap};
use std::fmt::Write;
use std::str;
use url::Url;

#[derive(Debug)]
pub struct Signature {
    // S3
    s3: S3,
    // The HTTPRequestMethod
    pub method: String,
    // The HTTP request path (/bucket/)
    pub path: String,
    // CanonicalQueryString
    pub query_string: String,
    // The HTTP request headers
    pub headers: BTreeMap<String, Vec<Vec<u8>>>,
    // The SignedHeaders
    pub signed_headers: String,
    // The HexEncode(Hash(RequestPayload))
    pub payload: String,
    // current date & time
    datetime: DateTime<Utc>,
}

impl Signature {
    #[must_use]
    pub fn new(s3: S3, method: &str, path: &str, query_string: &str) -> Self {
        Self {
            s3: s3,
            method: method.to_string(),
            path: path.to_string(),
            query_string: query_string.to_string(),
            datetime: Utc::now(),
            headers: BTreeMap::new(),
            signed_headers: String::new(),
            payload: String::new(),
        }
    }

    pub fn sign(&mut self) {
        let current_date = self.datetime.format("%Y%m%d");
        let current_datetime = self.datetime.format("%Y%m%dT%H%M%SZ");

        let host = format!("s3.{}.amazonaws.com", self.s3.region.name());
        self.add_header("host", &host);
        self.add_header("x-amz-date", &current_datetime.to_string());

        // TODO (pass digest after reading file maybe)
        let digest = sha256_digest(&self.payload);
        self.add_header("x-amz-content-sha256", &digest);

        let signed_headers = signed_headers(&self.headers);
        let canonical_headers = canonical_headers(&self.headers);

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
            &self.method, &self.path, &self.query_string, canonical_headers, signed_headers, digest
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
            "{}/{}/s3/aws4_request",
            &current_date,
            self.s3.region.name()
        );
        let canonical_request_hash = sha256_digest(&canonical_request);
        let string_to_sign = string_to_sign(
            &current_datetime.to_string(),
            &scope,
            &canonical_request_hash,
        );

        // 3. Calculate the signature for AWS Signature Version 4
        let signing_key = signature_key(
            self.s3.credentials.aws_secret_access_key(),
            &current_date.to_string(),
            self.s3.region.name(),
            "s3",
        );
        let s_key = hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref());
        let signature = hmac::sign(&s_key, string_to_sign.as_bytes());

        // 4. Add the signature to the HTTP request
        let authorization_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.s3.credentials.aws_access_key_id(),
            scope,
            signed_headers,
            write_hex_bytes(signature.as_ref())
        );

        let url = Url::parse(format!("https://{}/s3mon/?list-type=2", host).as_str()).unwrap();
        println!("url: {}", url);
        let mut headers = self
            .headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.parse::<HeaderName>().unwrap(),
                    canonical_values(v).parse::<HeaderValue>().unwrap(),
                ))
            })
            .collect::<Result<HeaderMap, ()>>()
            .unwrap();

        headers.insert("Authorization", authorization_header.parse().unwrap());

        let client = Client::new();
        let request = client
            .request(Method::from_bytes(self.method.as_bytes()).unwrap(), url)
            .headers(headers)
            .body("");

        println!("{:#?}", request);

        //let resp = request.send().await.unwrap();
        // println!("---> {:#?}", resp.text().await.unwrap());
    }

    pub fn add_header<K: ToString>(&mut self, key: K, value: &str) {
        let key = key.to_string().to_ascii_lowercase();
        let value = value.as_bytes().to_vec();
        match self.headers.entry(key) {
            Entry::Vacant(entry) => {
                let mut values = Vec::new();
                values.push(value);
                entry.insert(values);
            }
            Entry::Occupied(entry) => {
                entry.into_mut().push(value);
            }
        }
    }
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

fn signed_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
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

fn canonical_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
    let mut canonical = String::new();
    for (key, value) in headers.iter() {
        canonical.push_str(format!("{}:{}\n", key, canonical_values(value)).as_ref());
    }
    canonical
}

fn canonical_values(values: &[Vec<u8>]) -> String {
    let mut st = String::new();
    for v in values {
        let s = str::from_utf8(v).unwrap();
        if !st.is_empty() {
            st.push(',')
        }
        if s.starts_with('\"') {
            st.push_str(s);
        } else {
            st.push_str(s.replace("  ", " ").trim());
        }
    }
    st
}

fn hmac(key: &[u8], msg: &[u8]) -> hmac::Tag {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key.as_ref());
    hmac::sign(&s_key, msg)
}

fn signature_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_date = hmac(
        format!("AWS4{}", secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(k_date.as_ref(), region.as_bytes());
    let k_service = hmac(k_region.as_ref(), service.as_bytes());
    hmac(k_service.as_ref(), "aws4_request".as_bytes())
}

fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
