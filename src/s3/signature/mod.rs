//!  S3 signature v4
//! <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>

use crate::s3::{credentials::Credentials, region::Region};
use chrono::prelude::Utc;
use http::Method;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client,
};
use ring::{digest, hmac};
use serde_xml_rs;
use std::collections::{btree_map::Entry, BTreeMap};
use std::fmt::Write;
use std::str;
use url::Url;

#[derive(Debug)]
pub struct Signature {
    // The HTTPRequestMethod
    pub method: String,
    // AWS Region
    pub region: Region,
    // AWS Credentials
    pub creds: Credentials,
    // The HTTP request path (/bucket/)
    pub path: String,
    // The HTTP request headers
    pub headers: BTreeMap<String, Vec<Vec<u8>>>,
    // The SignedHeaders
    pub signed_headers: String,
    // The HexEncode(Hash(RequestPayload))
    pub payload: String,
}

impl Signature {
    #[must_use]
    pub fn new(method: &str, region: &Region, path: &str, creds: &Credentials) -> Self {
        Self {
            method: method.to_string(),
            region: region.clone(),
            creds: creds.clone(),
            path: path.to_string(),
            headers: BTreeMap::new(),
            signed_headers: String::new(),
            payload: String::new(),
        }
    }

    pub async fn sign(&mut self) {
        let now = Utc::now();
        let current_date = now.format("%Y%m%d");
        let current_datetime = now.format("%Y%m%dT%H%M%SZ");

        let host = format!("s3.{}.amazonaws.com", self.region.name());
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
            &self.method, &self.path, "", canonical_headers, signed_headers, digest
        );

        // 2. Create a string to sign for Signature Version 4
        //
        //     StringToSign =
        //         Algorithm + \n +
        //         RequestDateTime + \n +
        //         CredentialScope + \n +
        //         HashedCanonicalRequest
        //
        let scope = format!("{}/{}/s3/aws4_request", &current_date, self.region.name());
        let canonical_request_hash = sha256_digest(&canonical_request);
        let string_to_sign = string_to_sign(
            &current_datetime.to_string(),
            &scope,
            &canonical_request_hash,
        );

        // 3. Calculate the signature for AWS Signature Version 4
        let signing_key = signature_key(
            self.creds.aws_secret_access_key(),
            &current_date.to_string(),
            self.region.name(),
            "s3",
        );
        let s_key = hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref());
        let signature = hmac::sign(&s_key, string_to_sign.as_bytes());

        // 4. Add the signature to the HTTP request
        let authorization_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.creds.aws_access_key_id(),
            scope,
            signed_headers,
            write_hex_bytes(signature.as_ref())
        );

        let client = Client::new();
        let url = Url::parse(format!("https://{}/s3mon", host).as_str()).unwrap();
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

        let request = client
            .request(Method::from_bytes(self.method.as_bytes()).unwrap(), url)
            .headers(headers)
            .body("");

        println!("{:#?}", request);

        let resp = request.send().await.unwrap();
        println!("---> {:#?}", resp.text().await.unwrap());
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
