use crate::credentials::Credentials;
use crate::region::Region;
use chrono::prelude::Utc;
use http::Method;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response};
use ring::{digest, hmac};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::str;
use url::Url;

/// https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
///
/// 1. Create a canonical request for Signature Version 4
///
/// Example Canonical request pseudocode:
///     CanonicalRequest =
///         HTTPRequestMethod + '\n' +
///         CanonicalURI + '\n' +
///         CanonicalQueryString + '\n' +
///         CanonicalHeaders + '\n' +
///         SignedHeaders + '\n' +
///         HexEncode(Hash(RequestPayload))
///
/// 2. Create a string to sign for Signature Version 4
///
/// Structure of string to sign:
///     StringToSign =
///         Algorithm + \n +
///         RequestDateTime + \n +
///         CredentialScope + \n +
///         HashedCanonicalRequest
///
/// 3. Calculate the signature for AWS Signature Version 4
/// 4. Add the signature to the HTTP request
#[derive(Debug)]
pub struct Signature {
    // The HTTPRequestMethod
    pub method: String,
    // CredentialScope
    // This value is a string that includes the date, the Region you are targeting, the service you
    // are requesting, and a termination string ("aws4_request") in lowercase characters. The
    // Region and service name strings must be UTF-8 encoded.
    // <date>/<aws-region>/s3/aws4_request
    pub region: Region,
    // AWS Credentials
    pub creds: Credentials,
    // The HTTP request path
    pub path: String,
    // The HTTP request headers
    pub headers: BTreeMap<String, Vec<Vec<u8>>>,
    // The SignedHeaders
    pub signed_headers: String,
    // The HexEncode(Hash(RequestPayload))
    pub payload: String,
}

impl Signature {
    pub fn new(method: &str, region: &Region, path: &str, creds: &Credentials) -> Signature {
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

        let digest = sha256_digest(&self.payload);
        self.add_header("x-amz-content-sha256", &digest);

        let signed_headers = signed_headers(&self.headers);
        let canonical_headers = canonical_headers(&self.headers);

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            &self.method, &self.path, "", canonical_headers, signed_headers, digest
        );
        let scope = format!("{}/{}/s3/aws4_request", &current_date, self.region.name());
        let canonical_request_hash = sha256_digest(&canonical_request);
        println!(
            "{}\n---\n{}\n---",
            canonical_request, canonical_request_hash
        );

        let string_to_sign = string_to_sign(
            &current_datetime.to_string(),
            &scope,
            &canonical_request_hash,
        );
        println!("string_to_sign: \n{}\n", string_to_sign);

        let signing_key = signature_key(
            &self.creds.aws_secret_access_key(),
            &current_date.to_string(),
            self.region.name(),
            "s3",
        );

        println!("signing_key: {}", write_hex_bytes(signing_key.as_ref()));

        let s_key = hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref());
        let signature = hmac::sign(&s_key, string_to_sign.as_bytes());
        println!("signature: {}", write_hex_bytes(signature.as_ref()));

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
pub fn string_to_sign(timestamp: &str, scope: &str, hashed_canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp, scope, hashed_canonical_request
    )
}

fn signed_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
    let mut signed = String::new();
    headers
        .iter()
        .filter(|&(ref key, _)| !skipped_headers(&key))
        .for_each(|(key, _)| {
            if !signed.is_empty() {
                signed.push(';');
            }
            signed.push_str(key);
        });
    signed
}

fn skipped_headers(header: &str) -> bool {
    ["authorization", "content-length", "user-agent"].contains(&header)
}

// TODO for empty string or full payload
fn sha256_digest(string: &str) -> String {
    let mut hash = String::new();
    digest::digest(&digest::SHA256, string.as_bytes())
        .as_ref()
        .iter()
        .for_each(|k| {
            hash.push_str(&format!("{:02x}", k));
        });
    hash
}

fn canonical_headers(headers: &BTreeMap<String, Vec<Vec<u8>>>) -> String {
    let mut canonical = String::new();

    for (key, value) in headers.iter() {
        if skipped_headers(key) {
            continue;
        }
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

fn hmac(key: &str, msg: &str) -> String {
    let mut hash = String::new();
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key.as_ref());
    hmac::sign(&s_key, msg.as_bytes())
        .as_ref()
        .iter()
        .for_each(|k| {
            hash.push_str(&format!("{:02x}", k));
        });
    hash
}

fn signature_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let s_key = hmac::Key::new(
        hmac::HMAC_SHA256,
        format!("AWS4{}", secret_access_key).as_ref(),
    );
    let k_date = hmac::sign(&s_key, date.as_bytes());
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, k_date.as_ref());
    let k_region = hmac::sign(&s_key, region.as_bytes());
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, k_region.as_ref());
    let k_service = hmac::sign(&s_key, service.as_bytes());
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, k_service.as_ref());
    hmac::sign(&s_key, "aws4_request".as_bytes())
    //.as_ref()
    //.iter()
    //.for_each(|k| {
    //hash.push_str(&format!("{:02x}", k));
    //});
    //hash
}

fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
