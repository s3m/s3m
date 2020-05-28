use chrono::prelude::{DateTime, Utc};
use std::collections::BTreeMap;

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
    pub region: String,
    // The CanonicalURI
    pub canonical_uri: String,
    // The CanonicalQueryString
    pub canonical_query_string: String,
    // The CanonicalHeaders
    pub canonical_headers: BTreeMap<String, Vec<Vec<u8>>>,
    // The SignedHeaders
    pub signed_headers: String,
    // The HexEncode(Hash(RequestPayload))
    pub payload: String,
}

impl Signature {
    pub fn new(method: &str, region: &str) -> Signature {
        Signature {
            method: method.to_string(),
            region: region.to_string(),
            canonical_uri: String::new(),
            canonical_query_string: String::new(),
            canonical_headers: BTreeMap::new(),
            signed_headers: String::new(),
            payload: String::new(),
        }
    }

    pub fn sign(&mut self) {
        let now = Utc::now();
        let date = now.format("%Y%m%d");

        let scope = format!("{}/{}/s3/aws4_request", date, self.region);

        let string_to_sign = string_to_sign(now, &scope, &scope);
        println!("{}", string_to_sign);
    }
}

// Create a string to sign for Signature Version 4
// https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
pub fn string_to_sign(date: DateTime<Utc>, scope: &str, hashed_canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date.format("%Y%m%dT%H%M%SZ"),
        scope,
        hashed_canonical_request
    )
}
