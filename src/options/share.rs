use crate::s3::{Signature, S3};
use anyhow::Result;
use http::method::Method;

pub fn share(s3: S3, key: String, expire: usize) -> Result<String> {
    Signature::new(&s3, "s3", Method::from_bytes(b"GET").unwrap())?.presigned_url(&key, expire)
}