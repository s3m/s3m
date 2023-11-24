use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use crossbeam::channel::Sender;
use reqwest::Method;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug)]
pub struct PutObject<'a> {
    key: &'a str,
    file: &'a Path,
    acl: Option<String>,
    meta: Option<BTreeMap<String, String>>,
    sender: Option<Sender<usize>>,
}

impl<'a> PutObject<'a> {
    #[must_use]
    pub const fn new(
        key: &'a str,
        file: &'a Path,
        acl: Option<String>,
        meta: Option<BTreeMap<String, String>>,
        sender: Option<Sender<usize>>,
    ) -> Self {
        Self {
            key,
            file,
            acl,
            meta,
            sender,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<BTreeMap<&str, String>> {
        let (sha, md5, length) = tools::sha256_md5_digest(self.file).await?;
        // TODO
        // pass headers
        let (url, headers) = &self.sign(s3, sha.as_ref(), Some(md5.as_ref()), Some(length))?;
        let response = request::request(
            url.clone(),
            self.http_method()?,
            headers,
            Some(Path::new(self.file)),
            self.sender,
        )
        .await?;

        if response.status().is_success() {
            let mut h: BTreeMap<&str, String> = BTreeMap::new();
            if let Some(etag) = response.headers().get("ETag") {
                h.insert("ETag", etag.to_str()?.to_string());
            }
            if let Some(vid) = response.headers().get("x-amz-version-id") {
                h.insert("Version ID", vid.to_str()?.to_string());
            }
            if let Some(sse) = response.headers().get("x-amz-server-side-encryption") {
                h.insert("Server-side encryption", sse.to_str()?.to_string());
            }
            if let Some(exp) = response.headers().get("x-amz-expiration") {
                h.insert("Expiration", exp.to_str()?.to_string());
            }
            if let Some(pos) = response.headers().get("x-emc-previous-object-size") {
                h.insert("Previous object size", pos.to_str()?.to_string());
            }
            Ok(h)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl<'a> Action for PutObject<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();

        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
        if let Some(acl) = &self.acl {
            map.insert("x-amz-acl", acl);
        }

        if let Some(meta) = &self.meta {
            for (k, v) in meta {
                map.insert(k, v);
            }
        }

        Some(map)
    }

    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    fn path(&self) -> Option<Vec<&str>> {
        // remove leading / or //
        let clean_path = self
            .key
            .split('/')
            .filter(|p| !p.is_empty())
            .collect::<Vec<&str>>();
        Some(clean_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method() {
        let action = PutObject::new("key", Path::new("/"), None, None, None);
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }
}
