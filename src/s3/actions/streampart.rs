use crate::{
    s3::actions::{response_error, Action},
    s3::{request, tools, S3},
};
use anyhow::{anyhow, Result};
use crossbeam::channel::Sender;
use http::method::Method;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct StreamPart<'a> {
    key: &'a str,
    path: &'a Path,
    part_number: String,
    upload_id: &'a str,
    length: usize,
    sha256: String,
    md5: String,
}

impl<'a> StreamPart<'a> {
    #[must_use]
    pub fn new(
        key: &'a str,
        path: &'a Path,
        part_number: u16,
        upload_id: &'a str,
        length: usize,
        sha256: String,
        md5: String,
    ) -> Self {
        let pn = part_number.to_string();
        Self {
            key,
            path,
            part_number: pn,
            upload_id,
            length,
            sha256,
            md5,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<String> {
        let (url, headers) = &self.sign(s3, &self.sha256, Some(&self.md5), Some(self.length))?;
        let response = request::request(
            url.clone(),
            self.http_method(),
            headers,
            Some(self.path),
            None,
        )
        .await?;
        if response.status().is_success() {
            match response.headers().get("ETag") {
                Some(etag) => Ok(etag.to_str()?.to_string()),
                None => Err(anyhow!("missing ETag")),
            }
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
impl<'a> Action for StreamPart<'a> {
    fn http_method(&self) -> Method {
        Method::from_bytes(b"PUT").unwrap()
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
    }

    // URL query_pairs
    fn query_pairs(&self) -> Option<BTreeMap<&str, &str>> {
        let mut map: BTreeMap<&str, &str> = BTreeMap::new();
        map.insert("partNumber", &self.part_number);
        map.insert("uploadId", self.upload_id);
        Some(map)
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
    use bytes::Bytes;

    #[test]
    fn test_method() {
        let action = StreamPart::new("key", Bytes::from("Hello world"), 1, "uid", None);
        assert_eq!(Method::PUT, action.http_method());
    }
}
