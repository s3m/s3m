use crate::{
    s3::actions::{response_error, Action},
    s3::{request, S3},
};
use anyhow::{anyhow, Result};
use crossbeam::channel::Sender;
use reqwest::Method;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone)]
pub struct StreamPart<'a> {
    key: &'a str,
    path: &'a Path,
    part_number: String,
    upload_id: &'a str,
    length: usize,
    digest: (&'a [u8], &'a [u8]),
    sender: Option<Sender<usize>>,
}

impl<'a> StreamPart<'a> {
    #[must_use]
    pub fn new(
        key: &'a str,
        path: &'a Path,
        part_number: u16,
        upload_id: &'a str,
        length: usize,
        digest: (&'a [u8], &'a [u8]),
        sender: Option<Sender<usize>>,
    ) -> Self {
        let pn = part_number.to_string();
        Self {
            key,
            path,
            part_number: pn,
            upload_id,
            length,
            digest,
            sender,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<String> {
        let (url, headers) =
            &self.sign(s3, self.digest.0, Some(self.digest.1), Some(self.length))?;

        let response = request::request(
            url.clone(),
            self.http_method()?,
            headers,
            Some(self.path),
            self.sender,
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
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
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

    #[test]
    fn test_method() {
        let action = StreamPart::new("key", Path::new("/"), 1, "uid", 0, (b"sha", b"md5"), None);
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }
}
