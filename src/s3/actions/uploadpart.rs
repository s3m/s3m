use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::tools;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use http::method::Method;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct UploadPart<'a> {
    key: &'a str,
    file: &'a str,
    part_number: String,
    upload_id: &'a str,
    seek: u64,
    chunk: u64,
    pub content_length: Option<String>,
    pub content_type: Option<String>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<String>,
    pub x_amz_server_side_encryption_customer_key: Option<String>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<String>,
    pub x_amz_request_payer: Option<String>,
}

impl<'a> UploadPart<'a> {
    #[must_use]
    pub fn new(
        key: &'a str,
        file: &'a str,
        part_number: u16,
        upload_id: &'a str,
        seek: u64,
        chunk: u64,
    ) -> Self {
        let pn = part_number.to_string();
        Self {
            key,
            file,
            part_number: pn,
            upload_id,
            seek,
            chunk,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String> {
        let (sha256, md5, length) =
            tools::sha256_md5_digest_multipart(self.file, self.seek, self.chunk).await?;
        let (url, headers) = &self.sign(s3, &sha256, Some(&md5), Some(length))?;
        let response = request::multipart_upload(
            url.clone(),
            self.http_method(),
            headers,
            self.file.to_string(),
            self.seek,
            self.chunk,
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
impl<'a> Action for UploadPart<'a> {
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

    #[test]
    fn test_method() {
        let action = UploadPart::new("key", "file", 1, "uid", 1, 1);
        assert_eq!(Method::PUT, action.http_method());
    }
}
