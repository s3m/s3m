use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::tools;
use crate::s3::S3;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct StreamPart<'a> {
    key: &'a str,
    stream: Bytes,
    part_number: String,
    upload_id: &'a str,
    pub content_length: Option<String>,
    pub content_type: Option<String>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<String>,
    pub x_amz_server_side_encryption_customer_key: Option<String>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<String>,
    pub x_amz_request_payer: Option<String>,
}

impl<'a> StreamPart<'a> {
    #[must_use]
    pub fn new(key: &'a str, stream: Bytes, part_number: u16, upload_id: &'a str) -> Self {
        let pn = part_number.to_string();
        Self {
            key,
            stream,
            part_number: pn,
            upload_id,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String> {
        let sha256 = tools::sha256_digest(&self.stream);
        let md5 = tools::base64_md5(&self.stream);

        let (url, headers) = &self.sign(s3, &sha256, Some(&md5), None)?;
        let response = request::upload(
            url.clone(),
            self.http_verb(),
            headers,
            self.stream.to_owned(),
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
    fn http_verb(&self) -> &'static str {
        "PUT"
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
