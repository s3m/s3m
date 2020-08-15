use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::tools;
use crate::s3::S3;
// use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Default, Clone)]
pub struct UploadPart {
    key: String,
    file: String,
    part_number: String,
    upload_id: String,
    seek: u64,
    chunk: u64,
    pub content_length: Option<String>,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<String>,
    pub x_amz_server_side_encryption_customer_key: Option<String>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<String>,
    pub x_amz_request_payer: Option<String>,
}

impl UploadPart {
    #[must_use]
    pub fn new(
        key: String,
        file: String,
        part_number: String,
        upload_id: String,
        seek: u64,
        chunk: u64,
    ) -> Self {
        Self {
            key,
            file,
            part_number,
            upload_id,
            seek,
            chunk,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: &S3) -> Result<String, Box<dyn error::Error>> {
        let (digest, length) = tools::sha256_digest_multipart(&self.file, self.seek, self.chunk)?;
        let (url, headers) = &self.sign(s3, &digest, Some(length))?;
        let response = request::request_multipart(
            url.clone(),
            self.http_verb(),
            headers,
            self.file.to_string(),
            self.seek,
            self.chunk,
        )
        .await?;
        if response.status().is_success() {
            match response.headers().get("ETag") {
                Some(etag) => Ok(etag.to_str()?.to_string()),
                _ => Err("missing ETag".into()),
            }
        } else {
            Err(response_error(response).await?.into())
        }
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
impl Action for UploadPart {
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
        map.insert("uploadId", &self.upload_id);
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
