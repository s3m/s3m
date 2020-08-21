use crate::s3::actions::{response_error, Action};
use crate::s3::request;
use crate::s3::tools;
use crate::s3::S3;
// use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Default)]
pub struct PutObject<'a> {
    key: &'a str,
    file: &'a str,
    sender: Option<UnboundedSender<usize>>,
    pub x_amz_acl: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_length: Option<String>,
    pub content_md5: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<String>,
    pub x_amz_grant_full_control: Option<String>,
    pub x_amz_grant_read: Option<String>,
    pub x_amz_grant_read_acp: Option<String>,
    pub x_amz_grant_write_acp: Option<String>,
    pub x_amz_server_side_encryption: Option<String>,
    pub x_amz_storage_class: Option<String>,
    pub x_amz_website_redirect_location: Option<String>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<String>,
    pub x_amz_server_side_encryption_customer_key: Option<String>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<String>,
    pub x_amz_server_side_encryption_aws_kms_key_id: Option<String>,
    pub x_amz_server_side_encryption_context: Option<String>,
    pub x_amz_request_payer: Option<String>,
    pub x_amz_tagging: Option<String>,
    pub x_amz_object_lock_mode: Option<String>,
    pub x_amz_object_lock_retain_until_date: Option<String>,
    pub x_amz_object_lock_legal_hold: Option<String>,
}

impl<'a> PutObject<'a> {
    #[must_use]
    pub fn new(key: &'a str, file: &'a str, sender: Option<UnboundedSender<usize>>) -> Self {
        Self {
            key,
            file,
            sender,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(self, s3: &S3) -> Result<String, Box<dyn error::Error>> {
        let (sha, md5, length) = tools::sha256_md5_digest(self.file).await?;
        let (url, headers) = &self.sign(s3, &sha, Some(&md5), Some(length))?;
        let response = request::request(
            url.clone(),
            self.http_verb(),
            headers,
            Some(self.file.to_string()),
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
            Ok(h.iter()
                .map(|(k, v)| format!("{}: {}\n", k, v))
                .collect::<String>())
        } else {
            Err(response_error(response).await?.into())
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl<'a> Action for PutObject<'a> {
    fn http_verb(&self) -> &'static str {
        "PUT"
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        None
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
