use crate::s3::actions::Action;
use crate::s3::request;
use crate::s3::tools;
use crate::s3::S3;
// use serde_xml_rs::from_str;
use std::collections::BTreeMap;
use std::error;

#[derive(Debug, Default)]
pub struct PutObject {
    key: String,
    file: String,
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

impl PutObject {
    #[must_use]
    pub fn new(key: String, file: String) -> Self {
        Self {
            key,
            file,
            ..Default::default()
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(&self, s3: S3) -> Result<(), Box<dyn error::Error>> {
        let (digest, length) = tools::sha256_digest(&self.file)?;
        let (url, headers) = &self.sign(s3, &digest, Some(length))?;
        let response = request::request(
            url.clone(),
            self.http_verb(),
            headers,
            Some(self.file.to_string()),
        )
        .await?;

        println!("rs status: {}", response.status());
        //       if response.status() == 200 {
        //      println!("status: {}", response.status());
        let rs = response.text().await?;
        println!("rs: {}", rs);
        Ok(())
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
impl Action for PutObject {
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
