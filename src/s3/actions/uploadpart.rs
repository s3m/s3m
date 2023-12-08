use crate::{
    s3::actions::{response_error, Action},
    s3::{
        checksum::{sha256_md5_digest_multipart, Checksum},
        request, S3,
    },
};
use anyhow::{anyhow, Result};
use reqwest::Method;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug)]
pub struct UploadPart<'a> {
    key: &'a str,
    file: &'a Path,
    part_number: String,
    upload_id: &'a str,
    seek: u64,
    chunk: u64,
    additional_checksum: Option<&'a mut Checksum>,
    headers: Option<BTreeMap<String, String>>,
}

impl<'a> UploadPart<'a> {
    #[must_use]
    pub fn new(
        key: &'a str,
        file: &'a Path,
        part_number: u16,
        upload_id: &'a str,
        seek: u64,
        chunk: u64,
        additional_checksum: Option<&'a mut Checksum>,
    ) -> Self {
        let pn = part_number.to_string();

        log::debug!(
            "key: {key}, file: {file}, part_number: {pn}, upload_id: {upload_id}, seek: {seek}, chunk: {chunk}, additional_checksum: {additional_checksum:?}",
            key = key,
            file = file.display(),
            pn = pn,
            upload_id = upload_id,
            seek = seek,
            chunk = chunk,
            additional_checksum = additional_checksum
        );

        Self {
            key,
            file,
            part_number: pn,
            upload_id,
            seek,
            chunk,
            additional_checksum,
            headers: None,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(mut self, s3: &S3) -> Result<String> {
        let (sha256, md5, length, checksum) = sha256_md5_digest_multipart(
            self.file,
            self.seek,
            self.chunk,
            self.additional_checksum.take(),
        )
        .await?;

        // Update self.additional_checksum with the modified checksum, if any
        if let Some(ref mut additional_checksum) = self.additional_checksum {
            if let Some(ref new_checksum) = checksum {
                // Update the fields of additional_checksum with the modified values
                additional_checksum.algorithm = new_checksum.algorithm.clone();
                additional_checksum.checksum = new_checksum.checksum.clone();
            }
        }

        // add additional checksum to headers if provided
        if let Some(ref checksum) = checksum {
            // get the x-amz- header
            let amz_header = checksum.algorithm.as_amz().to_string();

            // update headers if exists
            if let Some(map) = self.headers.as_mut() {
                map.insert(amz_header, checksum.checksum.clone());
            } else {
                // create headers if not exists
                let mut map = BTreeMap::new();
                map.insert(amz_header, checksum.checksum.clone());
                self.headers = Some(map);
            }
        }

        let (url, headers) = &self.sign(s3, sha256.as_ref(), Some(md5.as_ref()), Some(length))?;

        let response = request::multipart_upload(
            url.clone(),
            self.http_method()?,
            headers,
            self.file,
            self.seek,
            self.chunk,
        )
        .await?;

        if response.status().is_success() {
            // if no additional checksum was provided, return only the ETag
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
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"PUT")?)
    }

    fn headers(&self) -> Option<BTreeMap<&str, &str>> {
        self.headers.as_ref().map(|map| {
            // Convert the headers to a new map with borrowed references
            map.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
        })
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
    use crate::s3::{
        tools, {Credentials, Region, S3},
    };
    use secrecy::Secret;

    #[test]
    fn test_method() {
        let action = UploadPart::new("key", Path::new("file"), 1, "uid", 1, 1, None);
        assert_eq!(Method::PUT, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs() {
        let action = UploadPart::new("key", Path::new("file"), 1, "uid", 1, 1, None);
        let mut map = BTreeMap::new();
        map.insert("partNumber", "1");
        map.insert("uploadId", "uid");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_path() {
        let action = UploadPart::new("key", Path::new("file"), 1, "uid", 1, 1, None);
        assert_eq!(Some(vec!["key"]), action.path());
    }

    #[test]
    fn test_sign() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );

        let action = UploadPart::new("key", Path::new("file"), 1, "uid", 1, 1, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?partNumber=1&uploadId=uid",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
