//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::{
    s3::actions::{response_error, Action},
    s3::responses::CompleteMultipartUploadResult,
    s3::{checksum::Checksum, request, tools, S3},
};
use anyhow::{anyhow, Result};
use base64ct::{Base64, Encoding};
use bytes::Bytes;
use reqwest::Method;
use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};
use serde_xml_rs::{from_str, to_string};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct CompleteMultipartUpload<'a> {
    key: &'a str,
    upload_id: &'a str,
    parts: BTreeMap<u16, Part>,
    additional_checksum: Option<Checksum>,
    headers: Option<BTreeMap<String, String>>,
}

impl<'a> Serialize for CompleteMultipartUpload<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let len = 1 + self.parts.len();
        let mut map = serializer.serialize_struct("CompleteMultipartUpload", len)?;
        for part in self.parts.values() {
            map.serialize_field("Part", part)?;
        }
        map.end()
    }
}

#[derive(Debug, Default, Clone)]
pub struct Part {
    pub etag: String,
    pub number: u16,
    pub checksum: Option<Checksum>,
}

impl Serialize for Part {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        if let Some(checksum) = &self.checksum {
            match checksum.algorithm.as_algorithm() {
                "CRC32" => map.serialize_entry("ChecksumCRC32", &checksum.checksum)?,
                "CRC32C" => map.serialize_entry("ChecksumCRC32C", &checksum.checksum)?,
                "SHA1" => map.serialize_entry("ChecksumSHA1", &checksum.checksum)?,
                "SHA256" => map.serialize_entry("ChecksumSHA256", &checksum.checksum)?,
                _ => (), // do nothing
            }
        }

        map.serialize_entry("ETag", &self.etag)?;
        map.serialize_entry("PartNumber", &self.number)?;

        map.end()
    }
}

impl<'a> CompleteMultipartUpload<'a> {
    #[must_use]
    pub fn new(
        key: &'a str,
        upload_id: &'a str,
        parts: BTreeMap<u16, Part>,
        additional_checksum: Option<Checksum>,
    ) -> Self {
        Self {
            key,
            upload_id,
            parts,
            additional_checksum,
            headers: None,
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    pub async fn request(mut self, s3: &S3) -> Result<CompleteMultipartUploadResult> {
        let parts = CompleteMultipartUpload {
            parts: self.parts.clone(),
            ..Self::default()
        };

        let body = to_string(&parts)?;

        let digest = tools::sha256_digest(&body);

        // add additional checksum to headers if provided
        if let Some(ref additional_checksum) = &self.additional_checksum {
            // get the amz header name
            let amz_header = additional_checksum.algorithm.as_amz();

            // For example, consider an object uploaded with a multipart
            // upload that has an ETag of C9A5A6878D97B48CC965C1E41859F034-14. In this
            // case, C9A5A6878D97B48CC965C1E41859F034 is the MD5 digest of all the digests
            // concatenated together. The -14 indicates that there are 14 parts associated with
            // this object's multipart upload.
            //
            // If you've enabled additional checksum values for your multipart object, Amazon
            // S3 calculates the checksum for each individual part by using the specified
            // checksum algorithm. The checksum for the completed object is calculated in the
            // same way that Amazon S3 calculates the MD5 digest for the multipart upload. You
            // can use this checksum to verify the integrity of the object.
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

            // hash the checksums of the parts with the additional checksum algorithm
            let mut hasher = additional_checksum.clone().hasher();

            // concatenate the checksums of the parts
            for part in self.parts.values() {
                if let Some(checksum) = &part.checksum {
                    let checksum_in_bytes =
                        Base64::decode_vec(&checksum.checksum).map_err(|e| {
                            anyhow::anyhow!(
                                "could not decode checksum: {}, {}",
                                checksum.checksum,
                                e
                            )
                        })?;
                    hasher.update(&checksum_in_bytes);
                }
            }

            // base64 encode the checksum
            let checksum = Base64::encode_string(&hasher.finalize());

            // populate the headers map
            if let Some(map) = self.headers.as_mut() {
                map.insert(
                    amz_header.to_string(),
                    format!("{checksum}-{}", self.parts.len()),
                );
            } else {
                let mut map = BTreeMap::new();
                map.insert(
                    amz_header.to_string(),
                    format!("{checksum}-{}", self.parts.len()),
                );

                self.headers = Some(map);
            }
        }

        // sign the request
        let (url, headers) = &self.sign(s3, digest.as_ref(), None, Some(body.len()))?;

        let response =
            request::upload(url.clone(), self.http_method()?, headers, Bytes::from(body)).await?;

        if response.status().is_success() {
            let rs: CompleteMultipartUploadResult = from_str(&response.text().await?)?;
            Ok(rs)
        } else {
            Err(anyhow!(response_error(response).await?))
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
impl<'a> Action for CompleteMultipartUpload<'a> {
    fn http_method(&self) -> Result<Method> {
        Ok(Method::from_bytes(b"POST")?)
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
        checksum::{Checksum, ChecksumAlgorithm},
        Credentials, Region, S3,
    };

    #[test]
    fn test_method() {
        let parts: BTreeMap<u16, Part> = BTreeMap::new();
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        assert_eq!(Method::POST, action.http_method().unwrap());
    }

    #[test]
    fn test_query_pairs() {
        let parts: BTreeMap<u16, Part> = BTreeMap::new();
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let mut map = BTreeMap::new();
        map.insert("uploadId", "uid");
        assert_eq!(Some(map), action.query_pairs());
    }

    #[test]
    fn test_path() {
        let parts: BTreeMap<u16, Part> = BTreeMap::new();
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        assert_eq!(Some(vec!["key"]), action.path());
    }

    #[test]
    fn test_headers() {
        let parts: BTreeMap<u16, Part> = BTreeMap::new();
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        assert_eq!(None, action.headers());
    }

    #[test]
    fn test_serialize() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: None,
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload><Part><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_serialize_with_checksum_sha256() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: Some(Checksum {
                    algorithm: ChecksumAlgorithm::Sha256,
                    checksum: "checksum".to_string(),
                }),
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload><Part><ChecksumSHA256>checksum</ChecksumSHA256><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_serialize_with_checksum_crc32() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: Some(Checksum {
                    algorithm: ChecksumAlgorithm::Crc32,
                    checksum: "checksum".to_string(),
                }),
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload><Part><ChecksumCRC32>checksum</ChecksumCRC32><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_serialize_with_checksum_crc32c() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: Some(Checksum {
                    algorithm: ChecksumAlgorithm::Crc32c,
                    checksum: "checksum".to_string(),
                }),
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload><Part><ChecksumCRC32C>checksum</ChecksumCRC32C><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_serialize_with_checksum_sha1() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: Some(Checksum {
                    algorithm: ChecksumAlgorithm::Sha1,
                    checksum: "checksum".to_string(),
                }),
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload><Part><ChecksumSHA1>checksum</ChecksumSHA1><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#;
        assert_eq!(expected, serialized);
    }

    #[tokio::test]
    async fn test_complete_multipart_upload() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
        );

        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        parts.insert(
            1,
            Part {
                etag: "etag".to_string(),
                number: 1,
                checksum: Some(Checksum {
                    algorithm: ChecksumAlgorithm::Sha256,
                    checksum: "checksum".to_string(),
                }),
            },
        );
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let (url, headers) = action
            .sign(&s3, tools::sha256_digest("").as_ref(), None, None)
            .unwrap();
        assert_eq!(
            "https://s3.us-west-1.amazonaws.com/awsexamplebucket1/key?uploadId=uid",
            url.as_str()
        );
        assert!(headers
            .get("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE"));
    }
}
