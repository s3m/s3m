//! Amazon S3 multipart upload limits
//! Maximum object size 5 TB
//! Maximum number of parts per upload  10,000
//! <https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html>

use crate::s3::error::Error;
use crate::{
    s3::actions::{Action, api_error, response_error, upload_response_headers},
    s3::responses::{CompleteMultipartUploadResult, ErrorResponse},
    s3::{S3, checksum::Checksum, request, tools},
};
use base64ct::{Base64, Encoding};
use bytes::Bytes;
use quick_xml::{de::from_str, se::to_string};
use reqwest::Method;
use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct CompleteMultipartUpload<'a> {
    key: &'a str,
    upload_id: &'a str,
    parts: BTreeMap<u16, Part>,
    additional_checksum: Option<Checksum>,
    headers: Option<BTreeMap<String, String>>,
}

impl Serialize for CompleteMultipartUpload<'_> {
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

#[derive(Debug, Clone)]
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
    pub const fn new(
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

    /// The additional checksum of the whole object, if one was requested: the checksum
    /// of the parts' checksums, followed by `-<number of parts>`.
    ///
    /// For example, consider an object uploaded with a multipart upload that has an
    /// `ETag` of `C9A5A6878D97B48CC965C1E41859F034-14`. In this case,
    /// `C9A5A6878D97B48CC965C1E41859F034` is the MD5 digest of all the digests
    /// concatenated together. The `-14` indicates that there are 14 parts associated
    /// with this object's multipart upload.
    ///
    /// If you've enabled additional checksum values for your multipart object, Amazon
    /// S3 calculates the checksum for each individual part by using the specified
    /// checksum algorithm. The checksum for the completed object is calculated in the
    /// same way that Amazon S3 calculates the MD5 digest for the multipart upload.
    /// <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>
    ///
    /// # Errors
    ///
    /// Will return `Err` if a part's checksum is not valid base64
    pub fn checksum(&self) -> Result<Option<Checksum>, Error> {
        let Some(additional_checksum) = &self.additional_checksum else {
            return Ok(None);
        };

        // hash the checksums of the parts with the additional checksum algorithm
        let mut hasher = additional_checksum.hasher();

        // concatenate the checksums of the parts
        for part in self.parts.values() {
            if let Some(checksum) = &part.checksum {
                let checksum_in_bytes = Base64::decode_vec(&checksum.checksum).map_err(|e| {
                    Error::Other(format!(
                        "could not decode checksum: {}, {}",
                        checksum.checksum, e
                    ))
                })?;
                hasher.update(&checksum_in_bytes);
            }
        }

        Ok(Some(Checksum {
            algorithm: additional_checksum.algorithm.clone(),
            checksum: format!(
                "{}-{}",
                Base64::encode_string(&hasher.finalize()),
                self.parts.len()
            ),
        }))
    }

    /// # Errors
    ///
    /// Will return `Err` if can not make the request
    // Explicit `Result<T, Error>`, not the `s3::error::Result` alias: importing
    // that alias would shadow `std::result::Result` and break `#[derive(Serialize)]`
    // in this file.
    pub async fn request(mut self, s3: &S3) -> Result<CompleteMultipartUploadResult, Error> {
        let body = to_string(&self)?;

        let digest = tools::sha256_digest(&body);

        // add the checksum of the whole object to the headers if requested
        if let Some(checksum) = self.checksum()? {
            self.headers
                .get_or_insert_with(BTreeMap::new)
                .insert(checksum.algorithm.as_amz().to_string(), checksum.checksum);
        }

        // sign the request
        let (url, headers) = &self.sign(s3, digest.as_ref(), None, Some(body.len()))?;

        let response = request::upload(
            s3.client(),
            url.clone(),
            self.http_method()?,
            headers,
            Bytes::from(body),
        )
        .await?;

        if response.status().is_success() {
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.text().await?;

            // S3 can answer 200 OK and still fail the upload, with an <Error> document
            // in the body (the response is only sent once processing finished).
            if from_str::<ErrorResponse>(&body).is_ok() {
                return Err(api_error(status, &headers, Some(body)));
            }

            let mut rs: CompleteMultipartUploadResult = from_str(&body)?;
            rs.headers = upload_response_headers(&headers)?;
            Ok(rs)
        } else {
            Err(response_error(response).await)
        }
    }
}

// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html>
impl Action for CompleteMultipartUpload<'_> {
    fn http_method(&self) -> Result<Method, Error> {
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
        Some(self.key.split('/').collect())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps,
    clippy::format_collect
)]
mod tests {
    use super::*;
    use crate::s3::{
        Credentials, Region, S3,
        checksum::{Checksum, ChecksumAlgorithm},
    };
    use secrecy::SecretString;

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
    fn test_serialize_1() {
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
        let expected = r"<CompleteMultipartUpload><Part><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>";
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_serialize_2() {
        let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
        for i in 1..=3 {
            parts.insert(
                i,
                Part {
                    etag: format!("etag{i}"),
                    number: i,
                    checksum: None,
                },
            );
        }
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        let serialized = to_string(&action).unwrap();
        let expected = r"<CompleteMultipartUpload><Part><ETag>etag1</ETag><PartNumber>1</PartNumber></Part><Part><ETag>etag2</ETag><PartNumber>2</PartNumber></Part><Part><ETag>etag3</ETag><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>";
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
        let expected = r"<CompleteMultipartUpload><Part><ChecksumSHA256>checksum</ChecksumSHA256><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>";
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
        let expected = r"<CompleteMultipartUpload><Part><ChecksumCRC32>checksum</ChecksumCRC32><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>";
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
        let expected = r"<CompleteMultipartUpload><Part><ChecksumCRC32C>checksum</ChecksumCRC32C><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>";
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
        let expected = r"<CompleteMultipartUpload><Part><ChecksumSHA1>checksum</ChecksumSHA1><ETag>etag</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>";
        assert_eq!(expected, serialized);
    }

    fn sha256_parts(checksums: &[&str]) -> BTreeMap<u16, Part> {
        (1..)
            .zip(checksums)
            .map(|(number, checksum)| {
                (
                    number,
                    Part {
                        etag: format!("etag{number}"),
                        number,
                        checksum: Some(Checksum {
                            algorithm: ChecksumAlgorithm::Sha256,
                            checksum: (*checksum).to_string(),
                        }),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_checksum_is_composite_of_parts() {
        // base64(sha256("part1")), base64(sha256("part2")) and
        // base64(sha256(sha256("part1") || sha256("part2"))), computed with Python hashlib.
        let parts = sha256_parts(&[
            "WeCjzHoMRflnNb8fBjJduQtLt0LyV98StrGN89ISZJk=",
            "R008mgtSt2i3xUr/ssOGqSGmszAHEJxwvntTbroXa7Q=",
        ]);
        let action = CompleteMultipartUpload::new(
            "key",
            "uid",
            parts,
            Some(Checksum::new(ChecksumAlgorithm::Sha256)),
        );
        let checksum = action.checksum().unwrap().unwrap();
        assert_eq!(ChecksumAlgorithm::Sha256, checksum.algorithm);
        assert_eq!(
            "xheoOGWlJSrr1KooTDWywen6ft/oubM2FCHlVKEDMfM=-2",
            checksum.checksum
        );
        assert_eq!(
            "sha256 (base64): xheoOGWlJSrr1KooTDWywen6ft/oubM2FCHlVKEDMfM=-2",
            checksum.report()
        );
    }

    #[test]
    fn test_checksum_without_additional_checksum() {
        let parts = sha256_parts(&["WeCjzHoMRflnNb8fBjJduQtLt0LyV98StrGN89ISZJk="]);
        let action = CompleteMultipartUpload::new("key", "uid", parts, None);
        assert!(action.checksum().unwrap().is_none());
    }

    #[test]
    fn test_checksum_rejects_invalid_part_checksum() {
        let parts = sha256_parts(&["not base64!"]);
        let action = CompleteMultipartUpload::new(
            "key",
            "uid",
            parts,
            Some(Checksum::new(ChecksumAlgorithm::Sha256)),
        );
        assert!(action.checksum().is_err());
    }

    async fn complete_against(
        server: &mockito::ServerGuard,
    ) -> Result<CompleteMultipartUploadResult, Error> {
        let s3 = S3::new(
            &Credentials::new("AKIA", &SecretString::new("secret".into())),
            &Region::custom("us-east-1", server.url()),
            Some("bucket".to_string()),
            false,
        );
        let parts = BTreeMap::from([(
            1,
            Part {
                etag: "\"etag1\"".to_string(),
                number: 1,
                checksum: None,
            },
        )]);
        CompleteMultipartUpload::new("key", "uid", parts, None)
            .request(&s3)
            .await
    }

    const DONE: &str = "<CompleteMultipartUploadResult><Location>l</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>\"done-1\"</ETag></CompleteMultipartUploadResult>";

    // <https://github.com/s3m/s3m/issues/75>: the response headers of
    // CompleteMultipartUpload are reported like those of PutObject.
    #[tokio::test]
    async fn test_request_reads_response_headers() {
        let mut server = mockito::Server::new_async().await;
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(mockito::Matcher::UrlEncoded(
                "uploadId".into(),
                "uid".into(),
            ))
            .with_header("x-amz-version-id", "v1")
            .with_header(
                "x-amz-expiration",
                "expiry-date=\"Sat, 09 Jan 2027 00:00:00 GMT\", rule-id=\"expire-45d\"",
            )
            .with_header("x-amz-server-side-encryption", "AES256")
            .with_body(DONE)
            .create_async()
            .await;

        let rs = complete_against(&server).await.unwrap();
        complete.assert_async().await;
        assert_eq!("\"done-1\"", rs.e_tag);
        assert_eq!(
            BTreeMap::from([
                (
                    "Expiration",
                    "expiry-date=\"Sat, 09 Jan 2027 00:00:00 GMT\", rule-id=\"expire-45d\""
                        .to_string()
                ),
                ("Server-side encryption", "AES256".to_string()),
                ("Version ID", "v1".to_string()),
            ]),
            rs.headers
        );
    }

    #[tokio::test]
    async fn test_request_without_response_headers() {
        let mut server = mockito::Server::new_async().await;
        let _complete = server
            .mock("POST", "/bucket/key")
            .match_query(mockito::Matcher::Any)
            .with_body(DONE)
            .create_async()
            .await;

        let rs = complete_against(&server).await.unwrap();
        assert!(rs.headers.is_empty());
    }

    // S3 can answer CompleteMultipartUpload with 200 OK and an <Error> document.
    #[tokio::test]
    async fn test_request_reports_error_document_with_200() {
        let mut server = mockito::Server::new_async().await;
        let _complete = server
            .mock("POST", "/bucket/key")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("x-amz-request-id", "req-1")
            .with_body("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><RequestId>req-1</RequestId></Error>")
            .create_async()
            .await;

        let Err(Error::Api(error)) = complete_against(&server).await else {
            panic!("expected an S3 API error");
        };
        assert_eq!(200, error.status);
        assert_eq!(Some("InternalError".to_string()), error.code);
        assert_eq!(
            Some("We encountered an internal error. Please try again.".to_string()),
            error.message
        );
        assert!(
            error.details.contains("Request ID: req-1"),
            "{}",
            error.details
        );
    }

    #[tokio::test]
    async fn test_request_reports_error_document_without_message() {
        let mut server = mockito::Server::new_async().await;
        let _complete = server
            .mock("POST", "/bucket/key")
            .match_query(mockito::Matcher::Any)
            .with_body("<Error><Code>InternalError</Code><RequestId>r1</RequestId></Error>")
            .create_async()
            .await;

        let Err(Error::Api(error)) = complete_against(&server).await else {
            panic!("expected an S3 API error");
        };
        assert_eq!(200, error.status);
        assert_eq!(Some("InternalError".to_string()), error.code);
    }

    #[tokio::test]
    async fn test_complete_multipart_upload() {
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
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

        assert!(
            headers
                .get("authorization")
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE")
        );
    }
}
