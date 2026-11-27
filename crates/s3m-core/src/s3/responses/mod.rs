use serde::Deserialize;
use serde::de::{Deserializer, Error};
use std::collections::BTreeMap;

/// # Errors
///
/// Will return `Err` if can't deserialize
pub fn bool_deserializer<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    match &s[..] {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(D::Error::custom(format!(
            "got {other}, but expected `true` or `false`"
        ))),
    }
}

/// Owner information for the object
#[derive(Deserialize, Debug, Clone, Default)]
pub struct Owner {
    #[serde(rename = "DisplayName")]
    /// Object owner's name.
    pub display_name: Option<String>,
    #[serde(rename = "ID", default)]
    /// Object owner's ID. Empty when the server leaves it out.
    pub id: String,
}

/// Initiator - Identifies who initiated the multipart upload.
#[derive(Deserialize, Debug, Clone)]
pub struct Initiator {
    #[serde(rename = "DisplayName")]
    pub display_name: Option<String>,
    #[serde(rename = "ID", default)]
    pub id: String,
}

/// An individual object in a `ListBucketResult`
#[derive(Deserialize, Debug, Clone)]
pub struct Object {
    #[serde(rename = "LastModified")]
    /// Date and time the object was last modified.
    pub last_modified: String,
    #[serde(rename = "ETag", default)]
    /// The entity tag is an MD5 hash of the object. The `ETag` only reflects changes to the
    /// contents of an object, not its metadata. Empty when the server leaves it out.
    pub e_tag: String,
    #[serde(rename = "StorageClass", default)]
    /// STANDARD | `STANDARD_IA` | `REDUCED_REDUNDANCY` | GLACIER. Empty when the server
    /// leaves it out.
    pub storage_class: String,
    #[serde(rename = "Key")]
    /// The object's key
    pub key: String,
    #[serde(rename = "Owner")]
    /// Bucket owner
    pub owner: Option<Owner>,
    #[serde(rename = "Size")]
    /// Size in bytes of the object.
    pub size: u64,
}

/// The parsed result of a s3 bucket listing
#[derive(Deserialize, Debug, Clone)]
pub struct ListBucketResult {
    #[serde(rename = "Name", default)]
    /// Name of the bucket. Empty when the server leaves it out.
    pub name: String,
    #[serde(rename = "NextMarker")]
    /// When the response is truncated (that is, the `IsTruncated` element value in the response
    /// is true), you can use the key name in this field as a marker in the subsequent request
    /// to get next set of objects. Amazon S3 lists objects in UTF-8 character encoding in
    /// lexicographical order.
    pub next_marker: Option<String>,
    #[serde(rename = "Delimiter")]
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys", default)]
    /// Sets the maximum number of keys returned in the response body. 0 when the
    /// server leaves it out.
    pub max_keys: i32,
    #[serde(rename = "Prefix", default)]
    /// Limits the response to keys that begin with the specified prefix. Empty when
    /// the response has no `<Prefix>`: Cloudflare R2 leaves the element out when the
    /// request has no prefix.
    pub prefix: String,
    #[serde(rename = "Marker")]
    /// Indicates where in the bucket listing begins. Marker is included in the response if
    /// it was sent with the request.
    pub marker: Option<String>,
    #[serde(rename = "EncodingType")]
    /// Specifies the encoding method to used
    pub encoding_type: Option<String>,
    #[serde(rename = "IsTruncated", deserialize_with = "bool_deserializer")]
    ///  Specifies whether (true) or not (false) all of the results were returned.
    ///  If the number of results exceeds that specified by `MaxKeys`, all of the results
    ///  might not be returned.
    pub is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    /// Metadata about each object returned.
    pub contents: Vec<Object>,
    #[serde(rename = "CommonPrefixes", default)]
    /// All of the keys rolled up into a common prefix count as a single return when
    /// calculating the number of returns.
    pub common_prefixes: Option<Vec<CommonPrefix>>,
}

/// `CommonPrefix` is used to group keys
#[derive(Deserialize, Debug, Clone)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    /// Keys that begin with the indicated prefix.
    pub prefix: String,
}

/// List of Buckets
#[derive(Deserialize, Debug, Clone)]
pub struct Buckets {
    #[serde(rename = "Bucket", default)]
    pub bucket: Vec<Bucket>,
}

/// An individual Bucket
#[derive(Deserialize, Debug, Clone)]
pub struct Bucket {
    /// Bucket Name
    #[serde(rename = "Name")]
    pub name: String,
    /// Bucket creation date (timestamp)
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
    #[serde(rename = "ServerSideEncryptionEnabled")]
    pub server_side_encryption_enabled: Option<bool>,
}

/// The parsed result of `ListBuckets`
#[derive(Deserialize, Debug, Clone)]
pub struct ListAllMyBucketsResult {
    /// Bucket owner
    #[serde(rename = "Owner")]
    pub owner: Option<Owner>,
    /// list of Buckets
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
    /// Opaque token for the next `ListBuckets` page; absent on the last page.
    #[serde(rename = "ContinuationToken", default)]
    pub continuation_token: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    #[serde(default)]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: Option<String>,
    #[serde(rename = "RequestId")]
    #[serde(default)]
    pub request_id: String,
}

/// Parse an S3 answer whose document element must be `root`.
///
/// The deserializer doesn't look at the name of the document element, so any XML
/// with the fields a struct needs would pass for it. That matters for the answers
/// that confirm something was stored: another document with an `<ETag>` must not
/// be taken for the completion of a multipart upload.
///
/// # Errors
///
/// Will return `Err` if the document element is not `root`, or the document can't
/// be parsed into `T`
pub(crate) fn from_xml_root<T: serde::de::DeserializeOwned>(
    body: &str,
    root: &str,
) -> std::result::Result<T, quick_xml::DeError> {
    use quick_xml::events::Event;

    // The name of every top-level element: a document has exactly one.
    let mut reader = quick_xml::Reader::from_str(body);
    let mut top_level = Vec::new();
    let mut depth = 0_usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                if depth == 0 {
                    top_level.push(element.local_name().as_ref().to_string());
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) if depth == 0 => {
                top_level.push(element.local_name().as_ref().to_string());
            }
            Ok(Event::End(_)) => depth = depth.saturating_sub(1),
            Ok(Event::Eof) => break,
            // Text outside the document element: not a document (an error message
            // appended by something in between, for example).
            Ok(Event::Text(text)) if depth == 0 => {
                let text: &str = text.as_ref();
                if !text
                    .trim_matches(|c: char| c.is_whitespace() || c == '\u{feff}')
                    .is_empty()
                {
                    return Err(quick_xml::DeError::Custom(format!(
                        "expected a <{root}> document, got text outside it"
                    )));
                }
            }
            Ok(Event::CData(_)) if depth == 0 => {
                return Err(quick_xml::DeError::Custom(format!(
                    "expected a <{root}> document, got text outside it"
                )));
            }
            Ok(_) => {}
            Err(error) => return Err(quick_xml::DeError::Custom(error.to_string())),
        }
    }
    match top_level.as_slice() {
        [found] if found == root => quick_xml::de::from_str(body),
        [found] => Err(quick_xml::DeError::Custom(format!(
            "expected a <{root}> document, got <{found}>"
        ))),
        [] => Err(quick_xml::DeError::Custom(format!(
            "expected a <{root}> document, got no element"
        ))),
        found => Err(quick_xml::DeError::Custom(format!(
            "expected one <{root}> document, got {}: <{}>",
            found.len(),
            found.join(">, <")
        ))),
    }
}

#[derive(Deserialize, Debug)]
pub struct InitiateMultipartUploadResult {
    #[serde(rename = "Bucket", default)]
    pub bucket: String,
    #[serde(rename = "Key", default)]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

#[derive(Deserialize, Debug)]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "Location", default)]
    pub location: String,
    #[serde(rename = "Bucket", default)]
    pub bucket: String,
    #[serde(rename = "Key", default)]
    pub key: String,
    #[serde(rename = "ETag")]
    pub e_tag: String,
    #[serde(rename = "ChecksumCRC32")]
    pub checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C")]
    pub checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumSHA1")]
    pub checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256")]
    pub checksum_sha256: Option<String>,
    /// Response headers worth reporting, labelled as s3m prints them, like the
    /// map `PutObject::request` returns: `Version ID`, `Expiration`,
    /// `Server-side encryption`, `Previous object size`.
    #[serde(skip)]
    pub headers: BTreeMap<&'static str, String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename = "DeleteResult")]
pub struct DeleteObjectsResult {
    #[serde(rename = "Deleted", default)]
    pub deleted: Vec<DeletedObject>,
    #[serde(rename = "Error", default)]
    pub errors: Vec<DeleteError>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct DeletedObject {
    pub key: String,
    #[serde(default)]
    pub version_id: Option<String>,
    /// `true` when S3 created a delete marker instead of removing data
    /// (key delete without a version id in a versioned bucket).
    #[serde(default)]
    pub delete_marker: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteError {
    pub key: String,
    #[serde(default)]
    pub version_id: Option<String>,
    pub code: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket", default)]
    pub bucket: String,
    #[serde(rename = "KeyMarker")]
    pub key_marker: Option<String>,
    #[serde(rename = "UploadIdMarker")]
    pub upload_id_marker: Option<String>,
    #[serde(rename = "NextKeyMarker")]
    pub next_key_marker: Option<String>,
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
    #[serde(rename = "Delimiter")]
    pub delimiter: Option<String>,
    #[serde(rename = "NextUploadIdMarker")]
    pub next_upload_id_marker: Option<String>,
    #[serde(rename = "MaxUploads", default)]
    pub max_uploads: usize,
    #[serde(rename = "IsTruncated", deserialize_with = "bool_deserializer")]
    pub is_truncated: bool,
    #[serde(rename = "Upload", default)]
    pub upload: Option<Vec<Upload>>,
    #[serde(rename = "CommonPrefixes", default)]
    pub common_prefixes: Option<Vec<CommonPrefix>>,
    #[serde(rename = "EncodingType")]
    pub encoding_type: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Upload {
    #[serde(rename = "Initiated")]
    pub initiated: String,
    #[serde(rename = "Initiator")]
    pub initiator: Option<Initiator>,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Owner")]
    pub owner: Option<Owner>,
    #[serde(rename = "StorageClass", default)]
    pub storage_class: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// `AccessControlPolicy`
#[derive(Deserialize, Debug)]
pub struct AccessControlPolicy {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "AccessControlList")]
    pub acl: AccessControlList,
}

/// `AccessControlList`
#[derive(Deserialize, Debug, Clone)]
pub struct AccessControlList {
    #[serde(rename = "Grant")]
    pub grant: Vec<Grant>,
}

/// `Grant`
#[derive(Deserialize, Debug, Clone)]
pub struct Grant {
    #[serde(rename = "Grantee")]
    pub grantee: Grantee,
    #[serde(rename = "Permission")]
    pub permission: String,
}

/// `Grantee`
#[derive(Deserialize, Debug, Clone)]
pub struct Grantee {
    #[serde(rename = "DisplayName")]
    pub display_name: Option<String>,
    #[serde(rename = "EmailAddress")]
    pub email_address: Option<String>,
    #[serde(rename = "ID")]
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub xsi_type: Option<String>,
    #[serde(rename = "URI")]
    pub uri: Option<String>,
}

/// `ListVersionsResult`
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListVersionsResult {
    #[serde(default)]
    pub name: String,
    pub prefix: Option<String>,
    #[serde(default)]
    pub key_marker: String,
    pub version_id_marker: Option<String>,
    /// Where the next page starts, when `is_truncated`.
    #[serde(default)]
    pub next_key_marker: Option<String>,
    /// With `next_key_marker`, the version the next page starts after.
    #[serde(default)]
    pub next_version_id_marker: Option<String>,
    #[serde(default)]
    pub max_keys: i32,
    pub is_truncated: bool,
    #[serde(rename = "Version", default)]
    pub versions: Vec<Version>,
    // Include these if you expect delete markers in other responses
    #[serde(rename = "DeleteMarker", default)]
    pub delete_markers: Vec<DeleteMarker>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Version {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,

    #[serde(rename = "LastModified")]
    pub last_modified: String,

    #[serde(default)]
    pub e_tag: String,

    #[serde(default)]
    pub checksum_algorithm: Option<String>,

    #[serde(default)]
    pub checksum_type: Option<String>,

    pub size: u64,
    #[serde(default)]
    pub owner: Owner,
    #[serde(default)]
    pub storage_class: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMarker {
    pub key: String,
    pub version_id: String,
    #[serde(default)]
    pub is_latest: bool,
    #[serde(rename = "LastModified", default)]
    pub last_modified: String,
    #[serde(default)]
    pub owner: Owner,
}

/// Bucket-level Object Lock configuration (`GetObjectLockConfiguration`).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectLockConfiguration {
    #[serde(rename = "ObjectLockEnabled")]
    pub object_lock_enabled: Option<String>,
    #[serde(rename = "Rule")]
    pub rule: Option<ObjectLockRule>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectLockRule {
    #[serde(rename = "DefaultRetention")]
    pub default_retention: Option<DefaultRetention>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DefaultRetention {
    #[serde(rename = "Mode")]
    pub mode: Option<String>,
    #[serde(rename = "Days")]
    pub days: Option<u32>,
    #[serde(rename = "Years")]
    pub years: Option<u32>,
}

/// Per-object retention (`GetObjectRetention`).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectRetention {
    #[serde(rename = "Mode")]
    pub mode: Option<String>,
    #[serde(rename = "RetainUntilDate")]
    pub retain_until_date: Option<String>,
}

/// Per-object legal hold (`GetObjectLegalHold`).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectLegalHold {
    #[serde(rename = "Status")]
    pub status: Option<String>,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestStruct {
        #[serde(deserialize_with = "bool_deserializer")]
        value: bool,
    }

    #[test]
    fn test_bool_deserializer_true() {
        let yaml = "value: \"true\"";
        let result: TestStruct = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(result, TestStruct { value: true });
    }

    #[test]
    fn test_bool_deserializer_false() {
        let yaml = "value: \"false\"";
        let result: TestStruct = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(result, TestStruct { value: false });
    }

    #[test]
    fn test_bool_deserializer_invalid() {
        let yaml = "value: \"yes\"";
        let result: Result<TestStruct, _> = serde_saphyr::from_str(yaml);
        assert!(result.is_err());
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("got yes, but expected `true` or `false`"),
                "unexpected error message: {msg}"
            );
        }
    }

    #[test]
    fn test_list_bucket_result_deserialization() {
        let yaml = r#"
Name: test-bucket
Prefix: test/
Marker: null
Delimiter: "/"
MaxKeys: 1000
EncodingType: null
IsTruncated: "false"
Contents: []
CommonPrefixes: []
NextContinuationToken: null
"#;
        let parsed: ListBucketResult = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(parsed.name, "test-bucket");
        assert!(!parsed.is_truncated);
        assert_eq!(parsed.max_keys, 1000);
        assert_eq!(parsed.contents.len(), 0);
    }

    // <https://github.com/s3m/s3m/issues/78>: Cloudflare R2 leaves out <Prefix> when
    // the request has none (AWS S3 sends an empty element). These are R2's responses
    // for a bucket with one object and for an empty bucket.
    #[test]
    fn test_list_bucket_result_without_prefix() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>s3m-test</Name><Contents><Key>probe78/a.txt</Key><Size>6</Size><LastModified>2026-11-26T23:37:12.345Z</LastModified><ETag>"b1946ac92492d2347c6235b8d2611184"</ETag><StorageClass>STANDARD</StorageClass></Contents><IsTruncated>false</IsTruncated><MaxKeys>1000</MaxKeys><KeyCount>1</KeyCount></ListBucketResult>"#;
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.name, "s3m-test");
        assert_eq!(parsed.prefix, "");
        assert_eq!(parsed.contents.len(), 1);
        assert_eq!(parsed.contents[0].key, "probe78/a.txt");

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>s3m-test</Name><IsTruncated>false</IsTruncated><MaxKeys>1000</MaxKeys><KeyCount>0</KeyCount></ListBucketResult>"#;
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.prefix, "");
        assert!(parsed.contents.is_empty());
    }

    #[test]
    fn test_list_multipart_uploads_result_deserialization() {
        let yaml = r#"
Bucket: example-bucket
KeyMarker: null
UploadIdMarker: null
NextKeyMarker: null
Prefix: null
Delimiter: "/"
NextUploadIdMarker: null
MaxUploads: 2
IsTruncated: "true"
Upload: []
CommonPrefixes: []
EncodingType: null
"#;
        let parsed: ListMultipartUploadsResult = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(parsed.bucket, "example-bucket");
        assert!(parsed.is_truncated);
        assert_eq!(parsed.max_uploads, 2);
    }

    #[test]
    fn test_list_versions_result_deserialization() {
        let yaml = r"
Name: versioned-bucket
Prefix: null
KeyMarker: start
VersionIdMarker: null
MaxKeys: 100
IsTruncated: false
Version: []
DeleteMarker: []
";
        let parsed: ListVersionsResult = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(parsed.name, "versioned-bucket");
        assert_eq!(parsed.key_marker, "start");
        assert!(!parsed.is_truncated);
        assert_eq!(parsed.versions.len(), 0);
        assert_eq!(parsed.delete_markers.len(), 0);
    }

    #[test]
    fn test_error_response_without_request_id() {
        let xml = r"<Error><Code>InvalidRequest</Code><Message>bad request</Message></Error>";
        let parsed: ErrorResponse = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.code, "InvalidRequest");
        assert_eq!(parsed.message, "bad request");
        assert_eq!(parsed.request_id, "");
    }

    #[test]
    fn test_list_buckets_empty_buckets_element() {
        let xml = r"<ListAllMyBucketsResult><Owner><ID>owner</ID></Owner><Buckets/></ListAllMyBucketsResult>";
        let parsed: ListAllMyBucketsResult = quick_xml::de::from_str(xml).unwrap();

        assert!(parsed.buckets.bucket.is_empty());
    }

    #[test]
    fn test_list_buckets_continuation_token() {
        let xml = r"<ListAllMyBucketsResult><Buckets><Bucket><Name>b1</Name><CreationDate>2026-03-14T00:00:00.000Z</CreationDate></Bucket></Buckets><ContinuationToken>a+b/c==</ContinuationToken></ListAllMyBucketsResult>";
        let parsed: ListAllMyBucketsResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.buckets.bucket.len(), 1);
        assert_eq!(parsed.continuation_token.as_deref(), Some("a+b/c=="));
    }

    #[test]
    fn test_list_buckets_without_buckets_element_is_error() {
        let xml = r"<ListAllMyBucketsResult><Owner><ID>owner</ID></Owner></ListAllMyBucketsResult>";

        assert!(quick_xml::de::from_str::<ListAllMyBucketsResult>(xml).is_err());
    }

    // <https://github.com/s3m/s3m/issues/91>: AWS S3 lists versions and delete markers
    // in key order, so the two kinds of elements alternate in one page (the shape of
    // the examples in the ListObjectVersions reference). That failed with "duplicate
    // field `Version`".
    #[test]
    fn test_list_versions_with_interleaved_delete_markers() {
        let version = |key: &str, id: &str| {
            format!(
                "<Version><Key>{key}</Key><VersionId>{id}</VersionId><IsLatest>false</IsLatest><LastModified>2026-11-27T00:00:00.000Z</LastModified><ETag>e</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Version>"
            )
        };
        let marker = |key: &str, id: &str| {
            format!(
                "<DeleteMarker><Key>{key}</Key><VersionId>{id}</VersionId><IsLatest>true</IsLatest><LastModified>2026-11-27T00:00:00.000Z</LastModified></DeleteMarker>"
            )
        };
        let xml = format!(
            "<ListVersionsResult><Name>bucket</Name><IsTruncated>false</IsTruncated>{}{}{}{}{}</ListVersionsResult>",
            version("a", "a1"),
            marker("b", "b2"),
            version("b", "b1"),
            marker("c", "c2"),
            version("c", "c1"),
        );

        let parsed: ListVersionsResult = quick_xml::de::from_str(&xml).unwrap();

        let ids = |entries: Vec<&str>| entries.join(",");
        assert_eq!(
            ids(parsed
                .versions
                .iter()
                .map(|v| v.version_id.as_str())
                .collect()),
            "a1,b1,c1"
        );
        assert_eq!(
            ids(parsed
                .delete_markers
                .iter()
                .map(|m| m.version_id.as_str())
                .collect()),
            "b2,c2"
        );
    }

    // #91: the same for the answer to DeleteObjects, where deleted keys and errors can
    // alternate.
    #[test]
    fn test_delete_result_with_interleaved_errors() {
        let xml = r"<DeleteResult><Deleted><Key>a</Key></Deleted><Error><Key>b</Key><Code>AccessDenied</Code><Message>no</Message></Error><Deleted><Key>c</Key></Deleted><Error><Key>d</Key><Code>AccessDenied</Code><Message>no</Message></Error></DeleteResult>";
        let parsed: DeleteObjectsResult = quick_xml::de::from_str(xml).unwrap();

        let deleted: Vec<&str> = parsed.deleted.iter().map(|d| d.key.as_str()).collect();
        let errors: Vec<&str> = parsed.errors.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(deleted, ["a", "c"]);
        assert_eq!(errors, ["b", "d"]);
    }

    // <https://github.com/s3m/s3m/issues/90>: a truncated page says where the next one
    // starts.
    #[test]
    fn test_list_versions_next_markers() {
        let xml = r"<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>key</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker></ListVersionsResult>";
        let parsed: ListVersionsResult = quick_xml::de::from_str(xml).unwrap();
        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_key_marker.as_deref(), Some("key"));
        assert_eq!(parsed.next_version_id_marker.as_deref(), Some("v2"));

        let xml = r"<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>";
        let parsed: ListVersionsResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.next_key_marker, None);
        assert_eq!(parsed.next_version_id_marker, None);
    }

    #[test]
    fn test_list_versions_without_owner() {
        let xml = r"<ListVersionsResult><Name>bucket</Name><Prefix></Prefix><KeyMarker></KeyMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Version><Key>key</Key><VersionId>version</VersionId><IsLatest>true</IsLatest><LastModified>2026-09-19T00:00:00Z</LastModified><ETag>etag</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Version><DeleteMarker><Key>deleted</Key><VersionId>marker</VersionId><IsLatest>false</IsLatest><LastModified>2026-09-19T00:00:00Z</LastModified></DeleteMarker></ListVersionsResult>";
        let parsed: ListVersionsResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.versions.len(), 1);
        assert_eq!(parsed.versions[0].owner.id, "");
        assert_eq!(parsed.delete_markers.len(), 1);
        assert_eq!(parsed.delete_markers[0].owner.id, "");
    }

    // <https://github.com/s3m/s3m/issues/81>: elements the S3 API reference marks as
    // not required, and that s3m doesn't need to list, delete or page, may be left out
    // by a server. Each test below leaves out every such element of one response.
    #[test]
    fn test_list_bucket_result_without_optional_elements() {
        let xml = r"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2026-11-27T00:00:00.000Z</LastModified><Size>3</Size><Owner><DisplayName>alice</DisplayName></Owner></Contents><Contents><Key>b.txt</Key><LastModified>2026-11-27T00:00:00.000Z</LastModified><Size>4</Size><Owner/></Contents></ListBucketResult>";
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.name, "");
        assert_eq!(parsed.max_keys, 0);
        assert_eq!(parsed.contents.len(), 2);
        let object = &parsed.contents[0];
        assert_eq!(object.key, "a.txt");
        assert_eq!(object.size, 3);
        assert_eq!(object.e_tag, "");
        assert_eq!(object.storage_class, "");
        let owner = object.owner.as_ref().unwrap();
        assert_eq!(owner.display_name.as_deref(), Some("alice"));
        assert_eq!(owner.id, "");
        assert_eq!(parsed.contents[1].owner.as_ref().unwrap().id, "");
    }

    #[test]
    fn test_list_multipart_uploads_result_without_optional_elements() {
        let xml = r"<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>a.bin</Key><UploadId>upload-1</UploadId><Initiated>2026-11-27T00:00:00.000Z</Initiated><Initiator><DisplayName>alice</DisplayName></Initiator><Owner><DisplayName>alice</DisplayName></Owner></Upload></ListMultipartUploadsResult>";
        let parsed: ListMultipartUploadsResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.bucket, "");
        assert_eq!(parsed.max_uploads, 0);
        let uploads = parsed.upload.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key, "a.bin");
        assert_eq!(uploads[0].upload_id, "upload-1");
        assert_eq!(uploads[0].storage_class, "");
        assert_eq!(uploads[0].initiator.as_ref().unwrap().id, "");
        assert_eq!(uploads[0].owner.as_ref().unwrap().id, "");
    }

    #[test]
    fn test_list_versions_result_without_optional_elements() {
        let xml = r"<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>a.txt</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><LastModified>2026-11-27T00:00:00.000Z</LastModified><Size>3</Size><Owner><DisplayName>alice</DisplayName></Owner></Version><DeleteMarker><Key>b.txt</Key><VersionId>v2</VersionId><Owner/></DeleteMarker></ListVersionsResult>";
        let parsed: ListVersionsResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.name, "");
        assert_eq!(parsed.key_marker, "");
        assert_eq!(parsed.max_keys, 0);
        assert_eq!(parsed.versions.len(), 1);
        let version = &parsed.versions[0];
        assert_eq!(version.key, "a.txt");
        assert_eq!(version.version_id, "v1");
        assert_eq!(version.size, 3);
        assert_eq!(version.e_tag, "");
        assert_eq!(version.storage_class, "");
        assert_eq!(version.owner.display_name.as_deref(), Some("alice"));
        assert_eq!(version.owner.id, "");
        assert_eq!(parsed.delete_markers.len(), 1);
        let marker = &parsed.delete_markers[0];
        assert_eq!(marker.key, "b.txt");
        assert_eq!(marker.version_id, "v2");
        assert!(!marker.is_latest);
        assert_eq!(marker.last_modified, "");
        assert_eq!(marker.owner.id, "");
    }

    // #81 review: with the echoed elements optional, any document with an <ETag> parsed
    // as a completed upload. The document element is checked for the two answers that
    // confirm a multipart upload: its start and its completion.
    #[test]
    fn test_from_xml_root_rejects_another_document() {
        let complete = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>"etag-3"</ETag></CompleteMultipartUploadResult>"#;
        let parsed: CompleteMultipartUploadResult =
            from_xml_root(complete, "CompleteMultipartUploadResult").unwrap();
        assert_eq!(parsed.e_tag, "\"etag-3\"");

        let copy = r#"<CopyObjectResult><ETag>"other"</ETag><LastModified>2026-11-27T00:00:00.000Z</LastModified></CopyObjectResult>"#;
        let error =
            from_xml_root::<CompleteMultipartUploadResult>(copy, "CompleteMultipartUploadResult")
                .unwrap_err()
                .to_string();
        assert!(
            error.contains(
                "expected a <CompleteMultipartUploadResult> document, got <CopyObjectResult>"
            ),
            "{error}"
        );

        let error = from_xml_root::<InitiateMultipartUploadResult>(
            "<ListPartsResult><UploadId>upload-1</UploadId></ListPartsResult>",
            "InitiateMultipartUploadResult",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("got <ListPartsResult>"), "{error}");

        // A second document after the expected one (an <Error> here) is not ignored.
        let error = from_xml_root::<CompleteMultipartUploadResult>(
            &format!("{complete}<Error><Code>InternalError</Code></Error>"),
            "CompleteMultipartUploadResult",
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("got 2: <CompleteMultipartUploadResult>, <Error>"),
            "{error}"
        );
        // Text after the document is not ignored either.
        for trailing in ["upstream failure", "<![CDATA[x]]>"] {
            let error = from_xml_root::<CompleteMultipartUploadResult>(
                &format!("{complete}{trailing}"),
                "CompleteMultipartUploadResult",
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("got text outside it"), "{trailing}: {error}");
        }
        // Comments and processing instructions around the document are fine.
        let parsed: CompleteMultipartUploadResult = from_xml_root(
            &format!("\u{feff}<!-- a -->\n{complete}\n<!-- b -->\n"),
            "CompleteMultipartUploadResult",
        )
        .unwrap();
        assert_eq!(parsed.e_tag, "\"etag-3\"");

        for body in ["", "not xml", "<?xml version=\"1.0\"?>"] {
            assert!(
                from_xml_root::<InitiateMultipartUploadResult>(
                    body,
                    "InitiateMultipartUploadResult"
                )
                .is_err(),
                "{body}"
            );
        }
    }

    // #81: s3m reads only the upload id of an initiated upload, and the ETag (and the
    // checksums) of a completed one; the bucket, key and location are echoed back.
    #[test]
    fn test_multipart_results_without_echoed_elements() {
        let xml = r"<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>";
        let parsed: InitiateMultipartUploadResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.upload_id, "upload-1");
        assert_eq!(parsed.bucket, "");
        assert_eq!(parsed.key, "");

        let xml = r#"<CompleteMultipartUploadResult><ETag>"etag-3"</ETag></CompleteMultipartUploadResult>"#;
        let parsed: CompleteMultipartUploadResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.e_tag, "\"etag-3\"");
        assert_eq!(parsed.location, "");
        assert_eq!(parsed.bucket, "");
        assert_eq!(parsed.key, "");

        // The upload id and the ETag are what s3m needs from them.
        let error = quick_xml::de::from_str::<InitiateMultipartUploadResult>(
            r"<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key></InitiateMultipartUploadResult>",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing field `UploadId`"), "{error}");
        let error = quick_xml::de::from_str::<CompleteMultipartUploadResult>(
            r"<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key></CompleteMultipartUploadResult>",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing field `ETag`"), "{error}");
    }

    #[test]
    fn test_delete_error_without_message() {
        let xml = r"<DeleteResult><Error><Key>a.txt</Key><Code>AccessDenied</Code></Error></DeleteResult>";
        let parsed: DeleteObjectsResult = quick_xml::de::from_str(xml).unwrap();

        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].code, "AccessDenied");
        assert_eq!(parsed.errors[0].message, "");
    }

    // What s3m needs stays required: without IsTruncated a listing could end early
    // without notice, and an entry without its key, size or date can't be used.
    #[test]
    fn test_listing_elements_that_stay_required() {
        let missing = |xml: &str, field: &str| {
            let error = quick_xml::de::from_str::<ListBucketResult>(xml)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("missing field `{field}`")),
                "{error}"
            );
        };
        missing(
            r"<ListBucketResult><Name>bucket</Name></ListBucketResult>",
            "IsTruncated",
        );
        missing(
            r"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><LastModified>2026-11-27T00:00:00.000Z</LastModified><Size>3</Size></Contents></ListBucketResult>",
            "Key",
        );
        missing(
            r"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2026-11-27T00:00:00.000Z</LastModified></Contents></ListBucketResult>",
            "Size",
        );
        missing(
            r"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><Size>3</Size></Contents></ListBucketResult>",
            "LastModified",
        );

        let error = quick_xml::de::from_str::<ListVersionsResult>(
            r"<ListVersionsResult><Name>bucket</Name></ListVersionsResult>",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing field `IsTruncated`"), "{error}");
        let error = quick_xml::de::from_str::<ListMultipartUploadsResult>(
            r"<ListMultipartUploadsResult><Bucket>bucket</Bucket></ListMultipartUploadsResult>",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing field `IsTruncated`"), "{error}");
    }
}
