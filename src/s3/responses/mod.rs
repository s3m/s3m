use serde::de::{Deserializer, Error};
use serde::Deserialize;

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
            "got {}, but expected `true` or `false`",
            other
        ))),
    }
}

/// Owner information for the object
#[derive(Deserialize, Debug, Clone)]
pub struct Owner {
    #[serde(rename = "DisplayName")]
    /// Object owner's name.
    pub display_name: Option<String>,
    #[serde(rename = "ID")]
    /// Object owner's ID.
    pub id: String,
}

/// Initiator - Identifies who initiated the multipart upload.
#[derive(Deserialize, Debug, Clone)]
pub struct Initiator {
    #[serde(rename = "DisplayName")]
    pub display_name: Option<String>,
    #[serde(rename = "ID")]
    pub id: String,
}

/// An individual object in a `ListBucketResult`
#[derive(Deserialize, Debug, Clone)]
pub struct Object {
    #[serde(rename = "LastModified")]
    /// Date and time the object was last modified.
    pub last_modified: String,
    #[serde(rename = "ETag")]
    /// The entity tag is an MD5 hash of the object. The ETag only reflects changes to the
    /// contents of an object, not its metadata.
    pub e_tag: String,
    #[serde(rename = "StorageClass")]
    /// STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER
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
    #[serde(rename = "Name")]
    /// Name of the bucket.
    pub name: String,
    #[serde(rename = "NextMarker")]
    /// When the response is truncated (that is, the IsTruncated element value in the response
    /// is true), you can use the key name in this field as a marker in the subsequent request
    /// to get next set of objects. Amazon S3 lists objects in UTF-8 character encoding in
    /// lexicographical order.
    pub next_marker: Option<String>,
    #[serde(rename = "Delimiter")]
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    /// Sets the maximum number of keys returned in the response body.
    pub max_keys: i32,
    #[serde(rename = "Prefix")]
    /// Limits the response to keys that begin with the specified prefix.
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
    ///  If the number of results exceeds that specified by MaxKeys, all of the results
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
    #[serde(rename = "Bucket")]
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
}

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: Option<String>,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}

#[derive(Deserialize, Debug)]
pub struct InitiateMultipartUploadResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

#[derive(Deserialize, Debug)]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "Location")]
    pub location: String,
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

#[derive(Deserialize, Debug)]
pub struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket")]
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
    #[serde(rename = "MaxUploads")]
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
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// AccessControlPolicy
#[derive(Deserialize, Debug)]
pub struct AccessControlPolicy {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "AccessControlList")]
    pub acl: AccessControlList,
}

/// AccessControlList
#[derive(Deserialize, Debug, Clone)]
pub struct AccessControlList {
    #[serde(rename = "Grant")]
    pub grant: Grant,
}

/// Grant
#[derive(Deserialize, Debug, Clone)]
pub struct Grant {
    #[serde(rename = "Grantee")]
    pub grantee: Grantee,
    #[serde(rename = "Permission")]
    pub permission: String,
}

/// Grantee
#[derive(Deserialize, Debug, Clone)]
pub struct Grantee {
    #[serde(rename = "DisplayName")]
    pub display_name: String,
    #[serde(rename = "EmailAddress")]
    pub email_address: Option<String>,
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "type")]
    pub xsi_type: Option<String>,
    #[serde(rename = "URI")]
    pub uri: Option<String>,
}
