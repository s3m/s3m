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

#[derive(Deserialize, Debug, Clone)]
/// Owner information for the object
pub struct Owner {
    #[serde(rename = "DisplayName")]
    /// Object owner's name.
    pub display_name: Option<String>,
    #[serde(rename = "ID")]
    /// Object owner's ID.
    pub id: String,
}

#[derive(Deserialize, Debug, Clone)]
/// An individual object in a `ListBucketResult`
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
    pub size: i32,
}

#[derive(Deserialize, Debug, Clone)]
/// The parsed result of a s3 bucket listing
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

#[derive(Deserialize, Debug, Clone)]
/// `CommonPrefix` is used to group keys
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    /// Keys that begin with the indicated prefix.
    pub prefix: String,
}
