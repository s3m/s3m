//! This target is an external consumer of both crates' preserved listing APIs.
use s3m::cli::{actions::Action, age_filter::AgeFilter};
use s3m_core::s3::actions::{ListBuckets, ListMultipartUploads, ListObjectsV2};

#[test]
fn legacy_listing_constructors_and_action_field_types_compile() {
    let _: fn(Option<String>) -> ListBuckets = ListBuckets::new;
    let _: fn(Option<String>) -> ListMultipartUploads = ListMultipartUploads::new;
    let _: fn(Option<String>, Option<String>, Option<String>) -> ListObjectsV2 = ListObjectsV2::new;
    let _ = ListBuckets::new(None);
    let _ = ListMultipartUploads::new(None);
    let _ = ListObjectsV2::new(None, None, None);
    let bucket: Option<String> = Some("bucket".into());
    let json: bool = true;
    let list_multipart_uploads: bool = false;
    let max_kub: Option<String> = Some("3".into());
    let older_than: Option<AgeFilter> = None;
    let prefix: Option<String> = Some("prefix/".into());
    let start_after: Option<String> = Some("prefix/start".into());
    let action = Action::ListObjects {
        bucket,
        json,
        list_multipart_uploads,
        max_kub,
        older_than,
        prefix,
        start_after,
    };
    let Action::ListObjects {
        bucket,
        json,
        list_multipart_uploads,
        max_kub,
        older_than,
        prefix,
        start_after,
    } = action
    else {
        unreachable!("constructed ListObjects above");
    };
    let _: Option<String> = bucket;
    let _: bool = json;
    let _: bool = list_multipart_uploads;
    let _: Option<String> = max_kub;
    let _: Option<AgeFilter> = older_than;
    let _: Option<String> = prefix;
    let _: Option<String> = start_after;
}
