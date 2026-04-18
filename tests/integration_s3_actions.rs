#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

use chrono::Utc;
use mockito::{Matcher, Server};
use s3m::{
    cli::{
        actions::{Action as CliAction, DeleteGroup, DuGroupBy, object_delete, object_du},
        globals::GlobalArgs,
    },
    s3::{
        Credentials, Region, S3,
        actions::{
            AbortMultipartUpload, CreateBucket, CreateMultipartUpload, DeleteBucket, DeleteObject,
            DeleteObjects, GetObject, GetObjectAcl, GetObjectAttributes, HeadObject, ListBuckets,
            ListMultipartUploads, ListObjectVersions, ListObjectsV2, ObjectIdentifier,
            PutObjectAcl,
        },
    },
};
use secrecy::SecretString;
use std::fmt::Write as _;

fn test_s3(endpoint: String, bucket: Option<&str>) -> S3 {
    let credentials = Credentials::new(
        "minioadmin",
        &SecretString::new("minioadmin".to_string().into()),
    );
    let region = Region::custom("us-east-1", endpoint);

    S3::new(&credentials, &region, bucket.map(str::to_string), false)
}

fn list_objects_xml(keys: &[String], is_truncated: bool) -> String {
    let mut contents = String::new();
    for key in keys {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>2025-03-13T00:00:00.000Z</LastModified><ETag>\"etag\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn list_objects_page_xml(
    entries: &[(&str, u64)],
    prefix: &str,
    is_truncated: bool,
    next_continuation_token: Option<&str>,
) -> String {
    let mut contents = String::new();
    for (key, size) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>2025-03-13T00:00:00.000Z</LastModified><ETag>\"etag\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    let next = next_continuation_token.map_or_else(String::new, |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{next}{contents}</ListBucketResult>"#
    )
}

fn list_objects_grouped_page_xml(
    entries: &[(&str, u64, &str)],
    prefix: &str,
    is_truncated: bool,
    next_continuation_token: Option<&str>,
) -> String {
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    let next = next_continuation_token.map_or_else(String::new, |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{next}{contents}</ListBucketResult>"#
    )
}

fn delete_objects_body(keys: &[String]) -> String {
    let mut objects = String::new();
    for key in keys {
        let _ = write!(objects, "<Object><Key>{key}</Key></Object>");
    }

    format!(
        r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{objects}<Quiet>true</Quiet></Delete>"#
    )
}

fn delete_result_xml(keys: &[String]) -> String {
    let mut deleted = String::new();
    for key in keys {
        let _ = write!(deleted, "<Deleted><Key>{key}</Key></Deleted>");
    }

    format!(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult>{deleted}</DeleteResult>"#)
}

fn delete_group(endpoint: &str, bucket: &str, keys: &[String]) -> DeleteGroup {
    DeleteGroup {
        objects: keys
            .iter()
            .map(|key| ObjectIdentifier {
                key: key.clone(),
                version_id: None,
            })
            .collect(),
        s3: test_s3(endpoint.to_string(), Some(bucket)),
    }
}

#[tokio::test]
async fn test_delete_actions_request_success_and_error() {
    let mut server = Server::new_async().await;
    let _abort = server
        .mock("DELETE", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "upload-1".into()))
        .with_status(200)
        .with_body("")
        .create_async()
        .await;
    let _delete_bucket = server
        .mock("DELETE", "/bucket")
        .with_status(200)
        .with_body("")
        .create_async()
        .await;
    let _delete_object = server
        .mock("DELETE", "/bucket/old-key")
        .with_status(200)
        .with_body("")
        .create_async()
        .await;
    let _delete_objects = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(
            r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>bulk-a</Key></Object><Object><Key>bulk-b</Key><VersionId>v2</VersionId></Object><Quiet>true</Quiet></Delete>"#.to_string(),
        ))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Deleted>
    <Key>bulk-a</Key>
  </Deleted>
  <Deleted>
    <Key>bulk-b</Key>
    <VersionId>v2</VersionId>
  </Deleted>
</DeleteResult>"#,
        )
        .create_async()
        .await;
    let _abort_error = server
        .mock("DELETE", "/bucket/missing")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "upload-2".into()))
        .with_status(404)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchUpload</Code>
  <Message>Upload not found</Message>
  <RequestId>123</RequestId>
</Error>"#,
        )
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    let abort = AbortMultipartUpload::new("key", "upload-1")
        .request(&s3)
        .await
        .unwrap();
    let delete_bucket = DeleteBucket::new().request(&s3).await.unwrap();
    let delete_object = DeleteObject::new("old-key").request(&s3).await.unwrap();
    let delete_objects = DeleteObjects::new(
        vec![
            ObjectIdentifier {
                key: "bulk-a".to_string(),
                version_id: None,
            },
            ObjectIdentifier {
                key: "bulk-b".to_string(),
                version_id: Some("v2".to_string()),
            },
        ],
        true,
    )
    .request(&s3)
    .await
    .unwrap();
    let abort_error = AbortMultipartUpload::new("missing", "upload-2")
        .request(&s3)
        .await
        .unwrap_err()
        .to_string();

    assert!(abort.is_empty());
    assert!(delete_bucket.is_empty());
    assert!(delete_object.is_empty());
    assert_eq!(delete_objects.deleted.len(), 2);
    assert!(delete_objects.errors.is_empty());
    assert!(abort_error.contains("HTTP Status Code: 404 Not Found"));
    assert!(abort_error.contains("Code: NoSuchUpload"));
}

#[tokio::test]
async fn test_multi_rm_single_object_uses_delete_object() {
    let mut server = Server::new_async().await;
    let delete_object = server
        .mock("DELETE", "/bucket/one.txt")
        .with_status(200)
        .with_body("")
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let keys = vec!["one.txt".to_string()];

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "one.txt".to_string(),
            older_than: None,
            recursive: false,
            targets: vec![delete_group(&server.url(), "bucket", &keys)],
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    delete_object.assert_async().await;
}

#[tokio::test]
async fn test_multi_rm_multiple_objects_use_delete_objects() {
    let mut server = Server::new_async().await;
    let keys = vec!["one.txt".to_string(), "two.txt".to_string()];
    let delete_objects = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&keys)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&keys))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "one.txt".to_string(),
            older_than: None,
            recursive: false,
            targets: vec![delete_group(&server.url(), "bucket", &keys)],
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    delete_objects.assert_async().await;
}

#[tokio::test]
async fn test_multi_rm_batches_delete_objects_requests_over_1000_keys() {
    let mut server = Server::new_async().await;
    let first_batch: Vec<String> = (0..1_000).map(|index| format!("bulk-{index:04}")).collect();
    let second_batch = vec!["bulk-1000".to_string()];

    let delete_1 = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&first_batch)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&first_batch))
        .expect(1)
        .create_async()
        .await;

    let delete_2 = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&second_batch)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&second_batch))
        .expect(1)
        .create_async()
        .await;

    let all_keys = first_batch
        .iter()
        .cloned()
        .chain(second_batch.iter().cloned())
        .collect::<Vec<_>>();
    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: all_keys[0].clone(),
            older_than: None,
            recursive: false,
            targets: vec![delete_group(&server.url(), "bucket", &all_keys)],
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    delete_1.assert_async().await;
    delete_2.assert_async().await;
}

#[tokio::test]
async fn test_multi_rm_groups_by_bucket_and_uses_delete_objects_per_group() {
    let mut server = Server::new_async().await;
    let bucket_a = vec!["a.txt".to_string()];
    let bucket_b = vec!["b.txt".to_string()];

    let delete_a = server
        .mock("POST", "/bucket-a")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&bucket_a)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&bucket_a))
        .expect(1)
        .create_async()
        .await;

    let delete_b = server
        .mock("POST", "/bucket-b")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&bucket_b)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&bucket_b))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket-a"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "a.txt".to_string(),
            older_than: None,
            recursive: false,
            targets: vec![
                delete_group(&server.url(), "bucket-a", &bucket_a),
                delete_group(&server.url(), "bucket-b", &bucket_b),
            ],
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    delete_a.assert_async().await;
    delete_b.assert_async().await;
}

#[tokio::test]
async fn test_multi_rm_surfaces_partial_delete_errors() {
    let mut server = Server::new_async().await;
    let keys = vec!["one.txt".to_string(), "two.txt".to_string()];
    let delete_objects = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&keys)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Deleted>
    <Key>one.txt</Key>
  </Deleted>
  <Error>
    <Key>two.txt</Key>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
</DeleteResult>"#,
        )
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    let err = object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "one.txt".to_string(),
            older_than: None,
            recursive: false,
            targets: vec![delete_group(&server.url(), "bucket", &keys)],
            upload_id: String::new(),
        },
    )
    .await
    .unwrap_err()
    .to_string();

    delete_objects.assert_async().await;
    assert!(err.contains("DeleteObjects returned 1 object error(s):"));
    assert!(err.contains("two.txt: Access Denied (AccessDenied)"));
}

#[tokio::test]
async fn test_rm_older_than_deletes_only_matching_single_object() {
    let mut server = Server::new_async().await;
    let now = Utc::now().to_rfc3339();
    let list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[
                ("logs/old.txt", 1, "2025-01-01T00:00:00.000Z"),
                ("logs/new.txt", 1, &now),
            ],
            "logs/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;
    let delete = server
        .mock("DELETE", "/bucket/logs/old.txt")
        .with_status(200)
        .with_body("")
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "logs/".to_string(),
            older_than: Some(s3m::cli::age_filter::parse_age_filter("30d").unwrap()),
            recursive: false,
            targets: Vec::new(),
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    list.assert_async().await;
    delete.assert_async().await;
}

#[tokio::test]
async fn test_rm_older_than_uses_delete_objects_for_multiple_matches_across_pages() {
    let mut server = Server::new_async().await;
    let now = Utc::now().to_rfc3339();
    let matched = vec!["logs/old-a.txt".to_string(), "logs/old-b.txt".to_string()];
    let list_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[
                ("logs/old-a.txt", 1, "2025-01-01T00:00:00.000Z"),
                ("logs/new.txt", 1, &now),
            ],
            "logs/",
            true,
            Some("page-2"),
        ))
        .expect(1)
        .create_async()
        .await;
    let list_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("continuation-token".into(), "page-2".into()),
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[("logs/old-b.txt", 1, "2025-02-01T00:00:00.000Z")],
            "logs/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;
    let delete = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&matched)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&matched))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "logs/".to_string(),
            older_than: Some(s3m::cli::age_filter::parse_age_filter("30d").unwrap()),
            recursive: false,
            targets: Vec::new(),
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    list_1.assert_async().await;
    list_2.assert_async().await;
    delete.assert_async().await;
}

#[tokio::test]
async fn test_rm_older_than_no_matches_is_ok() {
    let mut server = Server::new_async().await;
    let now = Utc::now().to_rfc3339();
    let list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[("logs/new.txt", 1, &now)],
            "logs/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: false,
            key: "logs/".to_string(),
            older_than: Some(s3m::cli::age_filter::parse_age_filter("30d").unwrap()),
            recursive: false,
            targets: Vec::new(),
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    list.assert_async().await;
}

#[tokio::test]
async fn test_recursive_bucket_delete_batches_and_deletes_bucket() {
    let mut server = Server::new_async().await;

    let first_batch: Vec<String> = (0..1_000).map(|index| format!("bulk-{index:04}")).collect();
    let second_batch = vec!["bulk-1000".to_string()];

    let list_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&first_batch, true))
        .expect(1)
        .create_async()
        .await;

    let list_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&second_batch, false))
        .expect(1)
        .create_async()
        .await;

    let list_3 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[], false))
        .expect(1)
        .create_async()
        .await;

    let delete_1 = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&first_batch)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&first_batch))
        .expect(1)
        .create_async()
        .await;

    let delete_2 = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&second_batch)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(delete_result_xml(&second_batch))
        .expect(1)
        .create_async()
        .await;

    let delete_bucket = server
        .mock("DELETE", "/bucket")
        .with_status(200)
        .with_body("")
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: true,
            key: String::new(),
            older_than: None,
            recursive: true,
            targets: Vec::new(),
            upload_id: String::new(),
        },
    )
    .await
    .unwrap();

    list_1.assert_async().await;
    list_2.assert_async().await;
    list_3.assert_async().await;
    delete_1.assert_async().await;
    delete_2.assert_async().await;
    delete_bucket.assert_async().await;
}

#[tokio::test]
async fn test_recursive_bucket_delete_stops_on_partial_delete_error() {
    let mut server = Server::new_async().await;
    let keys = vec!["denied-key".to_string()];

    let list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&keys, false))
        .expect(1)
        .create_async()
        .await;

    let delete = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_body(Matcher::Exact(delete_objects_body(&keys)))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Error>
    <Key>denied-key</Key>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
</DeleteResult>"#,
        )
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    let err = object_delete::handle(
        &s3,
        CliAction::DeleteObject {
            bucket: true,
            key: String::new(),
            older_than: None,
            recursive: true,
            targets: Vec::new(),
            upload_id: String::new(),
        },
    )
    .await
    .unwrap_err()
    .to_string();

    list.assert_async().await;
    delete.assert_async().await;

    assert!(err.contains("DeleteObjects returned 1 object error(s):"));
    assert!(err.contains("denied-key: Access Denied (AccessDenied)"));
}

#[tokio::test]
async fn test_bucket_and_acl_actions_request_success() {
    let mut server = Server::new_async().await;
    let _create_bucket = server
        .mock("PUT", "/bucket")
        .with_status(200)
        .with_header("location", "/bucket")
        .create_async()
        .await;
    let _put_acl = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("acl".into(), String::new()))
        .with_status(200)
        .with_header("ETag", "\"etag-value\"")
        .create_async()
        .await;
    let _get_acl = server
        .mock("GET", "/bucket/key")
        .match_query(Matcher::UrlEncoded("acl".into(), String::new()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body("<AccessControlPolicy/>")
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    let created = CreateBucket::new("private").request(&s3).await.unwrap();
    let acl_set = PutObjectAcl::new("key", "private")
        .request(&s3)
        .await
        .unwrap();
    let acl_get = GetObjectAcl::new("key").request(&s3).await.unwrap();

    assert_eq!(created.get("location"), Some(&"/bucket".to_string()));
    assert_eq!(acl_set.get("ETag"), Some(&"\"etag-value\"".to_string()));
    assert_eq!(acl_get.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn test_object_metadata_actions_request_success() {
    let mut server = Server::new_async().await;
    let _attributes = server
        .mock("GET", "/bucket/key")
        .match_query(Matcher::UrlEncoded("attributes".into(), String::new()))
        .with_status(200)
        .with_body("<GetObjectAttributesOutput/>")
        .create_async()
        .await;
    let _head = server
        .mock("HEAD", "/bucket/key")
        .with_status(200)
        .with_header("etag", "\"head-etag\"")
        .with_header("content-length", "4")
        .create_async()
        .await;
    let _get = server
        .mock("GET", "/bucket/payload")
        .with_status(200)
        .with_body("data")
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let globals = GlobalArgs::new();

    let attributes = GetObjectAttributes::new("key").request(&s3).await.unwrap();
    let head = HeadObject::new("key", None).request(&s3).await.unwrap();
    let get = GetObject::new("payload", None)
        .request(&s3, &globals)
        .await
        .unwrap();

    assert_eq!(attributes.status(), reqwest::StatusCode::OK);
    assert_eq!(head.get("etag"), Some(&"\"head-etag\"".to_string()));
    assert_eq!(get.text().await.unwrap(), "data");
}

#[tokio::test]
async fn test_listing_actions_request_success() {
    let mut server = Server::new_async().await;
    let _list_buckets = server
        .mock("GET", "/")
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Buckets>
    <Bucket>
      <Name>bucket-one</Name>
      <CreationDate>2025-03-13T00:00:00.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>"#,
        )
        .create_async()
        .await;
    let _list_uploads = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult>
  <Bucket>bucket</Bucket>
  <MaxUploads>1</MaxUploads>
  <IsTruncated>false</IsTruncated>
</ListMultipartUploadsResult>"#,
        )
        .create_async()
        .await;
    let _list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bucket</Name>
  <Prefix></Prefix>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#,
        )
        .create_async()
        .await;
    let _list_versions = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("prefix".into(), "prefix".into()),
            Matcher::UrlEncoded("versions".into(), String::new()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult>
  <Name>bucket</Name>
  <KeyMarker>start</KeyMarker>
  <MaxKeys>100</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListVersionsResult>"#,
        )
        .create_async()
        .await;

    let bucket_s3 = test_s3(server.url(), Some("bucket"));
    let root_s3 = test_s3(server.url(), None);

    let buckets = ListBuckets::new(None).request(&root_s3).await.unwrap();
    let uploads = ListMultipartUploads::new(None)
        .request(&bucket_s3)
        .await
        .unwrap();
    let objects = ListObjectsV2::new(None, None, None)
        .request(&bucket_s3)
        .await
        .unwrap();
    let versions = ListObjectVersions::new("prefix")
        .request(&bucket_s3)
        .await
        .unwrap();

    assert_eq!(buckets.buckets.bucket.len(), 1);
    assert_eq!(buckets.buckets.bucket[0].name, "bucket-one");
    assert_eq!(uploads.bucket, "bucket");
    assert_eq!(objects.name, "bucket");
    assert_eq!(versions.name, "bucket");
}

#[tokio::test]
async fn test_du_summary_one_page() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_page_xml(
            &[("a.txt", 10), ("b.txt", 25)],
            "",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let summary = object_du::summarize(&s3, None).await.unwrap();

    list_objects.assert_async().await;
    assert_eq!(summary.objects, 2);
    assert_eq!(summary.bytes, 35);
}

#[tokio::test]
async fn test_du_summary_multi_page() {
    let mut server = Server::new_async().await;
    let page_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_page_xml(
            &[("a.txt", 10), ("b.txt", 20)],
            "",
            true,
            Some("page-2"),
        ))
        .expect(1)
        .create_async()
        .await;
    let page_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("continuation-token".into(), "page-2".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_page_xml(&[("c.txt", 30)], "", false, None))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let summary = object_du::summarize(&s3, None).await.unwrap();

    page_1.assert_async().await;
    page_2.assert_async().await;
    assert_eq!(summary.objects, 3);
    assert_eq!(summary.bytes, 60);
}

#[tokio::test]
async fn test_du_summary_empty_bucket() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_page_xml(&[], "", false, None))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let summary = object_du::summarize(&s3, None).await.unwrap();

    list_objects.assert_async().await;
    assert_eq!(summary.objects, 0);
    assert_eq!(summary.bytes, 0);
}

#[tokio::test]
async fn test_du_summary_prefix_target() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/2026/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_page_xml(
            &[("logs/2026/01.txt", 5), ("logs/2026/02.txt", 7)],
            "logs/2026/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let summary = object_du::summarize(&s3, Some("logs/2026/".to_string()))
        .await
        .unwrap();

    list_objects.assert_async().await;
    assert_eq!(summary.objects, 2);
    assert_eq!(summary.bytes, 12);
}

#[tokio::test]
async fn test_du_grouped_one_page_same_day() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[
                ("logs/a.txt", 10, "2026-03-13T10:00:00.000Z"),
                ("logs/b.txt", 20, "2026-03-13T11:00:00.000Z"),
            ],
            "logs/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let report = object_du::summarize_report(&s3, Some("logs/".to_string()), Some(DuGroupBy::Day))
        .await
        .unwrap();

    list_objects.assert_async().await;
    match report {
        object_du::UsageReport::ByDay { days, total } => {
            assert_eq!(days.len(), 1);
            assert_eq!(days[0].day.to_string(), "2026-03-13");
            assert_eq!(days[0].summary.objects, 2);
            assert_eq!(days[0].summary.bytes, 30);
            assert_eq!(total.objects, 2);
            assert_eq!(total.bytes, 30);
        }
        object_du::UsageReport::Total(_) => panic!("expected grouped report"),
    }
}

#[tokio::test]
async fn test_du_grouped_multi_page_sorted_order() {
    let mut server = Server::new_async().await;
    let page_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[("b.txt", 20, "2026-03-14T11:00:00.000Z")],
            "",
            true,
            Some("page-2"),
        ))
        .expect(1)
        .create_async()
        .await;
    let page_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("continuation-token".into(), "page-2".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[("a.txt", 10, "2026-03-13T10:00:00.000Z")],
            "",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let report = object_du::summarize_report(&s3, None, Some(DuGroupBy::Day))
        .await
        .unwrap();

    page_1.assert_async().await;
    page_2.assert_async().await;
    match report {
        object_du::UsageReport::ByDay { days, total } => {
            assert_eq!(days.len(), 2);
            assert_eq!(days[0].day.to_string(), "2026-03-13");
            assert_eq!(days[1].day.to_string(), "2026-03-14");
            assert_eq!(total.objects, 2);
            assert_eq!(total.bytes, 30);
        }
        object_du::UsageReport::Total(_) => panic!("expected grouped report"),
    }
}

#[tokio::test]
async fn test_du_grouped_prefix_target() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "reports/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(
            &[("reports/a.csv", 5, "2026-03-15T00:10:00.000Z")],
            "reports/",
            false,
            None,
        ))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let report =
        object_du::summarize_report(&s3, Some("reports/".to_string()), Some(DuGroupBy::Day))
            .await
            .unwrap();

    list_objects.assert_async().await;
    match report {
        object_du::UsageReport::ByDay { days, total } => {
            assert_eq!(days.len(), 1);
            assert_eq!(days[0].day.to_string(), "2026-03-15");
            assert_eq!(total.objects, 1);
            assert_eq!(total.bytes, 5);
        }
        object_du::UsageReport::Total(_) => panic!("expected grouped report"),
    }
}

#[tokio::test]
async fn test_du_grouped_empty_result() {
    let mut server = Server::new_async().await;
    let list_objects = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "empty/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_grouped_page_xml(&[], "empty/", false, None))
        .expect(1)
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));
    let report = object_du::summarize_report(&s3, Some("empty/".to_string()), Some(DuGroupBy::Day))
        .await
        .unwrap();

    list_objects.assert_async().await;
    match report {
        object_du::UsageReport::ByDay { days, total } => {
            assert!(days.is_empty());
            assert_eq!(total.objects, 0);
            assert_eq!(total.bytes, 0);
        }
        object_du::UsageReport::Total(_) => panic!("expected grouped report"),
    }
}

#[test]
fn test_du_default_report_unchanged_without_group_by() {
    let report = object_du::UsageReport::Total(object_du::UsageSummary {
        objects: 2,
        bytes: 2_048,
    });

    assert_eq!(
        report,
        object_du::UsageReport::Total(object_du::UsageSummary {
            objects: 2,
            bytes: 2_048,
        })
    );
}

#[tokio::test]
async fn test_create_multipart_upload_request_success() {
    let mut server = Server::new_async().await;
    let _create_multipart = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>bucket</Bucket>
  <Key>key</Key>
  <UploadId>upload-id</UploadId>
</InitiateMultipartUploadResult>"#,
        )
        .create_async()
        .await;

    let s3 = test_s3(server.url(), Some("bucket"));

    let created = CreateMultipartUpload::new("key", None, None, None)
        .request(&s3)
        .await
        .unwrap();

    assert_eq!(created.bucket, "bucket");
    assert_eq!(created.key, "key");
    assert_eq!(created.upload_id, "upload-id");
}
