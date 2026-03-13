#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

use mockito::Server;
use s3m::{
    cli::globals::GlobalArgs,
    s3::{
        Credentials, Region, S3,
        actions::{
            AbortMultipartUpload, CreateBucket, CreateMultipartUpload, DeleteBucket, DeleteObject,
            GetObject, GetObjectAcl, GetObjectAttributes, HeadObject, ListBuckets,
            ListMultipartUploads, ListObjectVersions, ListObjectsV2, PutObjectAcl,
        },
    },
};
use secrecy::SecretString;

fn test_s3(endpoint: String, bucket: Option<&str>) -> S3 {
    let credentials = Credentials::new(
        "minioadmin",
        &SecretString::new("minioadmin".to_string().into()),
    );
    let region = Region::custom("us-east-1", endpoint);

    S3::new(&credentials, &region, bucket.map(str::to_string), false)
}

#[tokio::test]
async fn test_delete_actions_request_success_and_error() {
    let mut server = Server::new_async().await;
    let _abort = server
        .mock("DELETE", "/bucket/key")
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
    let _abort_error = server
        .mock("DELETE", "/bucket/missing")
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
    let abort_error = AbortMultipartUpload::new("missing", "upload-2")
        .request(&s3)
        .await
        .unwrap_err()
        .to_string();

    assert!(abort.is_empty());
    assert!(delete_bucket.is_empty());
    assert!(delete_object.is_empty());
    assert!(abort_error.contains("HTTP Status Code: 404 Not Found"));
    assert!(abort_error.contains("Code: NoSuchUpload"));
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
        .with_status(200)
        .with_header("ETag", "\"etag-value\"")
        .create_async()
        .await;
    let _get_acl = server
        .mock("GET", "/bucket/key")
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
async fn test_create_multipart_upload_request_success() {
    let mut server = Server::new_async().await;
    let _create_multipart = server
        .mock("POST", "/bucket/key")
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
