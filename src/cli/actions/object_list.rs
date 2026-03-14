use crate::{
    cli::{
        actions::Action,
        age_filter::{AgeFilter, parse_last_modified},
    },
    s3::{
        S3, actions,
        responses::{Bucket, Object, Upload},
    },
};
use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use colored::Colorize;
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq)]
struct BucketsJsonOutput {
    kind: &'static str,
    buckets: Vec<BucketJsonEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct BucketJsonEntry {
    name: String,
    creation_date: String,
    server_side_encryption_enabled: Option<bool>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ObjectsJsonOutput {
    kind: &'static str,
    bucket: String,
    prefix: Option<String>,
    start_after: Option<String>,
    objects: Vec<ObjectJsonEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ObjectJsonEntry {
    bucket: String,
    key: String,
    size_bytes: u64,
    last_modified: String,
    etag: String,
    storage_class: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct MultipartUploadsJsonOutput {
    kind: &'static str,
    bucket: String,
    uploads: Vec<MultipartUploadJsonEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct MultipartUploadJsonEntry {
    key: String,
    upload_id: String,
    initiated: String,
    storage_class: String,
}

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::ListObjects {
        bucket,
        json,
        list_multipart_uploads,
        max_kub,
        older_than,
        prefix,
        start_after,
    } = action
    {
        match (bucket, list_multipart_uploads) {
            (Some(_), false) => {
                let now = Utc::now();
                if json {
                    let bucket_name = s3.bucket().unwrap_or_default().to_string();
                    let objects = collect_filtered_objects(
                        s3,
                        bucket_name.clone(),
                        prefix.clone(),
                        start_after.clone(),
                        max_kub,
                        older_than,
                        now,
                    )
                    .await?;
                    print_json(&ObjectsJsonOutput {
                        kind: "objects",
                        bucket: bucket_name,
                        prefix,
                        start_after,
                        objects,
                    })?;
                } else {
                    visit_filtered_objects(
                        s3,
                        prefix,
                        start_after,
                        max_kub,
                        older_than,
                        now,
                        |object| print_object_info(&object),
                    )
                    .await?;
                }
            }

            (Some(_), true) => {
                let action = actions::ListMultipartUploads::new(max_kub);
                let rs = action.request(s3).await?;
                if json {
                    let uploads = rs
                        .upload
                        .unwrap_or_default()
                        .into_iter()
                        .map(|upload| MultipartUploadJsonEntry {
                            key: upload.key,
                            upload_id: upload.upload_id,
                            initiated: upload.initiated,
                            storage_class: upload.storage_class,
                        })
                        .collect();
                    print_json(&MultipartUploadsJsonOutput {
                        kind: "multipart_uploads",
                        bucket: s3.bucket().unwrap_or_default().to_string(),
                        uploads,
                    })?;
                } else if let Some(uploads) = rs.upload {
                    for upload in uploads {
                        print_upload_info(&upload)?;
                    }
                }
            }

            (None, _) => {
                let action = actions::ListBuckets::new(max_kub);
                let rs = action.request(s3).await?;
                if json {
                    let buckets = rs
                        .buckets
                        .bucket
                        .into_iter()
                        .map(|bucket| BucketJsonEntry {
                            name: bucket.name,
                            creation_date: bucket.creation_date,
                            server_side_encryption_enabled: bucket.server_side_encryption_enabled,
                        })
                        .collect();
                    print_json(&BucketsJsonOutput {
                        kind: "buckets",
                        buckets,
                    })?;
                } else {
                    for bucket in rs.buckets.bucket {
                        print_bucket_info(&bucket)?;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn collect_filtered_objects(
    s3: &S3,
    bucket: String,
    prefix: Option<String>,
    start_after: Option<String>,
    max_kub: Option<String>,
    older_than: Option<AgeFilter>,
    now: DateTime<Utc>,
) -> Result<Vec<ObjectJsonEntry>> {
    let mut objects = Vec::new();
    visit_filtered_objects(
        s3,
        prefix,
        start_after,
        max_kub,
        older_than,
        now,
        |object| {
            objects.push(ObjectJsonEntry {
                bucket: bucket.clone(),
                key: object.key,
                size_bytes: object.size,
                last_modified: object.last_modified,
                etag: object.e_tag,
                storage_class: object.storage_class,
            });
            Ok(())
        },
    )
    .await?;
    Ok(objects)
}

/// Visits listed objects across all `ListObjectsV2` pages and applies the optional age filter.
///
/// Filtering is based on each object's `LastModified` timestamp interpreted in UTC.
///
/// # Errors
/// Will return an error if listing fails, if `LastModified` can not be parsed for filtered
/// objects, or if S3 returns a truncated page without a continuation token.
pub(crate) async fn visit_filtered_objects<F>(
    s3: &S3,
    prefix: Option<String>,
    start_after: Option<String>,
    max_kub: Option<String>,
    older_than: Option<AgeFilter>,
    now: DateTime<Utc>,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(Object) -> Result<()>,
{
    let mut continuation_token: Option<String> = None;
    let mut start_after = start_after;

    loop {
        let mut action =
            actions::ListObjectsV2::new(prefix.clone(), start_after.take(), max_kub.clone());
        action.continuation_token = continuation_token.clone();
        let page = action.request(s3).await?;

        for object in page.contents {
            let matches = older_than.map_or(Ok(true), |filter| filter.matches(&object, now))?;
            if matches {
                visit(object)?;
            }
        }

        if !page.is_truncated {
            break;
        }

        continuation_token.clone_from(&page.next_continuation_token);
        if continuation_token.is_none() {
            return Err(anyhow!(
                "ListObjectsV2 returned a truncated response without a continuation token"
            ));
        }
    }

    Ok(())
}

fn print_object_info(object: &Object) -> Result<()> {
    let last_modified = parse_last_modified(object)?;
    println!(
        "{} {:>10} {:<}",
        format!("[{}]", last_modified.format("%F %T %Z")).green(),
        ByteSize(object.size).to_string().yellow(),
        object.key
    );
    Ok(())
}

fn print_upload_info(upload: &Upload) -> Result<()> {
    let dt = DateTime::parse_from_rfc3339(&upload.initiated)?;
    let initiated: DateTime<Utc> = DateTime::from(dt);
    println!(
        "{} {} {}",
        format!("[{}]", initiated.format("%F %T %Z")).green(),
        upload.upload_id.yellow(),
        upload.key
    );
    Ok(())
}

fn print_bucket_info(bucket: &Bucket) -> Result<()> {
    let dt = DateTime::parse_from_rfc3339(&bucket.creation_date)?;
    let creation_date: DateTime<Utc> = DateTime::from(dt);
    println!(
        "{} {}",
        format!("[{}]", creation_date.format("%F %T %Z")).green(),
        bucket.name.yellow()
    );
    Ok(())
}

fn print_json<T>(value: &T) -> Result<()>
where
    T: Serialize,
{
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
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
    use crate::s3::{Credentials, Region};
    use chrono::TimeZone;
    use mockito::{Matcher, Server};
    use secrecy::SecretString;
    use std::fmt::Write as _;

    fn test_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &Region::custom("us-west-1", endpoint),
            Some("bucket".to_string()),
            false,
        )
    }

    fn list_objects_page_xml(
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

    #[tokio::test]
    async fn test_visit_filtered_objects_filters_one_page() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(
                &[
                    ("logs/old.txt", 1, "2025-03-13T00:00:00.000Z"),
                    ("logs/new.txt", 1, "2026-03-14T11:30:00.000Z"),
                ],
                "logs/",
                false,
                None,
            ))
            .create_async()
            .await;

        let s3 = test_s3(server.url());
        let mut keys = Vec::new();

        visit_filtered_objects(
            &s3,
            Some("logs/".to_string()),
            None,
            None,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap(),
            |object| {
                keys.push(object.key);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(keys, vec!["logs/old.txt"]);
    }

    #[tokio::test]
    async fn test_visit_filtered_objects_filters_multiple_pages() {
        let mut server = Server::new_async().await;
        let _list_1 = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(
                &[("logs/old-a.txt", 1, "2025-01-01T00:00:00.000Z")],
                "logs/",
                true,
                Some("page-2"),
            ))
            .create_async()
            .await;
        let _list_2 = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("continuation-token".into(), "page-2".into()),
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(
                &[
                    ("logs/old-b.txt", 1, "2025-02-01T00:00:00.000Z"),
                    ("logs/new.txt", 1, "2026-03-14T11:59:00.000Z"),
                ],
                "logs/",
                false,
                None,
            ))
            .create_async()
            .await;

        let s3 = test_s3(server.url());
        let mut keys = Vec::new();

        visit_filtered_objects(
            &s3,
            Some("logs/".to_string()),
            None,
            None,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap(),
            |object| {
                keys.push(object.key);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(keys, vec!["logs/old-a.txt", "logs/old-b.txt"]);
    }

    #[tokio::test]
    async fn test_visit_filtered_objects_empty_result() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(&[], "logs/", false, None))
            .create_async()
            .await;

        let s3 = test_s3(server.url());
        let mut keys = Vec::new();

        visit_filtered_objects(
            &s3,
            Some("logs/".to_string()),
            None,
            None,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap(),
            |object| {
                keys.push(object.key);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_visit_filtered_objects_errors_on_invalid_last_modified() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(
                &[("logs/bad.txt", 1, "invalid")],
                "logs/",
                false,
                None,
            ))
            .create_async()
            .await;

        let s3 = test_s3(server.url());
        let err = visit_filtered_objects(
            &s3,
            Some("logs/".to_string()),
            None,
            None,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            Utc.with_ymd_and_hms(2026, 3, 14, 12, 0, 0).unwrap(),
            |_object| Ok(()),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("Failed to parse LastModified for object 'logs/bad.txt'"));
    }

    #[test]
    fn test_objects_json_output_is_stable() {
        let rendered = serde_json::to_value(ObjectsJsonOutput {
            kind: "objects",
            bucket: "bucket".to_string(),
            prefix: Some("logs/".to_string()),
            start_after: None,
            objects: vec![ObjectJsonEntry {
                bucket: "bucket".to_string(),
                key: "logs/a.txt".to_string(),
                size_bytes: 1,
                last_modified: "2026-03-14T00:00:00.000Z".to_string(),
                etag: "\"etag\"".to_string(),
                storage_class: "STANDARD".to_string(),
            }],
        })
        .unwrap();

        assert_eq!(rendered["kind"], "objects");
        assert_eq!(rendered["bucket"], "bucket");
        assert_eq!(rendered["objects"][0]["key"], "logs/a.txt");
        assert_eq!(rendered["objects"][0]["size_bytes"], 1);
    }

    #[tokio::test]
    async fn test_handle_json_objects_branch() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_page_xml(
                &[("logs/a.txt", 1, "2026-03-14T00:00:00.000Z")],
                "",
                false,
                None,
            ))
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::ListObjects {
                bucket: Some("bucket".to_string()),
                json: true,
                list_multipart_uploads: false,
                max_kub: None,
                older_than: None,
                prefix: None,
                start_after: None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_handle_json_buckets_branch() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets><Bucket><Name>bucket-one</Name><CreationDate>2026-03-14T00:00:00.000Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>"#,
            )
            .create_async()
            .await;

        handle(
            &S3::new(
                &Credentials::new(
                    "AKIAIOSFODNN7EXAMPLE",
                    &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
                ),
                &Region::custom("us-west-1", server.url()),
                None,
                false,
            ),
            Action::ListObjects {
                bucket: None,
                json: true,
                list_multipart_uploads: false,
                max_kub: None,
                older_than: None,
                prefix: None,
                start_after: None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_handle_json_multipart_uploads_branch() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><Bucket>bucket</Bucket><MaxUploads>1</MaxUploads><IsTruncated>false</IsTruncated><Upload><Key>logs/a.txt</Key><UploadId>upload-1</UploadId><Initiated>2026-03-14T00:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass><Initiator><ID>id</ID></Initiator><Owner><ID>owner</ID></Owner></Upload></ListMultipartUploadsResult>"#,
            )
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::ListObjects {
                bucket: Some("bucket".to_string()),
                json: true,
                list_multipart_uploads: true,
                max_kub: None,
                older_than: None,
                prefix: None,
                start_after: None,
            },
        )
        .await
        .unwrap();
    }
}
