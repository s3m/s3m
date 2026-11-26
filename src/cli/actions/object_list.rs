use crate::{
    cli::{
        actions::Action,
        age_filter::{AgeFilter, parse_last_modified},
        time,
    },
    s3::{
        S3, actions,
        responses::{Bucket, Object, Upload},
    },
};
use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use colored::Colorize;
use jiff::Timestamp;
use serde::Serialize;
use std::collections::HashSet;

/// Service maximum for `ListObjectsV2` `max-keys`.
const OBJECTS_PAGE_CAP: usize = 1_000;
/// Service maximum for `ListMultipartUploads` `max-uploads`.
const UPLOADS_PAGE_CAP: usize = 1_000;
/// Service maximum for `ListBuckets` `max-buckets`.
const BUCKETS_PAGE_CAP: usize = 10_000;

/// Client-enforced total number of records a listing may still visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListLimit {
    Unlimited,
    Remaining(usize),
}

impl ListLimit {
    /// Parses the `ls --number` value carried by `Action::ListObjects`.
    fn parse(max_kub: Option<&str>) -> Result<Self> {
        max_kub.map_or(Ok(Self::Unlimited), |value| {
            value.parse::<usize>().map(Self::Remaining).map_err(|_| {
                anyhow!("Invalid listing limit '{value}': expected a non-negative integer")
            })
        })
    }

    const fn is_exhausted(self) -> bool {
        matches!(self, Self::Remaining(0))
    }

    /// Page size for the next request: the remaining total, bounded by the service cap.
    fn page_size(self, service_cap: usize) -> usize {
        match self {
            Self::Unlimited => service_cap,
            Self::Remaining(remaining) => remaining.min(service_cap),
        }
    }

    const fn record_visited(&mut self) {
        if let Self::Remaining(remaining) = self {
            *remaining = remaining.saturating_sub(1);
        }
    }
}

/// Treats an empty cursor like an absent one.
fn present(cursor: Option<String>) -> Option<String> {
    cursor.filter(|value| !value.is_empty())
}

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
        let limit = ListLimit::parse(max_kub.as_deref())?;
        match (bucket, list_multipart_uploads) {
            (Some(_), false) => {
                let now = Timestamp::now();
                if json {
                    let bucket_name = s3.bucket().unwrap_or_default().to_string();
                    let objects = collect_filtered_objects(
                        s3,
                        bucket_name.clone(),
                        prefix.clone(),
                        start_after.clone(),
                        limit,
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
                        limit,
                        older_than,
                        now,
                        |object| print_object_info(&object),
                    )
                    .await?;
                }
            }

            (Some(_), true) => {
                if json {
                    let mut uploads = Vec::new();
                    visit_uploads(s3, prefix, start_after, limit, |upload| {
                        uploads.push(MultipartUploadJsonEntry {
                            key: upload.key,
                            upload_id: upload.upload_id,
                            initiated: upload.initiated,
                            storage_class: upload.storage_class,
                        });
                        Ok(())
                    })
                    .await?;
                    print_json(&MultipartUploadsJsonOutput {
                        kind: "multipart_uploads",
                        bucket: s3.bucket().unwrap_or_default().to_string(),
                        uploads,
                    })?;
                } else {
                    visit_uploads(s3, prefix, start_after, limit, |upload| {
                        print_upload_info(&upload)
                    })
                    .await?;
                }
            }

            (None, _) => {
                if json {
                    let mut buckets = Vec::new();
                    visit_buckets(s3, limit, |bucket| {
                        buckets.push(BucketJsonEntry {
                            name: bucket.name,
                            creation_date: bucket.creation_date,
                            server_side_encryption_enabled: bucket.server_side_encryption_enabled,
                        });
                        Ok(())
                    })
                    .await?;
                    print_json(&BucketsJsonOutput {
                        kind: "buckets",
                        buckets,
                    })?;
                } else {
                    visit_buckets(s3, limit, |bucket| print_bucket_info(&bucket)).await?;
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
    limit: ListLimit,
    older_than: Option<AgeFilter>,
    now: Timestamp,
) -> Result<Vec<ObjectJsonEntry>> {
    let mut objects = Vec::new();
    visit_filtered_objects(s3, prefix, start_after, limit, older_than, now, |object| {
        objects.push(ObjectJsonEntry {
            bucket: bucket.clone(),
            key: object.key,
            size_bytes: object.size,
            last_modified: object.last_modified,
            etag: object.e_tag,
            storage_class: object.storage_class,
        });
        Ok(())
    })
    .await?;
    Ok(objects)
}

/// Visits listed objects across `ListObjectsV2` pages, applies the optional age filter and stops
/// once `limit` objects passed the filter.
///
/// Filtering is based on each object's `LastModified` timestamp interpreted in UTC. Only
/// eligible objects consume the total. Filtered requests use full service pages so
/// sparse matches don't force small requests. Unfiltered requests ask for at most
/// the remaining total.
///
/// # Errors
/// Will return an error if listing fails, if `LastModified` can not be parsed for filtered
/// objects, or if more objects are needed and S3 returns a truncated page without a fresh
/// continuation token.
pub(crate) async fn visit_filtered_objects<F>(
    s3: &S3,
    prefix: Option<String>,
    start_after: Option<String>,
    limit: ListLimit,
    older_than: Option<AgeFilter>,
    now: Timestamp,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(Object) -> Result<()>,
{
    let mut limit = limit;
    let mut start_after = start_after;
    let mut continuation_token: Option<String> = None;
    let mut seen_tokens = HashSet::new();

    while !limit.is_exhausted() {
        let page_size = if older_than.is_some() {
            OBJECTS_PAGE_CAP
        } else {
            limit.page_size(OBJECTS_PAGE_CAP)
        };
        let mut action = actions::ListObjectsV2::new(
            prefix.clone(),
            start_after.take(),
            Some(page_size.to_string()),
        );
        action.continuation_token = continuation_token.take();
        let page = action.request(s3).await?;

        for object in page.contents {
            if limit.is_exhausted() {
                return Ok(());
            }
            if older_than.map_or(Ok(true), |filter| filter.matches(&object, now))? {
                visit(object)?;
                limit.record_visited();
            }
        }

        if limit.is_exhausted() || !page.is_truncated {
            return Ok(());
        }

        let token = present(page.next_continuation_token).ok_or_else(|| {
            anyhow!("ListObjectsV2 returned a truncated response without a continuation token")
        })?;
        if !seen_tokens.insert(token.clone()) {
            return Err(anyhow!(
                "ListObjectsV2 repeated a continuation token; refusing to loop"
            ));
        }
        continuation_token = Some(token);
    }

    Ok(())
}

/// Visits in-progress multipart uploads across `ListMultipartUploads` pages until `limit`
/// uploads were visited. `start_after` is sent once as `key-marker` (skipping every upload of
/// that key); later pages use the returned `NextKeyMarker`/`NextUploadIdMarker` pair.
async fn visit_uploads<F>(
    s3: &S3,
    prefix: Option<String>,
    start_after: Option<String>,
    limit: ListLimit,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(Upload) -> Result<()>,
{
    let mut limit = limit;
    let mut key_marker = start_after;
    let mut upload_id_marker: Option<String> = None;
    let mut seen_markers = HashSet::new();

    while !limit.is_exhausted() {
        let mut action =
            actions::ListMultipartUploads::new(Some(limit.page_size(UPLOADS_PAGE_CAP).to_string()));
        action.prefix.clone_from(&prefix);
        action.key_marker = key_marker.take();
        action.upload_id_marker = upload_id_marker.take();
        let page = action.request(s3).await?;

        for upload in page.upload.unwrap_or_default() {
            if limit.is_exhausted() {
                return Ok(());
            }
            visit(upload)?;
            limit.record_visited();
        }

        if limit.is_exhausted() || !page.is_truncated {
            return Ok(());
        }

        let (Some(next_key), Some(next_upload_id)) = (
            present(page.next_key_marker),
            present(page.next_upload_id_marker),
        ) else {
            return Err(anyhow!(
                "ListMultipartUploads returned a truncated response without NextKeyMarker and NextUploadIdMarker"
            ));
        };
        if !seen_markers.insert((next_key.clone(), next_upload_id.clone())) {
            return Err(anyhow!(
                "ListMultipartUploads repeated a marker pair; refusing to loop"
            ));
        }
        key_marker = Some(next_key);
        upload_id_marker = Some(next_upload_id);
    }

    Ok(())
}

/// Visits buckets across service-capped `ListBuckets` pages until `limit`
/// buckets were visited; an absent `ContinuationToken` ends the listing.
async fn visit_buckets<F>(s3: &S3, limit: ListLimit, mut visit: F) -> Result<()>
where
    F: FnMut(Bucket) -> Result<()>,
{
    let mut limit = limit;
    let mut continuation_token: Option<String> = None;
    let mut seen_tokens = HashSet::new();
    // `max-buckets` only sizes the pages; the limit is enforced here. Cloudflare R2
    // doesn't implement it and answers 501, so the listing goes on without it.
    let mut page_size_supported = true;

    while !limit.is_exhausted() {
        let max_buckets =
            page_size_supported.then(|| limit.page_size(BUCKETS_PAGE_CAP).to_string());
        let mut action = actions::ListBuckets::new(max_buckets);
        action.continuation_token.clone_from(&continuation_token);
        let page = match action.request(s3).await {
            Err(error)
                if page_size_supported
                    && (error.status() == Some(501) || error.code() == Some("NotImplemented")) =>
            {
                log::info!("ListBuckets max-buckets is not implemented, listing without it");
                page_size_supported = false;
                continue;
            }
            page => page?,
        };

        for bucket in page.buckets.bucket {
            if limit.is_exhausted() {
                return Ok(());
            }
            visit(bucket)?;
            limit.record_visited();
        }

        if limit.is_exhausted() {
            return Ok(());
        }
        let Some(token) = present(page.continuation_token) else {
            return Ok(());
        };
        if !seen_tokens.insert(token.clone()) {
            return Err(anyhow!(
                "ListBuckets repeated a continuation token; refusing to loop"
            ));
        }
        continuation_token = Some(token);
    }

    Ok(())
}

fn print_object_info(object: &Object) -> Result<()> {
    let last_modified = parse_last_modified(object)?;
    println!(
        "{} {:>10} {:<}",
        format!("[{}]", time::display(last_modified)).green(),
        ByteSize(object.size).to_string().yellow(),
        object.key
    );
    Ok(())
}

fn print_upload_info(upload: &Upload) -> Result<()> {
    let initiated = time::parse_rfc3339(&upload.initiated)?;
    println!(
        "{} {} {}",
        format!("[{}]", time::display(initiated)).green(),
        upload.upload_id.yellow(),
        upload.key
    );
    Ok(())
}

fn print_bucket_info(bucket: &Bucket) -> Result<()> {
    let creation_date = time::parse_rfc3339(&bucket.creation_date)?;
    println!(
        "{} {}",
        format!("[{}]", time::display(creation_date)).green(),
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
            ListLimit::Unlimited,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            "2026-03-14T12:00:00Z".parse().unwrap(),
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
            ListLimit::Unlimited,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            "2026-03-14T12:00:00Z".parse().unwrap(),
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
            ListLimit::Unlimited,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            "2026-03-14T12:00:00Z".parse().unwrap(),
            |object| {
                keys.push(object.key);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(keys, Vec::<String>::new());
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
            ListLimit::Unlimited,
            Some(crate::cli::age_filter::parse_age_filter("30d").unwrap()),
            "2026-03-14T12:00:00Z".parse().unwrap(),
            |_object| Ok(()),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("Failed to parse LastModified for object 'logs/bad.txt'"));
    }

    const TOKEN: &str = "1/a+b=c\u{e9}";
    const OLD: &str = "2025-01-01T00:00:00.000Z";
    const NEW: &str = "2026-03-14T11:00:00.000Z";
    // Exactly `now - 30d`: not strictly older, so never selected.
    const CUTOFF: &str = "2026-02-12T12:00:00.000Z";

    fn now() -> Timestamp {
        "2026-03-14T12:00:00Z".parse().unwrap()
    }

    fn host_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &Region::custom("us-west-1", endpoint),
            None,
            false,
        )
    }

    /// Matches a request only when its decoded query pairs are exactly `expected`.
    fn exact_query(
        expected: &[(&str, &str)],
    ) -> impl Fn(&mockito::Request) -> bool + Send + Sync + 'static {
        let mut expected: Vec<(String, String)> = expected
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        expected.sort();
        move |request| {
            let query = request
                .path_and_query()
                .split_once('?')
                .map_or("", |(_, query)| query);
            let mut actual: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect();
            actual.sort();
            actual == expected
        }
    }

    async fn page(
        server: &mut Server,
        path: &str,
        query: &[(&str, &str)],
        body: String,
    ) -> mockito::Mock {
        server
            .mock("GET", path)
            .match_query(Matcher::Any)
            .match_request(exact_query(query))
            .with_status(200)
            .with_body(body)
            .expect(1)
            .create_async()
            .await
    }

    fn numbered(prefix: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("{prefix}{index:04}"))
            .collect()
    }

    fn uploads_page_xml(
        uploads: &[(&str, &str)],
        is_truncated: bool,
        next: (Option<&str>, Option<&str>),
    ) -> String {
        let mut body = String::new();
        if let Some(key) = next.0 {
            let _ = write!(body, "<NextKeyMarker>{key}</NextKeyMarker>");
        }
        if let Some(upload_id) = next.1 {
            let _ = write!(body, "<NextUploadIdMarker>{upload_id}</NextUploadIdMarker>");
        }
        for (key, upload_id) in uploads {
            let _ = write!(
                body,
                "<Upload><Key>{key}</Key><UploadId>{upload_id}</UploadId><Initiated>{OLD}</Initiated><StorageClass>STANDARD</StorageClass></Upload>"
            );
        }
        format!(
            "<ListMultipartUploadsResult><Bucket>bucket</Bucket><MaxUploads>1000</MaxUploads><IsTruncated>{is_truncated}</IsTruncated>{body}</ListMultipartUploadsResult>"
        )
    }

    fn buckets_page_xml(names: &[String], token: Option<&str>) -> String {
        let mut buckets = String::new();
        for name in names {
            let _ = write!(
                buckets,
                "<Bucket><Name>{name}</Name><CreationDate>{OLD}</CreationDate></Bucket>"
            );
        }
        let token = token.map_or_else(String::new, |token| {
            format!("<ContinuationToken>{token}</ContinuationToken>")
        });
        format!(
            "<ListAllMyBucketsResult><Buckets>{buckets}</Buckets>{token}</ListAllMyBucketsResult>"
        )
    }

    async fn listed_keys(
        s3: &S3,
        limit: ListLimit,
        older_than: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        visit_filtered_objects(
            s3,
            Some("logs/".to_string()),
            None,
            limit,
            older_than.map(|value| crate::cli::age_filter::parse_age_filter(value).unwrap()),
            now(),
            |object| {
                keys.push(object.key);
                Ok(())
            },
        )
        .await?;
        Ok(keys)
    }

    async fn listed_upload_ids(s3: &S3, limit: ListLimit) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        visit_uploads(s3, None, None, limit, |upload| {
            ids.push(upload.upload_id);
            Ok(())
        })
        .await?;
        Ok(ids)
    }

    async fn listed_buckets(s3: &S3, limit: ListLimit) -> Result<Vec<String>> {
        let mut names = Vec::new();
        visit_buckets(s3, limit, |bucket| {
            names.push(bucket.name);
            Ok(())
        })
        .await?;
        Ok(names)
    }

    #[tokio::test]
    async fn test_objects_total_requests_remaining_up_to_service_cap() {
        let mut server = Server::new_async().await;
        let names = numbered("logs/k-", 1_002);
        let entries: Vec<(&str, u64, &str)> =
            names.iter().map(|key| (key.as_str(), 1, OLD)).collect();
        let page_1 = page(
            &mut server,
            "/bucket",
            &[
                ("list-type", "2"),
                ("max-keys", "1000"),
                ("prefix", "logs/"),
            ],
            list_objects_page_xml(&entries[..1_000], "logs/", true, Some(TOKEN)),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/bucket",
            &[
                ("list-type", "2"),
                ("max-keys", "1"),
                ("prefix", "logs/"),
                ("continuation-token", TOKEN),
            ],
            list_objects_page_xml(&entries[1_000..], "logs/", true, Some("unused")),
        )
        .await;

        let keys = listed_keys(&test_s3(server.url()), ListLimit::Remaining(1_001), None)
            .await
            .unwrap();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(keys, names[..1_001]);
    }

    #[tokio::test]
    async fn test_objects_total_counts_only_strictly_older_matches() {
        let mut server = Server::new_async().await;
        let first_page = [
            ("list-type", "2"),
            ("max-keys", "1000"),
            ("prefix", "logs/"),
        ];
        // 2 of 5 match on page 1, an empty truncated page, then the cap is reached on
        // page 3 before a malformed timestamp and a missing continuation token.
        let page_1 = page(
            &mut server,
            "/bucket",
            &first_page,
            list_objects_page_xml(
                &[
                    ("logs/old-a", 1, OLD),
                    ("logs/edge", 1, CUTOFF),
                    ("logs/new-1", 1, NEW),
                    ("logs/old-b", 1, OLD),
                    ("logs/new-2", 1, NEW),
                ],
                "logs/",
                true,
                Some("t2"),
            ),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/bucket",
            &[
                first_page[0],
                first_page[1],
                first_page[2],
                ("continuation-token", "t2"),
            ],
            list_objects_page_xml(&[], "logs/", true, Some("t3")),
        )
        .await;
        let page_3 = page(
            &mut server,
            "/bucket",
            &[
                first_page[0],
                first_page[1],
                first_page[2],
                ("continuation-token", "t3"),
            ],
            list_objects_page_xml(
                &[("logs/old-c", 1, OLD), ("logs/bad", 1, "invalid")],
                "logs/",
                true,
                None,
            ),
        )
        .await;

        let keys = listed_keys(&test_s3(server.url()), ListLimit::Remaining(3), Some("30d"))
            .await
            .unwrap();

        page_1.assert_async().await;
        page_2.assert_async().await;
        page_3.assert_async().await;
        assert_eq!(keys, ["logs/old-a", "logs/old-b", "logs/old-c"]);
    }

    #[tokio::test]
    async fn test_objects_stop_at_exact_cap_without_cursor() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/bucket",
            &[("list-type", "2"), ("max-keys", "2"), ("prefix", "logs/")],
            list_objects_page_xml(
                &[("logs/a", 1, OLD), ("logs/b", 1, OLD)],
                "logs/",
                true,
                None,
            ),
        )
        .await;

        let keys = listed_keys(&test_s3(server.url()), ListLimit::Remaining(2), None)
            .await
            .unwrap();

        page_1.assert_async().await;
        assert_eq!(keys, ["logs/a", "logs/b"]);
    }

    #[tokio::test]
    async fn test_objects_terminal_page_ignores_stray_token() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/bucket",
            &[
                ("list-type", "2"),
                ("max-keys", "1000"),
                ("prefix", "logs/"),
            ],
            list_objects_page_xml(&[("logs/a", 1, OLD)], "logs/", false, Some("stray")),
        )
        .await;

        let keys = listed_keys(&test_s3(server.url()), ListLimit::Unlimited, None)
            .await
            .unwrap();

        page_1.assert_async().await;
        assert_eq!(keys, ["logs/a"]);
    }

    #[tokio::test]
    async fn test_objects_missing_or_empty_cursor_errors_when_more_needed() {
        for token in [None, Some("")] {
            let mut server = Server::new_async().await;
            let page_1 = page(
                &mut server,
                "/bucket",
                &[
                    ("list-type", "2"),
                    ("max-keys", "1000"),
                    ("prefix", "logs/"),
                ],
                list_objects_page_xml(&[("logs/a", 1, OLD)], "logs/", true, token),
            )
            .await;

            let err = listed_keys(&test_s3(server.url()), ListLimit::Unlimited, None)
                .await
                .unwrap_err()
                .to_string();

            page_1.assert_async().await;
            assert!(
                err.contains("without a continuation token"),
                "{token:?}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn test_objects_repeated_cursor_errors() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/bucket",
            &[
                ("list-type", "2"),
                ("max-keys", "1000"),
                ("prefix", "logs/"),
            ],
            list_objects_page_xml(&[("logs/a", 1, OLD)], "logs/", true, Some("loop")),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/bucket",
            &[
                ("list-type", "2"),
                ("max-keys", "1000"),
                ("prefix", "logs/"),
                ("continuation-token", "loop"),
            ],
            list_objects_page_xml(&[("logs/b", 1, OLD)], "logs/", true, Some("loop")),
        )
        .await;

        let err = listed_keys(&test_s3(server.url()), ListLimit::Unlimited, None)
            .await
            .unwrap_err()
            .to_string();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert!(err.contains("repeated a continuation token"), "{err}");
    }

    #[tokio::test]
    async fn test_uploads_total_counts_shared_key_sessions_across_marker_pages() {
        let mut server = Server::new_async().await;
        let ids = numbered("u-", 1_001);
        let shared: Vec<(&str, &str)> = ids[..1_000]
            .iter()
            .map(|id| ("shared", id.as_str()))
            .collect();
        let page_1 = page(
            &mut server,
            "/bucket",
            &[("uploads", ""), ("max-uploads", "1000")],
            uploads_page_xml(&shared, true, (Some("shared"), Some("u-0999"))),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/bucket",
            &[
                ("uploads", ""),
                ("max-uploads", "1"),
                ("key-marker", "shared"),
                ("upload-id-marker", "u-0999"),
            ],
            uploads_page_xml(
                &[("shared", "u-1000"), ("z", "u-1001")],
                true,
                (Some("z"), Some("u-1001")),
            ),
        )
        .await;

        let listed = listed_upload_ids(&test_s3(server.url()), ListLimit::Remaining(1_001))
            .await
            .unwrap();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(listed, ids);
    }

    #[tokio::test]
    async fn test_uploads_missing_upload_id_marker_errors_only_when_more_needed() {
        let body = uploads_page_xml(&[("k", "u-1")], true, (Some("k"), None));

        let mut server = Server::new_async().await;
        let unlimited = page(
            &mut server,
            "/bucket",
            &[("uploads", ""), ("max-uploads", "1000")],
            body.clone(),
        )
        .await;
        let capped = page(
            &mut server,
            "/bucket",
            &[("uploads", ""), ("max-uploads", "1")],
            body,
        )
        .await;
        let s3 = test_s3(server.url());

        let err = listed_upload_ids(&s3, ListLimit::Unlimited)
            .await
            .unwrap_err()
            .to_string();
        let listed = listed_upload_ids(&s3, ListLimit::Remaining(1))
            .await
            .unwrap();

        unlimited.assert_async().await;
        capped.assert_async().await;
        assert!(err.contains("ListMultipartUploads"), "{err}");
        assert_eq!(listed, ["u-1"]);
    }

    #[tokio::test]
    async fn test_uploads_repeated_marker_pair_errors() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/bucket",
            &[("uploads", ""), ("max-uploads", "1000")],
            uploads_page_xml(&[("k", "u-1")], true, (Some("k"), Some("u-1"))),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/bucket",
            &[
                ("uploads", ""),
                ("max-uploads", "1000"),
                ("key-marker", "k"),
                ("upload-id-marker", "u-1"),
            ],
            uploads_page_xml(&[("k", "u-2")], true, (Some("k"), Some("u-1"))),
        )
        .await;

        let err = listed_upload_ids(&test_s3(server.url()), ListLimit::Unlimited)
            .await
            .unwrap_err()
            .to_string();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert!(err.contains("repeated a marker pair"), "{err}");
    }

    #[tokio::test]
    async fn test_buckets_unlimited_requests_service_cap_and_follows_token() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/",
            &[("max-buckets", "10000")],
            buckets_page_xml(&["b-1".to_string()], Some(TOKEN)),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/",
            &[("max-buckets", "10000"), ("continuation-token", TOKEN)],
            buckets_page_xml(&["b-2".to_string()], None),
        )
        .await;

        let names = listed_buckets(&host_s3(server.url()), ListLimit::Unlimited)
            .await
            .unwrap();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(names, ["b-1", "b-2"]);
    }

    #[tokio::test]
    async fn test_buckets_total_requests_remaining_up_to_service_cap() {
        let mut server = Server::new_async().await;
        let names = numbered("b-", 10_002);
        let page_1 = page(
            &mut server,
            "/",
            &[("max-buckets", "10000")],
            buckets_page_xml(&names[..10_000], Some("t2")),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/",
            &[("max-buckets", "1"), ("continuation-token", "t2")],
            buckets_page_xml(&names[10_000..], Some("t3")),
        )
        .await;

        let listed = listed_buckets(&host_s3(server.url()), ListLimit::Remaining(10_001))
            .await
            .unwrap();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(listed, names[..10_001]);
    }

    #[tokio::test]
    async fn test_buckets_repeated_token_errors() {
        let mut server = Server::new_async().await;
        let page_1 = page(
            &mut server,
            "/",
            &[("max-buckets", "10000")],
            buckets_page_xml(&["b-1".to_string()], Some("loop")),
        )
        .await;
        let page_2 = page(
            &mut server,
            "/",
            &[("max-buckets", "10000"), ("continuation-token", "loop")],
            buckets_page_xml(&["b-2".to_string()], Some("loop")),
        )
        .await;

        let err = listed_buckets(&host_s3(server.url()), ListLimit::Unlimited)
            .await
            .unwrap_err()
            .to_string();

        page_1.assert_async().await;
        page_2.assert_async().await;
        assert!(err.contains("repeated a continuation token"), "{err}");
    }

    #[tokio::test]
    async fn test_handle_rejects_invalid_limit_without_requests() {
        let mut server = Server::new_async().await;
        let any_request = server
            .mock("GET", Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        for value in ["abc", "-1", "", "1.5", "18446744073709551616"] {
            let err = handle(
                &test_s3(server.url()),
                Action::ListObjects {
                    bucket: Some("bucket".to_string()),
                    json: true,
                    list_multipart_uploads: false,
                    max_kub: Some(value.to_string()),
                    older_than: None,
                    prefix: None,
                    start_after: None,
                },
            )
            .await
            .unwrap_err()
            .to_string();

            assert!(err.contains("Invalid listing limit"), "{value}: {err}");
        }
        any_request.assert_async().await;
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
            .match_query(Matcher::UrlEncoded("max-buckets".into(), "10000".into()))
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
