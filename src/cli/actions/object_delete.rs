use crate::{
    cli::{
        actions::{Action, DeleteGroup, object_list},
        age_filter::AgeFilter,
    },
    s3::{S3, actions},
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use std::fmt::Write as _;

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::DeleteObject {
        key,
        upload_id,
        bucket,
        older_than,
        recursive,
        targets,
        version_id,
        bypass_governance,
    } = action
    {
        if bucket {
            if recursive {
                delete_bucket_recursive(s3, bypass_governance).await?;
            } else {
                let action = actions::DeleteBucket::new();
                action.request(s3).await?;
            }
        } else if upload_id.is_empty() {
            if recursive {
                // `--recursive` without `--bucket`: delete every object under the
                // literal key prefix. The bucket itself is never deleted here.
                return delete_prefix_recursive(s3, &key, bypass_governance).await;
            }

            if let Some(older_than) = older_than {
                let matched = collect_matched_delete_objects(
                    s3,
                    if key.is_empty() {
                        None
                    } else {
                        Some(key.clone())
                    },
                    older_than,
                    Utc::now(),
                )
                .await?;

                delete_matched_objects(s3, &matched, bypass_governance).await?;
                print_filtered_delete_summary(matched.len(), matched.len());
                return Ok(());
            }

            if count_delete_targets(&targets) <= 1 {
                let (target_s3, target_key) = single_delete_target(s3, &key, &targets)?;
                let output = actions::DeleteObject::new(target_key)
                    .version_id(version_id.clone())
                    .bypass_governance(bypass_governance)
                    .request(target_s3)
                    .await?;
                report_single_delete(target_key, version_id.as_deref(), &output);
            } else {
                delete_groups(&targets, bypass_governance).await?;
            }
        } else {
            let target_s3 = targets.first().map_or(s3, |group| &group.s3);
            let action = actions::AbortMultipartUpload::new(&key, &upload_id);
            action.request(target_s3).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct PrefixDeleteStats {
    /// Keys S3 confirmed as handled by `DeleteObjects`.
    removed: usize,
    /// Subset of `removed` where S3 only added a delete marker (versioned bucket).
    delete_markers: usize,
}

/// Delete every object whose key starts with the literal `prefix`.
///
/// Uses `ListObjectsV2` (current objects only) and `DeleteObjects` without version
/// ids, so in versioned buckets this only adds delete markers and retains older
/// versions. Lists and deletes page by page, so memory is bounded by one page.
async fn delete_prefix_recursive(s3: &S3, prefix: &str, bypass_governance: bool) -> Result<()> {
    if prefix.is_empty() {
        return Err(anyhow!(
            "Refusing recursive delete with an empty prefix; use `-b --recursive` to delete a whole bucket"
        ));
    }

    if let Some(warning) = missing_trailing_slash_warning(prefix) {
        eprintln!("{warning}");
    }

    let mut stats = PrefixDeleteStats::default();
    if let Err(err) = delete_prefix_pages(s3, prefix, bypass_governance, &mut stats).await {
        return Err(anyhow!(
            "Recursive delete of prefix '{prefix}' is incomplete after removing {} {}: {err:#}",
            stats.removed,
            object_label(stats.removed)
        ));
    }

    if stats.removed == 0 {
        println!("No objects found under prefix '{prefix}'; nothing deleted.");
    } else if stats.delete_markers > 0 {
        println!(
            "Deleted {} {} under prefix '{prefix}': {} delete {} created, earlier versions are retained.",
            stats.removed,
            object_label(stats.removed),
            stats.delete_markers,
            if stats.delete_markers == 1 {
                "marker"
            } else {
                "markers"
            }
        );
    } else {
        println!(
            "Deleted {} {} under prefix '{prefix}'.",
            stats.removed,
            object_label(stats.removed)
        );
    }

    Ok(())
}

/// A literal prefix without a trailing `/` also matches sibling keys such as
/// `integration/180/...` for `integration/18`; warn so this is never a surprise.
fn missing_trailing_slash_warning(prefix: &str) -> Option<String> {
    if prefix.ends_with('/') {
        return None;
    }

    Some(format!(
        "warning: prefix '{prefix}' is matched literally and also matches keys such as '{prefix}0' or '{prefix}-other/...'; add a trailing '/' to match only keys under '{prefix}/'"
    ))
}

async fn delete_prefix_pages(
    s3: &S3,
    prefix: &str,
    bypass_governance: bool,
    stats: &mut PrefixDeleteStats,
) -> Result<()> {
    let mut continuation_token: Option<String> = None;

    loop {
        let mut list = actions::ListObjectsV2::new(
            Some(prefix.to_string()),
            None,
            Some(actions::DeleteObjects::MAX_OBJECTS.to_string()),
        );
        list.continuation_token = continuation_token.take();
        let page = list.request(s3).await?;

        // Never delete a key outside the prefix, even if the server ignored it.
        let identifiers = page
            .contents
            .into_iter()
            .map(|object| {
                if object.key.starts_with(prefix) {
                    Ok(actions::ObjectIdentifier {
                        key: object.key,
                        version_id: None,
                    })
                } else {
                    Err(anyhow!(
                        "ListObjectsV2 returned key '{}' outside prefix '{prefix}'; nothing more was deleted",
                        object.key
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        for batch in split_delete_batches(&identifiers) {
            let requested = batch.len();
            let result = actions::DeleteObjects::new(batch, false)
                .bypass_governance(bypass_governance)
                .request(s3)
                .await?;

            if !result.errors.is_empty() {
                // Keys confirmed before the first error still count as removed.
                stats.removed += result.deleted.len();
                stats.delete_markers += result.deleted.iter().filter(|d| d.delete_marker).count();
                return Err(anyhow!(format_delete_objects_errors(&result.errors)));
            }

            if result.deleted.len() != requested {
                stats.removed += result.deleted.len();
                stats.delete_markers += result.deleted.iter().filter(|d| d.delete_marker).count();
                return Err(anyhow!(
                    "DeleteObjects confirmed {} of {requested} requested keys",
                    result.deleted.len()
                ));
            }

            stats.removed += requested;
            stats.delete_markers += result.deleted.iter().filter(|d| d.delete_marker).count();
        }

        if !page.is_truncated {
            return Ok(());
        }

        continuation_token.clone_from(&page.next_continuation_token);
        if continuation_token.is_none() {
            return Err(anyhow!(
                "ListObjectsV2 returned a truncated response without a continuation token"
            ));
        }
    }
}

async fn collect_matched_delete_objects(
    s3: &S3,
    prefix: Option<String>,
    older_than: AgeFilter,
    now: DateTime<Utc>,
) -> Result<Vec<actions::ObjectIdentifier>> {
    let mut objects = Vec::new();
    object_list::visit_filtered_objects(
        s3,
        prefix,
        None,
        Some(actions::DeleteObjects::MAX_OBJECTS.to_string()),
        Some(older_than),
        now,
        |object| {
            objects.push(actions::ObjectIdentifier {
                key: object.key,
                version_id: None,
            });
            Ok(())
        },
    )
    .await?;

    Ok(objects)
}

fn count_delete_targets(targets: &[DeleteGroup]) -> usize {
    targets.iter().map(|group| group.objects.len()).sum()
}

fn single_delete_target<'a>(
    fallback_s3: &'a S3,
    fallback_key: &'a str,
    targets: &'a [DeleteGroup],
) -> Result<(&'a S3, &'a str)> {
    if let Some(group) = targets.first()
        && let Some(object) = group.objects.first()
    {
        return Ok((&group.s3, &object.key));
    }

    if fallback_key.is_empty() {
        return Err(anyhow!("Object target missing"));
    }

    Ok((fallback_s3, fallback_key))
}

/// Report the outcome of a single-object delete, making it clear when a
/// versioned bucket only inserted a delete marker (data retained) rather than
/// removing the object.
fn report_single_delete(
    key: &str,
    requested_version: Option<&str>,
    output: &actions::DeleteObjectOutput,
) {
    if output.delete_marker {
        match &output.version_id {
            Some(v) => println!(
                "{key}: delete marker created (version {v}); existing versions are retained"
            ),
            None => println!("{key}: delete marker created; existing versions are retained"),
        }
    } else if let Some(v) = requested_version {
        println!("{key}: deleted version {v}");
    } else {
        println!("{key}: deleted");
    }
}

async fn delete_groups(groups: &[DeleteGroup], bypass_governance: bool) -> Result<()> {
    for group in groups {
        for batch in split_delete_batches(&group.objects) {
            let result = actions::DeleteObjects::new(batch, true)
                .bypass_governance(bypass_governance)
                .request(&group.s3)
                .await?;

            if !result.errors.is_empty() {
                return Err(anyhow!(format_delete_objects_errors(&result.errors)));
            }
        }
    }

    Ok(())
}

async fn delete_matched_objects(
    s3: &S3,
    objects: &[actions::ObjectIdentifier],
    bypass_governance: bool,
) -> Result<()> {
    match objects {
        [] => Ok(()),
        [object] => {
            actions::DeleteObject::new(&object.key)
                .bypass_governance(bypass_governance)
                .request(s3)
                .await?;
            Ok(())
        }
        _ => {
            for batch in split_delete_batches(objects) {
                let result = actions::DeleteObjects::new(batch, true)
                    .bypass_governance(bypass_governance)
                    .request(s3)
                    .await?;

                if !result.errors.is_empty() {
                    return Err(anyhow!(format_delete_objects_errors(&result.errors)));
                }
            }

            Ok(())
        }
    }
}

fn print_filtered_delete_summary(matched: usize, deleted: usize) {
    println!(
        "Matched {matched} {}; deleted {deleted} {}.",
        object_label(matched),
        object_label(deleted)
    );
}

fn object_label(count: usize) -> &'static str {
    if count == 1 { "object" } else { "objects" }
}

async fn delete_bucket_recursive(s3: &S3, bypass_governance: bool) -> Result<()> {
    // List and delete every object *version* and delete marker, then delete the
    // now-empty bucket. This works for versioned / Object-Lock buckets as well as
    // plain buckets (where each object is reported as a single "null" version).
    // Each round re-lists from the start, so deleting the listed entries makes the
    // next page surface until the bucket is empty.
    //
    // On an Object-Lock bucket, retained versions are refused by `DeleteObjects`
    // unless `--bypass-governance` is given (and COMPLIANCE retention can never be
    // bypassed); those errors are surfaced and the bucket is left intact.
    loop {
        let result = actions::ListObjectVersions::new("").request(s3).await?;

        let identifiers: Vec<actions::ObjectIdentifier> = result
            .versions
            .into_iter()
            .map(|v| actions::ObjectIdentifier {
                key: v.key,
                version_id: Some(v.version_id),
            })
            .chain(
                result
                    .delete_markers
                    .into_iter()
                    .map(|d| actions::ObjectIdentifier {
                        key: d.key,
                        version_id: Some(d.version_id),
                    }),
            )
            .collect();

        if identifiers.is_empty() {
            if result.is_truncated {
                return Err(anyhow!(
                    "ListObjectVersions returned a truncated response without entries"
                ));
            }

            break;
        }

        for batch in split_delete_batches(&identifiers) {
            let result = actions::DeleteObjects::new(batch, true)
                .bypass_governance(bypass_governance)
                .request(s3)
                .await?;

            if !result.errors.is_empty() {
                return Err(anyhow!(format_delete_objects_errors(&result.errors)));
            }
        }
    }

    let action = actions::DeleteBucket::new();
    action.request(s3).await?;

    Ok(())
}

fn split_delete_batches(
    objects: &[actions::ObjectIdentifier],
) -> Vec<Vec<actions::ObjectIdentifier>> {
    objects
        .chunks(actions::DeleteObjects::MAX_OBJECTS)
        .map(<[actions::ObjectIdentifier]>::to_vec)
        .collect()
}

fn format_delete_objects_errors(errors: &[crate::s3::responses::DeleteError]) -> String {
    let mut message = format!("DeleteObjects returned {} object error(s):", errors.len());

    for error in errors {
        let object = match &error.version_id {
            Some(version_id) => format!("{}?versionId={version_id}", error.key),
            None => error.key.clone(),
        };

        let _ = write!(message, "\n - {object}: {} ({})", error.message, error.code);
    }

    message
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
    use crate::{
        cli::actions::DeleteGroup,
        s3::{Credentials, Region, actions::ObjectIdentifier, responses::DeleteError},
    };
    use secrecy::SecretString;

    fn test_s3(bucket: &str) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some(bucket.to_string()),
            false,
        )
    }

    #[test]
    fn test_split_delete_batches() {
        let objects: Vec<ObjectIdentifier> = (0..2_001)
            .map(|index| ObjectIdentifier {
                key: format!("key-{index}"),
                version_id: None,
            })
            .collect();

        let batches = split_delete_batches(&objects);

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 1_000);
        assert_eq!(batches[1].len(), 1_000);
        assert_eq!(batches[2].len(), 1);
    }

    #[test]
    fn test_format_delete_objects_errors() {
        let message = format_delete_objects_errors(&[
            DeleteError {
                key: "one".to_string(),
                version_id: None,
                code: "AccessDenied".to_string(),
                message: "Access Denied".to_string(),
            },
            DeleteError {
                key: "two".to_string(),
                version_id: Some("v2".to_string()),
                code: "InternalError".to_string(),
                message: "try again".to_string(),
            },
        ]);

        assert!(message.contains("DeleteObjects returned 2 object error(s):"));
        assert!(message.contains("one: Access Denied (AccessDenied)"));
        assert!(message.contains("two?versionId=v2: try again (InternalError)"));
    }

    #[test]
    fn test_count_delete_targets() {
        let targets = vec![
            DeleteGroup {
                objects: vec![ObjectIdentifier {
                    key: "one".to_string(),
                    version_id: None,
                }],
                s3: test_s3("bucket-a"),
            },
            DeleteGroup {
                objects: vec![
                    ObjectIdentifier {
                        key: "two".to_string(),
                        version_id: None,
                    },
                    ObjectIdentifier {
                        key: "three".to_string(),
                        version_id: None,
                    },
                ],
                s3: test_s3("bucket-b"),
            },
        ];

        assert_eq!(count_delete_targets(&targets), 3);
    }

    #[test]
    fn test_single_delete_target_prefers_group_target() {
        let targets = vec![DeleteGroup {
            objects: vec![ObjectIdentifier {
                key: "one".to_string(),
                version_id: None,
            }],
            s3: test_s3("bucket-a"),
        }];

        let fallback = test_s3("fallback");
        let (target_s3, target_key) =
            single_delete_target(&fallback, "fallback", &targets).unwrap();

        assert_eq!(target_key, "one");
        assert!(target_s3.endpoint().unwrap().as_str().contains("/bucket-a"));
    }

    #[test]
    fn test_print_filtered_delete_summary_pluralization() {
        assert_eq!(object_label(1), "object");
        assert_eq!(object_label(2), "objects");
    }

    // ---- prefix-scoped recursive deletion (mocked S3) ----

    fn mock_s3(endpoint: String) -> S3 {
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

    fn list_page(keys: &[String], truncated: bool, next: Option<&str>) -> String {
        let mut contents = String::new();
        for key in keys {
            let _ = write!(
                contents,
                "<Contents><Key>{key}</Key><LastModified>2026-03-14T00:00:00.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
            );
        }
        let next = next.map_or_else(String::new, |t| {
            format!("<NextContinuationToken>{t}</NextContinuationToken>")
        });
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>"#
        )
    }

    fn keys(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|i| format!("integration/18/k-{i:04}")).collect()
    }

    fn requested_keys(body: &str) -> Vec<String> {
        body.split("<Key>")
            .skip(1)
            .filter_map(|s| s.split("</Key>").next())
            .map(str::to_string)
            .collect()
    }

    /// `DeleteObjects` responder confirming every requested key; asserts <=1000 keys.
    fn confirm_all(server: &mut mockito::Server, delete_marker: bool) -> mockito::Mock {
        server
            .mock("POST", "/bucket")
            .match_query(mockito::Matcher::UrlEncoded("delete".into(), String::new()))
            .match_request(|req| {
                requested_keys(&req.utf8_lossy_body().unwrap()).len() <= 1_000
            })
            .with_status(200)
            .with_body_from_request(move |req| {
                let mut out = String::from("<DeleteResult>");
                for key in requested_keys(&req.utf8_lossy_body().unwrap()) {
                    let _ = write!(
                        out,
                        "<Deleted><Key>{key}</Key><DeleteMarker>{delete_marker}</DeleteMarker></Deleted>"
                    );
                }
                out.push_str("</DeleteResult>");
                out.into_bytes()
            })
    }

    fn prefix_query() -> mockito::Matcher {
        mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
            mockito::Matcher::UrlEncoded("prefix".into(), "integration/18/".into()),
        ])
    }

    fn recursive_action(key: &str) -> Action {
        Action::DeleteObject {
            key: key.to_string(),
            upload_id: String::new(),
            bucket: false,
            older_than: None,
            recursive: true,
            targets: Vec::new(),
            version_id: None,
            bypass_governance: false,
        }
    }

    #[tokio::test]
    async fn test_prefix_delete_lists_literal_prefix_and_never_deletes_bucket() {
        let mut server = mockito::Server::new_async().await;
        let all = vec![
            "integration/18/".to_string(),
            "integration/18/file1.rpm".to_string(),
            "integration/18/el9/file2.rpm".to_string(),
        ];
        // Only matches when the prefix query is exactly `integration/18/`.
        let list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&all, false, None))
            .expect(1)
            .create_async()
            .await;
        let delete = confirm_all(&mut server, false)
            .match_body(mockito::Matcher::Regex(
                "<Key>integration/18/</Key>.*<Key>integration/18/file1.rpm</Key>.*<Key>integration/18/el9/file2.rpm</Key>".into(),
            ))
            .expect(1)
            .create_async()
            .await;
        let bucket_delete = server
            .mock("DELETE", "/bucket")
            .expect(0)
            .create_async()
            .await;

        handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap();

        list.assert_async().await;
        delete.assert_async().await;
        bucket_delete.assert_async().await;
    }

    #[tokio::test]
    async fn test_prefix_delete_empty_listing_is_noop() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&[], false, None))
            .create_async()
            .await;
        let delete = confirm_all(&mut server, false)
            .expect(0)
            .create_async()
            .await;
        let bucket_delete = server
            .mock("DELETE", "/bucket")
            .expect(0)
            .create_async()
            .await;

        handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap();

        delete.assert_async().await;
        bucket_delete.assert_async().await;
    }

    #[tokio::test]
    async fn test_prefix_delete_paginates_and_batches_over_1000() {
        let mut server = mockito::Server::new_async().await;
        // Page 1: 1,001 keys in one response (server ignoring max-keys) must be
        // split into 1,000 + 1. Page 2 via continuation token: 5 keys.
        let _page1 = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..1_001), true, Some("tok-2")))
            .expect(1)
            .create_async()
            .await;
        let page2 = server
            .mock("GET", "/bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("continuation-token".into(), "tok-2".into()),
                mockito::Matcher::UrlEncoded("prefix".into(), "integration/18/".into()),
            ]))
            .with_status(200)
            .with_body(list_page(&keys(1_001..1_006), false, None))
            .expect(1)
            .create_async()
            .await;
        // 1,000 + 1 from page 1, then 5 from page 2 => exactly three delete calls.
        let delete = confirm_all(&mut server, false)
            .expect(3)
            .create_async()
            .await;

        let mut stats = PrefixDeleteStats::default();
        delete_prefix_pages(&mock_s3(server.url()), "integration/18/", false, &mut stats)
            .await
            .unwrap();

        page2.assert_async().await;
        delete.assert_async().await;
        assert_eq!(stats.removed, 1_006);
        assert_eq!(stats.delete_markers, 0);
    }

    #[tokio::test]
    async fn test_prefix_delete_counts_delete_markers() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..3), false, None))
            .create_async()
            .await;
        let _delete = confirm_all(&mut server, true).create_async().await;

        let mut stats = PrefixDeleteStats::default();
        delete_prefix_pages(&mock_s3(server.url()), "integration/18/", false, &mut stats)
            .await
            .unwrap();

        assert_eq!(stats.removed, 3);
        assert_eq!(stats.delete_markers, 3);
    }

    #[tokio::test]
    async fn test_prefix_delete_listing_failure_propagates_without_deleting() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(mockito::Matcher::Any)
            .with_status(500)
            .with_body("<Error><Code>InternalError</Code><Message>boom</Message></Error>")
            .create_async()
            .await;
        let delete = confirm_all(&mut server, false)
            .expect(0)
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("incomplete"), "{err}");
        delete.assert_async().await;
    }

    #[tokio::test]
    async fn test_prefix_delete_second_page_listing_failure_is_reported_as_incomplete() {
        let mut server = mockito::Server::new_async().await;
        let _page1 = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..2), true, Some("tok-2")))
            .create_async()
            .await;
        let _page2 = server
            .mock("GET", "/bucket")
            .match_query(mockito::Matcher::UrlEncoded(
                "continuation-token".into(),
                "tok-2".into(),
            ))
            .with_status(500)
            .with_body("<Error><Code>InternalError</Code><Message>boom</Message></Error>")
            .create_async()
            .await;
        let _delete = confirm_all(&mut server, false).create_async().await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("incomplete after removing 2 objects"), "{err}");
    }

    #[tokio::test]
    async fn test_prefix_delete_request_failure_propagates() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..2), false, None))
            .create_async()
            .await;
        let _delete = server
            .mock("POST", "/bucket")
            .match_query(mockito::Matcher::UrlEncoded("delete".into(), String::new()))
            .with_status(500)
            .with_body("<Error><Code>InternalError</Code><Message>boom</Message></Error>")
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("incomplete after removing 0 objects"), "{err}");
    }

    #[tokio::test]
    async fn test_prefix_delete_per_object_error_propagates() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..2), false, None))
            .create_async()
            .await;
        let _delete = server
            .mock("POST", "/bucket")
            .match_query(mockito::Matcher::UrlEncoded("delete".into(), String::new()))
            .with_status(200)
            .with_body(
                "<DeleteResult><Deleted><Key>integration/18/k-0000</Key></Deleted><Error><Key>integration/18/k-0001</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error></DeleteResult>",
            )
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("incomplete after removing 1 object:"), "{err}");
        assert!(
            err.contains("integration/18/k-0001: Access Denied (AccessDenied)"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_prefix_delete_refuses_unconfirmed_keys() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(prefix_query())
            .with_status(200)
            .with_body(list_page(&keys(0..2), false, None))
            .create_async()
            .await;
        let _delete = server
            .mock("POST", "/bucket")
            .match_query(mockito::Matcher::UrlEncoded("delete".into(), String::new()))
            .with_status(200)
            .with_body(
                "<DeleteResult><Deleted><Key>integration/18/k-0000</Key></Deleted></DeleteResult>",
            )
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("confirmed 1 of 2"), "{err}");
        assert!(err.contains("incomplete after removing 1 object"), "{err}");
    }

    #[tokio::test]
    async fn test_prefix_delete_rejects_keys_outside_prefix() {
        let mut server = mockito::Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(list_page(
                &[
                    "integration/18/ok".to_string(),
                    "integration/180/other".to_string(),
                ],
                false,
                None,
            ))
            .create_async()
            .await;
        let delete = confirm_all(&mut server, false)
            .expect(0)
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action("integration/18/"))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("outside prefix"), "{err}");
        delete.assert_async().await;
    }

    #[test]
    fn test_missing_trailing_slash_warning() {
        assert_eq!(missing_trailing_slash_warning("integration/18/"), None);
        let warning = missing_trailing_slash_warning("integration/18").unwrap();
        assert!(warning.contains("'integration/18' is matched literally"));
        assert!(warning.contains("add a trailing '/'"));
    }

    #[tokio::test]
    async fn test_prefix_delete_refuses_empty_prefix_without_requests() {
        let mut server = mockito::Server::new_async().await;
        let any = server
            .mock("GET", "/bucket")
            .match_query(mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let err = handle(&mock_s3(server.url()), recursive_action(""))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("empty prefix"), "{err}");
        any.assert_async().await;
    }
}
