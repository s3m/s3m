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
    } = action
    {
        if bucket {
            if recursive {
                delete_bucket_recursive(s3).await?;
            } else {
                let action = actions::DeleteBucket::new();
                action.request(s3).await?;
            }
        } else if upload_id.is_empty() {
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

                delete_matched_objects(s3, &matched).await?;
                print_filtered_delete_summary(matched.len(), matched.len());
                return Ok(());
            }

            if count_delete_targets(&targets) <= 1 {
                let (target_s3, target_key) = single_delete_target(s3, &key, &targets)?;
                let action = actions::DeleteObject::new(target_key);
                action.request(target_s3).await?;
            } else {
                delete_groups(&targets).await?;
            }
        } else {
            let target_s3 = targets.first().map_or(s3, |group| &group.s3);
            let action = actions::AbortMultipartUpload::new(&key, &upload_id);
            action.request(target_s3).await?;
        }
    }

    Ok(())
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

async fn delete_groups(groups: &[DeleteGroup]) -> Result<()> {
    for group in groups {
        for batch in split_delete_batches(&group.objects) {
            let result = actions::DeleteObjects::new(batch, true)
                .request(&group.s3)
                .await?;

            if !result.errors.is_empty() {
                return Err(anyhow!(format_delete_objects_errors(&result.errors)));
            }
        }
    }

    Ok(())
}

async fn delete_matched_objects(s3: &S3, objects: &[actions::ObjectIdentifier]) -> Result<()> {
    match objects {
        [] => Ok(()),
        [object] => {
            let action = actions::DeleteObject::new(&object.key);
            action.request(s3).await?;
            Ok(())
        }
        _ => {
            for batch in split_delete_batches(objects) {
                let result = actions::DeleteObjects::new(batch, true).request(s3).await?;

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

async fn delete_bucket_recursive(s3: &S3) -> Result<()> {
    loop {
        let action = actions::ListObjectsV2::new(
            None,
            None,
            Some(actions::DeleteObjects::MAX_OBJECTS.to_string()),
        );
        let objects = action.request(s3).await?;

        if objects.contents.is_empty() {
            if objects.is_truncated {
                return Err(anyhow!(
                    "ListObjectsV2 returned a truncated response without object contents"
                ));
            }

            break;
        }

        let batch_objects: Vec<actions::ObjectIdentifier> = objects
            .contents
            .into_iter()
            .map(|object| actions::ObjectIdentifier {
                key: object.key,
                version_id: None,
            })
            .collect();

        for batch in split_delete_batches(&batch_objects) {
            let result = actions::DeleteObjects::new(batch, true).request(s3).await?;

            if !result.errors.is_empty() {
                return Err(anyhow!(format_delete_objects_errors(&result.errors)));
            }
        }
    }

    // TODO: support versioned bucket cleanup by listing object versions and delete markers,
    // then populating ObjectIdentifier::version_id for DeleteObjects requests.
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
}
