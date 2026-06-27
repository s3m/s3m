use crate::{
    cli::actions::{Action, ObjectLockSetTarget},
    s3::{
        Error, S3,
        actions::{
            GetObjectLegalHold, GetObjectLockConfiguration, GetObjectRetention, PutObjectLegalHold,
            PutObjectLockConfiguration, PutObjectRetention,
        },
        responses::{ObjectLegalHold, ObjectLockConfiguration, ObjectRetention},
    },
};
use anyhow::Result;
use serde_json::json;

/// `true` when the error means "no Object Lock setting is present" rather than a
/// real failure. The S3 API signals this with a 404; `MinIO` returns HTTP 400
/// carrying `NoSuchObjectLockConfiguration` for a missing object or bucket
/// retention / legal hold.
fn is_unset(e: &Error) -> bool {
    e.is_not_found()
        || matches!(
            e.code(),
            Some("NoSuchObjectLockConfiguration" | "ObjectLockConfigurationNotFoundError")
        )
}

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    match action {
        Action::ObjectLockGet {
            key,
            version_id,
            json,
        } => handle_get(s3, key, version_id, json).await,
        Action::ObjectLockSet(target) => handle_set(s3, target).await,
        _ => Ok(()),
    }
}

async fn handle_get(
    s3: &S3,
    key: Option<String>,
    version_id: Option<String>,
    json: bool,
) -> Result<()> {
    match key {
        None => print_bucket_config(s3, json).await,
        Some(key) => print_object_lock(s3, &key, version_id, json).await,
    }
}

async fn print_bucket_config(s3: &S3, json: bool) -> Result<()> {
    // A bucket without Object Lock enabled returns a 404-style error.
    let config = match GetObjectLockConfiguration::new().request(s3).await {
        Ok(config) => Some(config),
        Err(e) if is_unset(&e) => None,
        Err(e) => return Err(e.into()),
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&bucket_json(config.as_ref()))?
        );
        return Ok(());
    }

    match config.as_ref().and_then(default_retention_fields) {
        Some((mode, days, years)) => {
            println!("Object Lock: Enabled");
            println!("Default retention:");
            println!("  mode: {mode}");
            if let Some(days) = days {
                println!("  days: {days}");
            }
            if let Some(years) = years {
                println!("  years: {years}");
            }
        }
        None => match config {
            Some(_) => println!("Object Lock: Enabled (no default retention rule)"),
            None => println!("Object Lock: not enabled on this bucket"),
        },
    }

    Ok(())
}

async fn print_object_lock(
    s3: &S3,
    key: &str,
    version_id: Option<String>,
    json: bool,
) -> Result<()> {
    let retention = match GetObjectRetention::new(key, version_id.clone())
        .request(s3)
        .await
    {
        Ok(r) => Some(r),
        Err(e) if is_unset(&e) => None,
        Err(e) => return Err(e.into()),
    };

    let legal_hold = match GetObjectLegalHold::new(key, version_id).request(s3).await {
        Ok(h) => Some(h),
        Err(e) if is_unset(&e) => None,
        Err(e) => return Err(e.into()),
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&object_json(
                key,
                retention.as_ref(),
                legal_hold.as_ref()
            ))?
        );
        return Ok(());
    }

    match &retention {
        Some(ObjectRetention {
            mode: Some(mode),
            retain_until_date,
        }) => {
            println!("Retention:");
            println!("  mode: {mode}");
            if let Some(date) = retain_until_date {
                println!("  retain until: {date}");
            }
        }
        _ => println!("Retention: none"),
    }

    let status = legal_hold
        .as_ref()
        .and_then(|h| h.status.as_deref())
        .unwrap_or("OFF");
    println!("Legal hold: {status}");

    Ok(())
}

async fn handle_set(s3: &S3, target: ObjectLockSetTarget) -> Result<()> {
    match target {
        ObjectLockSetTarget::BucketDefault { mode, days, years } => {
            PutObjectLockConfiguration::new(mode, days, years)
                .request(s3)
                .await?;
            println!("bucket default retention set: mode={}", mode.as_amz());
        }
        ObjectLockSetTarget::Object {
            key,
            retention,
            legal_hold,
            version_id,
            bypass_governance,
        } => {
            if let Some((mode, retain_until)) = retention {
                PutObjectRetention::new(
                    &key,
                    mode,
                    retain_until.clone(),
                    version_id.clone(),
                    bypass_governance,
                )
                .request(s3)
                .await?;
                println!("retention set: mode={} until={retain_until}", mode.as_amz());
            }

            if let Some(enabled) = legal_hold {
                PutObjectLegalHold::new(&key, enabled, version_id)
                    .request(s3)
                    .await?;
                println!("legal hold: {}", if enabled { "ON" } else { "OFF" });
            }
        }
    }

    Ok(())
}

fn default_retention_fields(
    config: &ObjectLockConfiguration,
) -> Option<(&str, Option<u32>, Option<u32>)> {
    let dr = config.rule.as_ref()?.default_retention.as_ref()?;
    let mode = dr.mode.as_deref()?;
    Some((mode, dr.days, dr.years))
}

fn bucket_json(config: Option<&ObjectLockConfiguration>) -> serde_json::Value {
    match config {
        None => json!({ "kind": "object-lock", "enabled": false }),
        Some(config) => {
            let default_retention = default_retention_fields(config)
                .map(|(mode, days, years)| json!({ "mode": mode, "days": days, "years": years }));
            json!({
                "kind": "object-lock",
                "enabled": true,
                "default_retention": default_retention,
            })
        }
    }
}

fn object_json(
    key: &str,
    retention: Option<&ObjectRetention>,
    legal_hold: Option<&ObjectLegalHold>,
) -> serde_json::Value {
    let retention = retention.and_then(|r| {
        r.mode
            .as_ref()
            .map(|mode| json!({ "mode": mode, "retain_until_date": r.retain_until_date }))
    });
    let legal_hold = legal_hold
        .and_then(|h| h.status.clone())
        .unwrap_or_else(|| "OFF".to_string());

    json!({
        "kind": "object-lock",
        "key": key,
        "retention": retention,
        "legal_hold": legal_hold,
    })
}
