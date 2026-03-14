use crate::{
    cli::actions::{Action, DuGroupBy},
    s3::{S3, actions, responses::ListBucketResult},
};
use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use chrono::{DateTime, NaiveDate, Utc};
use colored::Colorize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageSummary {
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyUsage {
    pub day: NaiveDate,
    pub summary: UsageSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageReport {
    Total(UsageSummary),
    ByDay {
        days: Vec<DailyUsage>,
        total: UsageSummary,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct UsageSummaryJson {
    object_count: u64,
    bytes: u64,
    human_size: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct DailyUsageJson {
    date: String,
    object_count: u64,
    bytes: u64,
    human_size: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
enum UsageReportJson {
    Total {
        target: String,
        group_by: Option<String>,
        summary: UsageSummaryJson,
    },
    ByDay {
        target: String,
        group_by: Option<String>,
        days: Vec<DailyUsageJson>,
        total: UsageSummaryJson,
    },
}

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::DiskUsage {
        group_by,
        json,
        prefix,
        target,
    } = action
    {
        let report = summarize_report(s3, prefix, group_by).await?;
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&report_json(&target, &report))?
            );
        } else {
            print!("{}", format_report(&target, &report));
        }
    }

    Ok(())
}

/// # Errors
/// Will return an error if the bucket usage summary can not be computed
pub async fn summarize(s3: &S3, prefix: Option<String>) -> Result<UsageSummary> {
    match summarize_report(s3, prefix, None).await? {
        UsageReport::Total(summary) => Ok(summary),
        UsageReport::ByDay { total, .. } => Ok(total),
    }
}

/// # Errors
/// Will return an error if the bucket usage report can not be computed
pub async fn summarize_report(
    s3: &S3,
    prefix: Option<String>,
    group_by: Option<DuGroupBy>,
) -> Result<UsageReport> {
    let mut continuation_token: Option<String> = None;
    let mut total = UsageSummary {
        objects: 0,
        bytes: 0,
    };
    let mut per_day: BTreeMap<NaiveDate, UsageSummary> = BTreeMap::new();

    loop {
        let page = request_page(s3, prefix.clone(), continuation_token.clone()).await?;
        update_summary(&mut total, &page);

        if group_by == Some(DuGroupBy::Day) {
            update_daily_summary(&mut per_day, &page)?;
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

    if group_by == Some(DuGroupBy::Day) {
        Ok(UsageReport::ByDay {
            days: per_day
                .into_iter()
                .map(|(day, summary)| DailyUsage { day, summary })
                .collect(),
            total,
        })
    } else {
        Ok(UsageReport::Total(total))
    }
}

async fn request_page(
    s3: &S3,
    prefix: Option<String>,
    continuation_token: Option<String>,
) -> Result<ListBucketResult> {
    let mut action = actions::ListObjectsV2::new(prefix, None, None);
    action.continuation_token = continuation_token;
    action.request(s3).await
}

fn update_summary(summary: &mut UsageSummary, page: &ListBucketResult) {
    for object in &page.contents {
        summary.objects += 1;
        summary.bytes += object.size;
    }
}

/// Grouping is based on each object's `LastModified` converted to a UTC calendar date.
fn update_daily_summary(
    per_day: &mut BTreeMap<NaiveDate, UsageSummary>,
    page: &ListBucketResult,
) -> Result<()> {
    for object in &page.contents {
        let day = parse_last_modified_day(&object.last_modified)?;
        let summary = per_day.entry(day).or_insert(UsageSummary {
            objects: 0,
            bytes: 0,
        });
        summary.objects += 1;
        summary.bytes += object.size;
    }

    Ok(())
}

fn parse_last_modified_day(last_modified: &str) -> Result<NaiveDate> {
    let timestamp = DateTime::parse_from_rfc3339(last_modified)?;
    Ok(timestamp.with_timezone(&Utc).date_naive())
}

fn format_report(target: &str, report: &UsageReport) -> String {
    match report {
        UsageReport::Total(summary) => format_summary(target, *summary),
        UsageReport::ByDay { days, total } => format_grouped_summary(target, days, *total),
    }
}

fn format_summary(target: &str, summary: UsageSummary) -> String {
    format!(
        "{target}: {} {}, {} ({} B)\n",
        summary.objects,
        object_label(summary.objects),
        ByteSize(summary.bytes),
        summary.bytes
    )
}

fn format_grouped_summary(target: &str, days: &[DailyUsage], total: UsageSummary) -> String {
    let mut lines = vec![
        format!("{target} grouped by UTC day:"),
        format_grouped_header(),
    ];

    for daily in days {
        lines.push(format_grouped_row(
            &daily.day.to_string(),
            daily.summary.objects,
            daily.summary.bytes,
        ));
    }

    lines.push(format_grouped_row("TOTAL", total.objects, total.bytes));
    lines.join("\n") + "\n"
}

fn format_grouped_header() -> String {
    format!(
        "{:<10} {:>8} {:>10} {:>12}",
        "DATE", "OBJECTS", "SIZE", "RAW_BYTES"
    )
    .bold()
    .to_string()
}

fn format_grouped_row(date: &str, objects: u64, bytes: u64) -> String {
    format!(
        "{:<10} {:>8} {:>10} {:>12}",
        date,
        objects,
        ByteSize(bytes),
        format!("{bytes} B")
    )
}

fn object_label(objects: u64) -> &'static str {
    if objects == 1 { "object" } else { "objects" }
}

fn summary_json(summary: UsageSummary) -> UsageSummaryJson {
    UsageSummaryJson {
        object_count: summary.objects,
        bytes: summary.bytes,
        human_size: ByteSize(summary.bytes).to_string(),
    }
}

fn report_json(target: &str, report: &UsageReport) -> UsageReportJson {
    match report {
        UsageReport::Total(summary) => UsageReportJson::Total {
            target: target.to_string(),
            group_by: None,
            summary: summary_json(*summary),
        },
        UsageReport::ByDay { days, total } => UsageReportJson::ByDay {
            target: target.to_string(),
            group_by: Some("day".to_string()),
            days: days
                .iter()
                .map(|daily| DailyUsageJson {
                    date: daily.day.to_string(),
                    object_count: daily.summary.objects,
                    bytes: daily.summary.bytes,
                    human_size: ByteSize(daily.summary.bytes).to_string(),
                })
                .collect(),
            total: summary_json(*total),
        },
    }
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
    use crate::s3::{Credentials, Region, S3};
    use mockito::{Matcher, Server};
    use secrecy::SecretString;

    #[test]
    fn test_format_summary_plural() {
        let rendered = format_summary(
            "s3/my-bucket",
            UsageSummary {
                objects: 2,
                bytes: 2_048,
            },
        );

        assert_eq!(rendered, "s3/my-bucket: 2 objects, 2.0 KiB (2048 B)\n");
    }

    #[test]
    fn test_format_summary_singular() {
        let rendered = format_summary(
            "s3/my-bucket/prefix",
            UsageSummary {
                objects: 1,
                bytes: 1,
            },
        );

        assert_eq!(rendered, "s3/my-bucket/prefix: 1 object, 1 B (1 B)\n");
    }

    #[test]
    fn test_format_grouped_summary_sorted_with_total() {
        let rendered = format_grouped_summary(
            "s3/my-bucket",
            &[
                DailyUsage {
                    day: NaiveDate::from_ymd_opt(2026, 3, 13).unwrap(),
                    summary: UsageSummary {
                        objects: 2,
                        bytes: 20,
                    },
                },
                DailyUsage {
                    day: NaiveDate::from_ymd_opt(2026, 3, 14).unwrap(),
                    summary: UsageSummary {
                        objects: 1,
                        bytes: 5,
                    },
                },
            ],
            UsageSummary {
                objects: 3,
                bytes: 25,
            },
        );

        assert!(rendered.contains("s3/my-bucket grouped by UTC day:"));
        assert!(rendered.contains("DATE"));
        assert!(rendered.contains("2026-03-13"));
        assert!(rendered.contains("2026-03-14"));
        assert!(rendered.contains("TOTAL"));
    }

    #[test]
    fn test_parse_last_modified_day_uses_utc() {
        let day = parse_last_modified_day("2026-03-14T01:30:00+02:00").unwrap();
        assert_eq!(day, NaiveDate::from_ymd_opt(2026, 3, 13).unwrap());
    }

    #[test]
    fn test_report_json_total_shape() {
        let rendered = serde_json::to_value(report_json(
            "s3/my-bucket",
            &UsageReport::Total(UsageSummary {
                objects: 2,
                bytes: 2048,
            }),
        ))
        .unwrap();

        assert_eq!(rendered["target"], "s3/my-bucket");
        assert_eq!(rendered["summary"]["object_count"], 2);
        assert_eq!(rendered["summary"]["bytes"], 2048);
    }

    #[test]
    fn test_report_json_grouped_shape() {
        let rendered = serde_json::to_value(report_json(
            "s3/my-bucket",
            &UsageReport::ByDay {
                days: vec![DailyUsage {
                    day: NaiveDate::from_ymd_opt(2026, 3, 14).unwrap(),
                    summary: UsageSummary {
                        objects: 1,
                        bytes: 5,
                    },
                }],
                total: UsageSummary {
                    objects: 1,
                    bytes: 5,
                },
            },
        ))
        .unwrap();

        assert_eq!(rendered["group_by"], "day");
        assert_eq!(rendered["days"][0]["date"], "2026-03-14");
        assert_eq!(rendered["total"]["object_count"], 1);
    }

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

    #[tokio::test]
    async fn test_handle_json_total_branch() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2026-03-14T00:00:00.000Z</LastModified><ETag>"etag"</ETag><Size>5</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"#,
            )
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::DiskUsage {
                group_by: None,
                json: true,
                prefix: None,
                target: "s3/bucket".to_string(),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_handle_json_grouped_branch() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2026-03-14T00:00:00.000Z</LastModified><ETag>"etag"</ETag><Size>5</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"#,
            )
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::DiskUsage {
                group_by: Some(DuGroupBy::Day),
                json: true,
                prefix: None,
                target: "s3/bucket".to_string(),
            },
        )
        .await
        .unwrap();
    }
}
