use crate::{
    cli::{Host, actions::Action, age_filter::parse_last_modified, config::MonitorRule},
    s3::{S3, actions},
};
use anyhow::{Context, Result, anyhow};
use chrono::{Duration, Utc};
use futures::stream::{self, StreamExt};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MonitorOutputFormat {
    #[default]
    Prometheus,
    Influxdb,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckResult {
    host: String,
    bucket: String,
    prefix: String,
    suffix: String,
    exist: bool,
    error: bool,
    size_mismatch: bool,
}

#[derive(Debug, Clone)]
pub struct MonitorCheck {
    host: String,
    bucket: String,
    rule: MonitorRule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuleStats {
    exists: bool,
    any_large_enough: bool,
}

/// # Errors
/// Will return an error if the monitor action can not be executed
pub async fn handle(s3: &S3, action: Action) -> Result<()> {
    if let Action::Monitor {
        host: _,
        checks,
        format,
        exit_on_check_failure,
        number,
    } = action
    {
        let base_s3 = s3.clone();
        let results = stream::iter(checks)
            .map(move |check| {
                let base_s3 = base_s3.clone();
                async move { run_check(base_s3, check).await }
            })
            .buffer_unordered(usize::from(number.max(1)))
            .collect::<Vec<_>>()
            .await;

        let output = match format {
            MonitorOutputFormat::Prometheus => format_prometheus(&results),
            MonitorOutputFormat::Influxdb => format_influxdb(&results),
        };

        print!("{output}");

        if exit_on_check_failure && has_check_failures(&results) {
            return Err(anyhow!("one or more monitor checks failed"));
        }
    }

    Ok(())
}

/// # Errors
/// Will return an error if the host has no monitor buckets, a bucket has no rules,
/// or the host monitor rules are invalid.
pub fn prepare_checks(host: &str, host_config: &Host) -> Result<Vec<MonitorCheck>> {
    if host_config.buckets.is_empty() {
        return Err(anyhow!("No monitor buckets configured for host: {host}"));
    }

    let mut checks = Vec::new();

    for (bucket, rules) in &host_config.buckets {
        if rules.is_empty() {
            return Err(anyhow!(
                "No monitor rules configured for host '{host}' bucket '{bucket}'"
            ));
        }

        for rule in rules {
            checks.push(MonitorCheck {
                host: host.to_string(),
                bucket: bucket.clone(),
                rule: rule.clone(),
            });
        }
    }

    Ok(checks)
}

async fn run_check(base_s3: S3, check: MonitorCheck) -> CheckResult {
    let MonitorCheck { host, bucket, rule } = check;
    let s3 = base_s3.with_bucket(Some(bucket.clone()));

    let mut exist = false;
    let mut size_mismatch = false;
    let mut error = false;

    match evaluate_rule(&s3, &rule).await {
        Ok(stats) => {
            exist = stats.exists;
            if exist && rule.size > 0 {
                size_mismatch = !stats.any_large_enough;
            }
        }
        Err(err) => {
            log::error!(
                "Error checking host='{host}' bucket='{bucket}' prefix='{}': {err}",
                rule.prefix
            );
            error = true;
        }
    }

    CheckResult {
        host,
        bucket,
        prefix: rule.prefix,
        suffix: rule.suffix,
        exist,
        error,
        size_mismatch,
    }
}

async fn evaluate_rule(s3: &S3, rule: &MonitorRule) -> Result<RuleStats> {
    let now = Utc::now();
    let age_seconds =
        i64::try_from(rule.age).context("monitor rule age exceeds the supported range")?;
    let oldest_allowed = now - Duration::seconds(age_seconds);
    let mut continuation_token: Option<String> = None;
    let mut exists = false;
    let mut any_large_enough = false;

    loop {
        let mut action = actions::ListObjectsV2::new(Some(rule.prefix.clone()), None, None);
        action.continuation_token = continuation_token.clone();
        let page = action.request(s3).await?;

        for object in page.contents {
            if !rule.suffix.is_empty() && !object.key.ends_with(&rule.suffix) {
                continue;
            }

            let last_modified = parse_last_modified(&object)?;
            if last_modified < oldest_allowed {
                continue;
            }

            exists = true;
            if rule.size == 0 || object.size >= rule.size {
                any_large_enough = true;
                return Ok(RuleStats {
                    exists,
                    any_large_enough,
                });
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

    Ok(RuleStats {
        exists,
        any_large_enough,
    })
}

fn has_check_failures(results: &[CheckResult]) -> bool {
    results
        .iter()
        .any(|result| result.error || !result.exist || result.size_mismatch)
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_tag(value: &str) -> String {
    value
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

fn prometheus_labels(result: &CheckResult) -> String {
    if result.suffix.is_empty() {
        format!(
            "host=\"{}\",bucket=\"{}\",prefix=\"{}\"",
            escape_label(&result.host),
            escape_label(&result.bucket),
            escape_label(&result.prefix),
        )
    } else {
        format!(
            "host=\"{}\",bucket=\"{}\",prefix=\"{}\",suffix=\"{}\"",
            escape_label(&result.host),
            escape_label(&result.bucket),
            escape_label(&result.prefix),
            escape_label(&result.suffix),
        )
    }
}

fn influx_tags(result: &CheckResult) -> String {
    if result.suffix.is_empty() {
        format!(
            "host={},bucket={},prefix={}",
            escape_tag(&result.host),
            escape_tag(&result.bucket),
            escape_tag(&result.prefix),
        )
    } else {
        format!(
            "host={},bucket={},prefix={},suffix={}",
            escape_tag(&result.host),
            escape_tag(&result.bucket),
            escape_tag(&result.prefix),
            escape_tag(&result.suffix),
        )
    }
}

fn format_prometheus(results: &[CheckResult]) -> String {
    let mut sorted: Vec<&CheckResult> = results.iter().collect();
    sorted.sort_by(|a, b| {
        a.host
            .cmp(&b.host)
            .then(a.bucket.cmp(&b.bucket))
            .then(a.prefix.cmp(&b.prefix))
            .then(a.suffix.cmp(&b.suffix))
    });

    let mut output = String::new();

    output.push_str("# HELP s3m_object_exists Object exists within the configured age window\n");
    output.push_str("# TYPE s3m_object_exists gauge\n");
    for result in &sorted {
        let _ = writeln!(
            output,
            "s3m_object_exists{{{}}} {}",
            prometheus_labels(result),
            i32::from(result.exist),
        );
    }

    output.push_str("# HELP s3m_check_error S3 API call failed\n");
    output.push_str("# TYPE s3m_check_error gauge\n");
    for result in &sorted {
        let _ = writeln!(
            output,
            "s3m_check_error{{{}}} {}",
            prometheus_labels(result),
            i32::from(result.error),
        );
    }

    output.push_str("# HELP s3m_size_mismatch Object size is below the configured minimum\n");
    output.push_str("# TYPE s3m_size_mismatch gauge\n");
    for result in &sorted {
        let _ = writeln!(
            output,
            "s3m_size_mismatch{{{}}} {}",
            prometheus_labels(result),
            i32::from(result.size_mismatch),
        );
    }

    output
}

fn format_influxdb(results: &[CheckResult]) -> String {
    let mut sorted: Vec<&CheckResult> = results.iter().collect();
    sorted.sort_by(|a, b| {
        a.host
            .cmp(&b.host)
            .then(a.bucket.cmp(&b.bucket))
            .then(a.prefix.cmp(&b.prefix))
            .then(a.suffix.cmp(&b.suffix))
    });

    let mut lines: Vec<String> = sorted
        .iter()
        .map(|result| {
            format!(
                "s3m,{} error={}i,exist={}i,size_mismatch={}i",
                influx_tags(result),
                i32::from(result.error),
                i32::from(result.exist),
                i32::from(result.size_mismatch),
            )
        })
        .collect();
    lines.push(String::new());
    lines.join("\n")
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
    use chrono::SecondsFormat;
    use mockito::{Matcher, Server};
    use secrecy::SecretString;

    fn test_s3(endpoint: String, bucket: Option<&str>) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &Region::custom("us-west-1", endpoint),
            bucket.map(str::to_string),
            false,
        )
    }

    fn list_objects_xml(bucket: &str, prefix: &str, entries: &[(&str, u64, &str)]) -> String {
        list_objects_xml_page(bucket, prefix, entries, false, None)
    }

    fn list_objects_xml_page(
        bucket: &str,
        prefix: &str,
        entries: &[(&str, u64, &str)],
        is_truncated: bool,
        next_continuation_token: Option<&str>,
    ) -> String {
        let mut contents = String::new();
        for (key, size, last_modified) in entries {
            let _ = write!(
                contents,
                "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
            );
        }

        let next = next_continuation_token.map_or_else(String::new, |token| {
            format!("<NextContinuationToken>{token}</NextContinuationToken>")
        });

        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>{bucket}</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{next}{contents}</ListBucketResult>"#
        )
    }

    fn error_xml(code: &str, message: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{message}</Message><RequestId>request-id</RequestId></Error>"#
        )
    }

    #[tokio::test]
    async fn test_evaluate_rule_fresh_object_matches_size() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml(
                "bucket",
                "logs/",
                &[("logs/app.log", 4_096, &now)],
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(stats.exists);
        assert!(stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_detects_size_mismatch() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml(
                "bucket",
                "logs/",
                &[("logs/app.log", 512, &now)],
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(stats.exists);
        assert!(!stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_ignores_old_objects() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml(
                "bucket",
                "logs/",
                &[("logs/app.log", 4_096, "2019-10-14T08:52:23.231Z")],
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 60,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(!stats.exists);
        assert!(!stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_ignores_non_matching_suffix() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml(
                "bucket",
                "logs/",
                &[("logs/app.txt", 4_096, &now)],
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(!stats.exists);
        assert!(!stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_mixed_sizes_one_large_enough() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml(
                "bucket",
                "logs/",
                &[
                    ("logs/small.log", 500, &now),
                    ("logs/large.log", 2_000, &now),
                ],
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(stats.exists);
        assert!(stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_stops_after_first_sufficient_match() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _page1 = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml_page(
                "bucket",
                "logs/",
                &[("logs/app.log", 4_096, &now)],
                true,
                Some("next-page"),
            ))
            .expect(1)
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(stats.exists);
        assert!(stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_evaluate_rule_continues_across_pages_until_size_match() {
        let mut server = Server::new_async().await;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let _page1 = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml_page(
                "bucket",
                "logs/",
                &[("logs/app.log", 512, &now)],
                true,
                Some("next-page"),
            ))
            .create_async()
            .await;
        let _page2 = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
                Matcher::UrlEncoded("continuation-token".into(), "next-page".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(list_objects_xml_page(
                "bucket",
                "logs/",
                &[("logs/app-2.log", 4_096, &now)],
                false,
                None,
            ))
            .create_async()
            .await;

        let stats = evaluate_rule(
            &test_s3(server.url(), Some("bucket")),
            &MonitorRule {
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                age: 86_400,
                size: 1_024,
            },
        )
        .await
        .unwrap();

        assert!(stats.exists);
        assert!(stats.any_large_enough);
    }

    #[tokio::test]
    async fn test_run_check_marks_error_on_failed_listing() {
        let mut server = Server::new_async().await;
        let _list = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("list-type".into(), "2".into()),
                Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            ]))
            .with_status(404)
            .with_header("content-type", "application/xml")
            .with_body(error_xml(
                "NoSuchBucket",
                "The specified bucket does not exist",
            ))
            .create_async()
            .await;

        let result = run_check(
            test_s3(server.url(), None),
            MonitorCheck {
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                rule: MonitorRule {
                    prefix: "logs/".to_string(),
                    suffix: String::new(),
                    age: 86_400,
                    size: 512,
                },
            },
        )
        .await;

        assert!(!result.exist);
        assert!(result.error);
        assert!(!result.size_mismatch);
    }

    #[test]
    fn test_format_prometheus_includes_host_and_suffix() {
        let output = format_prometheus(&[
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket-b".to_string(),
                prefix: "logs/".to_string(),
                suffix: String::new(),
                exist: false,
                error: true,
                size_mismatch: false,
            },
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket-a".to_string(),
                prefix: "daily/".to_string(),
                suffix: ".log".to_string(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
        ]);

        assert!(output.contains("# HELP s3m_object_exists"));
        assert!(output.contains(
            "s3m_object_exists{host=\"s3\",bucket=\"bucket-a\",prefix=\"daily/\",suffix=\".log\"} 1"
        ));
        assert!(
            output.contains("s3m_check_error{host=\"s3\",bucket=\"bucket-b\",prefix=\"logs/\"} 1")
        );
    }

    #[test]
    fn test_prometheus_sorted_and_grouped() {
        let output = format_prometheus(&[
            CheckResult {
                host: "host_B".to_string(),
                bucket: "bucket_B".to_string(),
                prefix: "foo/".to_string(),
                suffix: String::new(),
                exist: false,
                error: true,
                size_mismatch: false,
            },
            CheckResult {
                host: "host_A".to_string(),
                bucket: "bucket_A".to_string(),
                prefix: "test/".to_string(),
                suffix: String::new(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
        ]);
        let lines: Vec<&str> = output.lines().collect();
        let exists_a = lines
            .iter()
            .find(|line| line.contains("s3m_object_exists") && line.contains("host_A"))
            .copied();
        let exists_b = lines
            .iter()
            .find(|line| line.contains("s3m_object_exists") && line.contains("host_B"))
            .copied();
        assert_eq!(
            exists_a,
            Some(r#"s3m_object_exists{host="host_A",bucket="bucket_A",prefix="test/"} 1"#)
        );
        assert_eq!(
            exists_b,
            Some(r#"s3m_object_exists{host="host_B",bucket="bucket_B",prefix="foo/"} 0"#)
        );
    }

    #[test]
    fn test_prometheus_has_help_and_type_headers() {
        let output = format_prometheus(&[CheckResult {
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            prefix: "daily/".to_string(),
            suffix: String::new(),
            exist: true,
            error: false,
            size_mismatch: false,
        }]);

        assert!(output.contains("# HELP s3m_object_exists"));
        assert!(output.contains("# TYPE s3m_object_exists gauge"));
        assert!(output.contains("# HELP s3m_check_error"));
        assert!(output.contains("# TYPE s3m_check_error gauge"));
        assert!(output.contains("# HELP s3m_size_mismatch"));
        assert!(output.contains("# TYPE s3m_size_mismatch gauge"));
    }

    #[test]
    fn test_format_influxdb_includes_host_label() {
        let output = format_influxdb(&[CheckResult {
            host: "s3".to_string(),
            bucket: "bucket-a".to_string(),
            prefix: "daily/".to_string(),
            suffix: ".log".to_string(),
            exist: true,
            error: false,
            size_mismatch: false,
        }]);

        assert_eq!(
            output,
            "s3m,host=s3,bucket=bucket-a,prefix=daily/,suffix=.log error=0i,exist=1i,size_mismatch=0i\n"
        );
    }

    #[test]
    fn test_influxdb_format_sorted() {
        let output = format_influxdb(&[
            CheckResult {
                host: "host_B".to_string(),
                bucket: "bucket_B".to_string(),
                prefix: "foo/".to_string(),
                suffix: String::new(),
                exist: false,
                error: true,
                size_mismatch: false,
            },
            CheckResult {
                host: "host_A".to_string(),
                bucket: "bucket_A".to_string(),
                prefix: "test/".to_string(),
                suffix: String::new(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
        ]);

        let mut lines = output.lines();
        assert_eq!(
            lines.next(),
            Some("s3m,host=host_A,bucket=bucket_A,prefix=test/ error=0i,exist=1i,size_mismatch=0i")
        );
        assert_eq!(
            lines.next(),
            Some("s3m,host=host_B,bucket=bucket_B,prefix=foo/ error=1i,exist=0i,size_mismatch=0i")
        );
    }

    #[test]
    fn test_escape_special_chars_in_both_formats() {
        let results = [CheckResult {
            host: "s3 host".to_string(),
            bucket: r#"buck"et"#.to_string(),
            prefix: "pre\\fix".to_string(),
            suffix: ".log".to_string(),
            exist: true,
            error: false,
            size_mismatch: false,
        }];

        let prom = format_prometheus(&results);
        let influx = format_influxdb(&results);

        assert!(prom.contains(r#"host="s3 host""#));
        assert!(prom.contains(r#"bucket="buck\"et""#));
        assert!(prom.contains(r#"prefix="pre\\fix""#));
        assert!(influx.contains(r#"host=s3\ host,bucket=buck"et,prefix=pre\fix,suffix=.log"#));
    }

    #[test]
    fn test_sorting_uses_suffix_when_host_bucket_and_prefix_match() {
        let output = format_prometheus(&[
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                prefix: "logs/".to_string(),
                suffix: ".zst".to_string(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                prefix: "logs/".to_string(),
                suffix: ".log".to_string(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
        ]);
        let lines: Vec<&str> = output
            .lines()
            .filter(|line| line.starts_with("s3m_object_exists"))
            .collect();

        assert_eq!(
            lines,
            vec![
                r#"s3m_object_exists{host="s3",bucket="bucket",prefix="logs/",suffix=".log"} 1"#,
                r#"s3m_object_exists{host="s3",bucket="bucket",prefix="logs/",suffix=".zst"} 1"#,
            ]
        );
    }

    #[test]
    fn test_empty_results() {
        assert_eq!(
            format_prometheus(&[]),
            "# HELP s3m_object_exists Object exists within the configured age window\n\
             # TYPE s3m_object_exists gauge\n\
             # HELP s3m_check_error S3 API call failed\n\
             # TYPE s3m_check_error gauge\n\
             # HELP s3m_size_mismatch Object size is below the configured minimum\n\
             # TYPE s3m_size_mismatch gauge\n"
        );
        assert_eq!(format_influxdb(&[]), "");
    }

    #[test]
    fn test_both_formats_encode_identical_values() {
        let results = [
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket_A".to_string(),
                prefix: "daily/".to_string(),
                suffix: String::new(),
                exist: true,
                error: false,
                size_mismatch: false,
            },
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket_B".to_string(),
                prefix: "logs/".to_string(),
                suffix: String::new(),
                exist: false,
                error: true,
                size_mismatch: false,
            },
            CheckResult {
                host: "s3".to_string(),
                bucket: "bucket_C".to_string(),
                prefix: "data/".to_string(),
                suffix: ".log".to_string(),
                exist: true,
                error: false,
                size_mismatch: true,
            },
        ];

        let prom = format_prometheus(&results);
        let influx = format_influxdb(&results);

        assert!(
            prom.contains(r#"s3m_object_exists{host="s3",bucket="bucket_A",prefix="daily/"} 1"#)
        );
        assert!(prom.contains(r#"s3m_check_error{host="s3",bucket="bucket_A",prefix="daily/"} 0"#));
        assert!(influx.contains(
            "s3m,host=s3,bucket=bucket_A,prefix=daily/ error=0i,exist=1i,size_mismatch=0i"
        ));

        assert!(
            prom.contains(r#"s3m_object_exists{host="s3",bucket="bucket_B",prefix="logs/"} 0"#)
        );
        assert!(prom.contains(r#"s3m_check_error{host="s3",bucket="bucket_B",prefix="logs/"} 1"#));
        assert!(influx.contains(
            "s3m,host=s3,bucket=bucket_B,prefix=logs/ error=1i,exist=0i,size_mismatch=0i"
        ));

        assert!(prom.contains(
            r#"s3m_object_exists{host="s3",bucket="bucket_C",prefix="data/",suffix=".log"} 1"#
        ));
        assert!(prom.contains(
            r#"s3m_size_mismatch{host="s3",bucket="bucket_C",prefix="data/",suffix=".log"} 1"#
        ));
        assert!(
            influx.contains(
                "s3m,host=s3,bucket=bucket_C,prefix=data/,suffix=.log error=0i,exist=1i,size_mismatch=1i"
            )
        );
    }

    #[test]
    fn test_has_check_failures_detects_any_bad_result() {
        assert!(!has_check_failures(&[CheckResult {
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            prefix: "daily/".to_string(),
            suffix: String::new(),
            exist: true,
            error: false,
            size_mismatch: false,
        }]));
        assert!(has_check_failures(&[CheckResult {
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            prefix: "daily/".to_string(),
            suffix: String::new(),
            exist: false,
            error: false,
            size_mismatch: false,
        }]));
    }
}
