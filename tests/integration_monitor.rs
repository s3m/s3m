#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

mod common;

use chrono::{SecondsFormat, Utc};
use common::get_s3m_binary;
use mockito::{Matcher, Server};
use std::fmt::Write as _;
use std::io::Write;
use std::process::Command;
use tempfile::NamedTempFile;

fn run_s3m_with_config(config_path: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(get_s3m_binary())
        .arg("--config")
        .arg(config_path)
        .args(args)
        .output()
        .unwrap()
}

fn create_monitor_config(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    buckets_yaml: &str,
) -> NamedTempFile {
    let config = format!(
        r"---
hosts:
  s3:
    endpoint: {endpoint}
    access_key: {access_key}
    secret_key: {secret_key}
    buckets:
{buckets_yaml}
"
    );

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn list_objects_xml(bucket: &str, prefix: &str, entries: &[(&str, u64, &str)]) -> String {
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>{bucket}</Name><Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn error_xml(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{message}</Message><RequestId>request-id</RequestId></Error>"#
    )
}

#[test]
fn test_monitor_prometheus_output_default() {
    let mut server = Server::new();
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "backups/daily".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(
            "bucket-a",
            "backups/daily",
            &[("backups/daily-2026-03-16.log", 40_960, &now)],
        ))
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: backups/daily
          suffix: .log
          age: 1d
          size: 30720",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("# HELP s3m_object_exists"));
    assert!(stdout.contains(
        r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="backups/daily",suffix=".log"} 1"#
    ));
    assert!(stdout.contains(
        r#"s3m_check_error{host="s3",bucket="bucket-a",prefix="backups/daily",suffix=".log"} 0"#
    ));
    assert!(stdout.contains(
        r#"s3m_size_mismatch{host="s3",bucket="bucket-a",prefix="backups/daily",suffix=".log"} 0"#
    ));
}

#[test]
fn test_monitor_exit_on_check_failure_returns_non_zero_after_printing_metrics() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "backups/daily".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml("bucket-a", "backups/daily", &[]))
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: backups/daily
          age: 1d",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3", "--exit-on-check-failure"]);

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("# HELP s3m_object_exists"));
    assert!(
        stdout
            .contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="backups/daily"} 0"#)
    );
    assert!(
        stdout.contains(r#"s3m_check_error{host="s3",bucket="bucket-a",prefix="backups/daily"} 0"#)
    );
    assert!(stderr.contains("one or more monitor checks failed"));
}

#[test]
fn test_monitor_influxdb_output() {
    let mut server = Server::new();
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(
            "bucket-a",
            "logs/",
            &[("logs/app.log", 2_048, &now)],
        ))
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: logs/
          suffix: .log
          age: 1h
          size: 1024",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3", "--format", "influxdb"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout,
        "s3m,host=s3,bucket=bucket-a,prefix=logs/,suffix=.log error=0i,exist=1i,size_mismatch=0i\n"
    );
}

#[test]
fn test_monitor_prometheus_reports_check_error_without_failure_flag() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket-a")
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
        .create();

    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        r"      bucket-a:
        - prefix: logs/
          age: 60s",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="logs/"} 0"#));
    assert!(stdout.contains(r#"s3m_check_error{host="s3",bucket="bucket-a",prefix="logs/"} 1"#));
    assert!(stdout.contains(r#"s3m_size_mismatch{host="s3",bucket="bucket-a",prefix="logs/"} 0"#));
}

#[test]
fn test_monitor_rejects_empty_rule_list() {
    let server = Server::new();
    let config = create_monitor_config(
        &server.url(),
        "minioadmin",
        "minioadmin",
        "      bucket-a: []",
    );

    let output = run_s3m_with_config(config.path(), &["monitor", "s3"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("No monitor rules configured"));
}

#[test]
fn test_monitor_uses_custom_endpoint_when_region_is_also_set() {
    let mut server = Server::new();
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let _list = server
        .mock("GET", "/bucket-a")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(
            "bucket-a",
            "logs/",
            &[("logs/app.log", 2_048, &now)],
        ))
        .create();

    let config = format!(
        r"---
hosts:
  s3:
    endpoint: {}
    region: us-east-1
    access_key: minioadmin
    secret_key: minioadmin
    buckets:
      bucket-a:
        - prefix: logs/
          age: 1h
",
        server.url()
    );

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config.as_bytes()).unwrap();
    file.flush().unwrap();

    let output = run_s3m_with_config(file.path(), &["monitor", "s3"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(r#"s3m_object_exists{host="s3",bucket="bucket-a",prefix="logs/"} 1"#));
}
