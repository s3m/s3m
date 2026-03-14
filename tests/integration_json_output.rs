#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

mod common;

use common::{create_config_file, get_s3m_binary};
use mockito::{Matcher, Server};
use serde_json::Value;
use std::fmt::Write as _;
use std::process::Command;

fn run_s3m_with_config(config_path: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(get_s3m_binary())
        .arg("--config")
        .arg(config_path)
        .args(args)
        .output()
        .unwrap()
}

fn list_objects_xml(entries: &[(&str, u64, &str)]) -> String {
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn list_buckets_xml(names: &[(&str, &str)]) -> String {
    let mut buckets = String::new();
    for (name, creation_date) in names {
        let _ = write!(
            buckets,
            "<Bucket><Name>{name}</Name><CreationDate>{creation_date}</CreationDate></Bucket>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets>{buckets}</Buckets></ListAllMyBucketsResult>"#
    )
}

fn list_versions_xml(entries: &[(&str, &str, bool, &str, u64)]) -> String {
    let mut versions = String::new();
    for (key, version_id, is_latest, last_modified, size) in entries {
        let _ = write!(
            versions,
            "<Version><Key>{key}</Key><VersionId>{version_id}</VersionId><IsLatest>{is_latest}</IsLatest><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><Owner><ID>owner</ID></Owner><StorageClass>STANDARD</StorageClass></Version>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>bucket</Name><Prefix>prefix</Prefix><KeyMarker></KeyMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{versions}</ListVersionsResult>"#
    )
}

#[test]
fn test_ls_json_one_result() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[(
            "file.txt",
            17,
            "2026-03-14T08:00:00.000Z",
        )]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["ls", "s3/bucket", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["kind"], "objects");
    assert_eq!(stdout["bucket"], "bucket");
    assert_eq!(stdout["objects"][0]["key"], "file.txt");
    assert_eq!(stdout["objects"][0]["size_bytes"], 17);
}

#[test]
fn test_ls_json_empty_results() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["ls", "s3/bucket", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["objects"].as_array().unwrap().len(), 0);
}

#[test]
fn test_ls_buckets_json() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/")
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_buckets_xml(&[(
            "bucket-one",
            "2026-03-14T08:00:00.000Z",
        )]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["ls", "s3", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["kind"], "buckets");
    assert_eq!(stdout["buckets"][0]["name"], "bucket-one");
}

#[test]
fn test_ls_plain_output_unchanged_without_json() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[(
            "file.txt",
            17,
            "2026-03-14T08:00:00.000Z",
        )]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["ls", "s3/bucket"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("file.txt"));
    assert!(stdout.contains("[2026-03-14 08:00:00 UTC]"));
    assert!(serde_json::from_slice::<Value>(&output.stdout).is_err());
}

#[test]
fn test_get_meta_json_output() {
    let mut server = Server::new();
    let _head = server
        .mock("HEAD", "/bucket/file.txt")
        .with_status(200)
        .with_header("content-length", "42")
        .with_header("content-type", "text/plain")
        .with_header("etag", "\"head-etag\"")
        .with_header("last-modified", "Sat, 14 Mar 2026 08:00:00 GMT")
        .with_header("x-amz-meta-owner", "alice")
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(
        config.path(),
        &["get", "-m", "s3/bucket/file.txt", "--json"],
    );

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["bucket"], "bucket");
    assert_eq!(stdout["key"], "file.txt");
    assert_eq!(stdout["content_length"], 42);
    assert_eq!(stdout["content_type"], "text/plain");
    assert_eq!(stdout["last_modified"], "2026-03-14T08:00:00+00:00");
    assert_eq!(stdout["metadata"]["owner"], "alice");
}

#[test]
fn test_get_versions_json_output() {
    let mut server = Server::new();
    let _versions = server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("prefix".into(), "prefix".into()),
            Matcher::UrlEncoded("versions".into(), String::new()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_versions_xml(&[(
            "prefix",
            "v1",
            true,
            "2026-03-14T08:00:00.000Z",
            42,
        )]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(
        config.path(),
        &["get", "s3/bucket/prefix", "--versions", "--json"],
    );

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["bucket"], "bucket");
    assert_eq!(stdout["key_prefix"], "prefix");
    assert_eq!(stdout["versions"][0]["version_id"], "v1");
    assert_eq!(stdout["versions"][0]["size_bytes"], 42);
}

#[test]
fn test_du_json_summary() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[
            ("a.txt", 5, "2026-03-13T00:00:00.000Z"),
            ("b.txt", 7, "2026-03-14T00:00:00.000Z"),
        ]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["du", "s3/bucket", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["target"], "s3/bucket");
    assert_eq!(stdout["summary"]["object_count"], 2);
    assert_eq!(stdout["summary"]["bytes"], 12);
}

#[test]
fn test_du_json_grouped_by_day() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_objects_xml(&[
            ("a.txt", 5, "2026-03-13T23:00:00.000Z"),
            ("b.txt", 7, "2026-03-14T00:00:00.000Z"),
        ]))
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(
        config.path(),
        &["du", "s3/bucket", "--group-by", "day", "--json"],
    );

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["group_by"], "day");
    assert_eq!(stdout["days"][0]["date"], "2026-03-13");
    assert_eq!(stdout["days"][1]["date"], "2026-03-14");
    assert_eq!(stdout["total"]["bytes"], 12);
}
