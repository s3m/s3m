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
    list_objects_page_xml(entries, false, None)
}

fn list_objects_page_xml(
    entries: &[(&str, u64, &str)],
    is_truncated: bool,
    next_continuation_token: Option<&str>,
) -> String {
    let next = next_continuation_token.map_or_else(String::new, |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });
    let mut contents = String::new();
    for (key, size, last_modified) in entries {
        let _ = write!(
            contents,
            "<Contents><Key>{key}</Key><LastModified>{last_modified}</LastModified><ETag>\"etag-{key}\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix></Prefix><MaxKeys>1000</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{next}{contents}</ListBucketResult>"#
    )
}

fn list_buckets_xml(names: &[(&str, &str)]) -> String {
    list_buckets_page_xml(names, None)
}

fn list_buckets_page_xml(names: &[(&str, &str)], continuation_token: Option<&str>) -> String {
    let token = continuation_token.map_or_else(String::new, |token| {
        format!("<ContinuationToken>{token}</ContinuationToken>")
    });
    let mut buckets = String::new();
    for (name, creation_date) in names {
        let _ = write!(
            buckets,
            "<Bucket><Name>{name}</Name><CreationDate>{creation_date}</CreationDate></Bucket>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets>{buckets}</Buckets>{token}</ListAllMyBucketsResult>"#
    )
}

fn list_uploads_page_xml(uploads: &[(&str, &str)], next_markers: Option<(&str, &str)>) -> String {
    let next = next_markers.map_or_else(String::new, |(key, upload_id)| {
        format!("<NextKeyMarker>{key}</NextKeyMarker><NextUploadIdMarker>{upload_id}</NextUploadIdMarker>")
    });
    let mut entries = String::new();
    for (key, upload_id) in uploads {
        let _ = write!(
            entries,
            "<Upload><Key>{key}</Key><UploadId>{upload_id}</UploadId><Initiated>2026-03-14T08:00:00.000Z</Initiated><StorageClass>STANDARD</StorageClass></Upload>"
        );
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><Bucket>bucket</Bucket><MaxUploads>1000</MaxUploads><IsTruncated>{}</IsTruncated>{next}{entries}</ListMultipartUploadsResult>"#,
        next_markers.is_some()
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

fn json_keys(value: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

/// Last whitespace-separated field of every stdout line (the key or bucket name).
fn text_names(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(|line| line.split_whitespace().last().unwrap().to_string())
        .collect()
}

#[test]
fn test_ls_number_is_a_total_across_object_pages_in_text_and_json() {
    let mut server = Server::new();
    let token = "1/a+b=c\u{e9}";
    let page_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[
            ("list-type", "2"),
            ("max-keys", "3"),
            ("prefix", "logs/"),
            ("start-after", "logs/a"),
        ]))
        .with_status(200)
        .with_body(list_objects_page_xml(
            &[
                ("logs/b", 1, "2026-03-14T08:00:00.000Z"),
                ("logs/c", 1, "2026-03-14T08:00:00.000Z"),
            ],
            true,
            Some(token),
        ))
        .expect(2)
        .create();
    let page_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[
            ("list-type", "2"),
            ("max-keys", "1"),
            ("prefix", "logs/"),
            ("continuation-token", token),
        ]))
        .with_status(200)
        .with_body(list_objects_page_xml(
            &[
                ("logs/d", 1, "2026-03-14T08:00:00.000Z"),
                ("logs/e", 1, "2026-03-14T08:00:00.000Z"),
            ],
            true,
            Some("unused"),
        ))
        .expect(2)
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let json = run_s3m_with_config(
        config.path(),
        &[
            "ls",
            "s3/bucket/logs/",
            "--number",
            "3",
            "--start-after",
            "logs/a",
            "--json",
        ],
    );
    let text = run_s3m_with_config(
        config.path(),
        &["ls", "s3/bucket", "-p", "logs/", "-n", "3", "-a", "logs/a"],
    );

    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    assert!(
        text.status.success(),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let stdout: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(
        json_keys(&stdout),
        ["bucket", "kind", "objects", "prefix", "start_after"]
    );
    assert_eq!(stdout["prefix"], "logs/");
    assert_eq!(stdout["start_after"], "logs/a");
    let json_names: Vec<&str> = stdout["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| object["key"].as_str().unwrap())
        .collect();
    assert_eq!(json_names, ["logs/b", "logs/c", "logs/d"]);
    assert_eq!(text_names(&text.stdout), json_names);
    page_1.assert();
    page_2.assert();
}

#[test]
fn test_ls_number_zero_is_empty_success_without_requests() {
    let mut server = Server::new();
    let any_request = server.mock("GET", Matcher::Any).expect(0).create();
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");

    let objects = run_s3m_with_config(config.path(), &["ls", "s3/bucket", "-n", "0", "--json"]);
    let uploads = run_s3m_with_config(
        config.path(),
        &["ls", "s3/bucket", "--multipart", "--number", "0", "--json"],
    );
    let buckets = run_s3m_with_config(config.path(), &["ls", "s3", "--number", "0", "--json"]);
    let text = run_s3m_with_config(config.path(), &["ls", "s3/bucket", "--number", "0"]);

    for output in [&objects, &uploads, &buckets, &text] {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        serde_json::from_slice::<Value>(&objects.stdout).unwrap(),
        serde_json::json!({"kind": "objects", "bucket": "bucket", "prefix": null, "start_after": null, "objects": []})
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&uploads.stdout).unwrap(),
        serde_json::json!({"kind": "multipart_uploads", "bucket": "bucket", "uploads": []})
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&buckets.stdout).unwrap(),
        serde_json::json!({"kind": "buckets", "buckets": []})
    );
    assert_eq!(String::from_utf8_lossy(&text.stdout), "");
    any_request.assert();
}

#[test]
fn test_ls_multipart_total_follows_marker_pairs_with_prefix() {
    let mut server = Server::new();
    let page_1 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[
            ("uploads", ""),
            ("max-uploads", "3"),
            ("prefix", "logs/"),
            ("key-marker", "logs/a"),
        ]))
        .with_status(200)
        .with_body(list_uploads_page_xml(
            &[("logs/b", "u-1"), ("logs/b", "u-2")],
            Some(("logs/b", "u-2")),
        ))
        .expect(2)
        .create();
    let page_2 = server
        .mock("GET", "/bucket")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[
            ("uploads", ""),
            ("max-uploads", "1"),
            ("prefix", "logs/"),
            ("key-marker", "logs/b"),
            ("upload-id-marker", "u-2"),
        ]))
        .with_status(200)
        .with_body(list_uploads_page_xml(
            &[("logs/c", "u-3"), ("logs/d", "u-4")],
            Some(("logs/d", "u-4")),
        ))
        .expect(2)
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let json = run_s3m_with_config(
        config.path(),
        &[
            "ls",
            "s3/bucket/logs/",
            "--multipart",
            "--start-after",
            "logs/a",
            "--number",
            "3",
            "--json",
        ],
    );
    let text = run_s3m_with_config(
        config.path(),
        &[
            "ls",
            "s3/bucket",
            "-m",
            "-p",
            "logs/",
            "-a",
            "logs/a",
            "-n",
            "3",
        ],
    );

    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    assert!(
        text.status.success(),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let stdout: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(json_keys(&stdout), ["bucket", "kind", "uploads"]);
    let upload_ids: Vec<&str> = stdout["uploads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|upload| upload["upload_id"].as_str().unwrap())
        .collect();
    assert_eq!(upload_ids, ["u-1", "u-2", "u-3"]);
    let text_upload_ids: Vec<String> = String::from_utf8_lossy(&text.stdout)
        .lines()
        .map(|line| line.split_whitespace().nth(3).unwrap().to_string())
        .collect();
    assert_eq!(text_upload_ids, upload_ids);
    page_1.assert();
    page_2.assert();
}

#[test]
fn test_ls_buckets_total_follows_continuation_token() {
    let mut server = Server::new();
    let page_1 = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[("max-buckets", "3")]))
        .with_status(200)
        .with_body(list_buckets_page_xml(
            &[
                ("b-1", "2026-03-14T08:00:00.000Z"),
                ("b-2", "2026-03-14T08:00:00.000Z"),
            ],
            Some("t2"),
        ))
        .expect(2)
        .create();
    let page_2 = server
        .mock("GET", "/")
        .match_query(Matcher::Any)
        .match_request(exact_query(&[
            ("max-buckets", "1"),
            ("continuation-token", "t2"),
        ]))
        .with_status(200)
        .with_body(list_buckets_page_xml(
            &[
                ("b-3", "2026-03-14T08:00:00.000Z"),
                ("b-4", "2026-03-14T08:00:00.000Z"),
            ],
            Some("t3"),
        ))
        .expect(2)
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let json = run_s3m_with_config(config.path(), &["ls", "s3", "--number", "3", "--json"]);
    let text = run_s3m_with_config(config.path(), &["ls", "s3", "-n", "3"]);

    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    assert!(
        text.status.success(),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let stdout: Value = serde_json::from_slice(&json.stdout).unwrap();
    let names: Vec<&str> = stdout["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|bucket| bucket["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["b-1", "b-2", "b-3"]);
    assert_eq!(text_names(&text.stdout), names);
    page_1.assert();
    page_2.assert();
}

#[test]
fn test_ls_host_only_rejects_bucket_scoped_flags_without_requests() {
    let mut server = Server::new();
    let any_request = server.mock("GET", Matcher::Any).expect(0).create();
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");

    for (args, message) in [
        (
            vec!["ls", "s3", "--multipart"],
            "--multipart requires a bucket target",
        ),
        (
            vec!["ls", "s3", "-m"],
            "--multipart requires a bucket target",
        ),
        (
            vec!["ls", "s3", "--prefix", "logs/"],
            "--prefix requires a bucket target",
        ),
        (
            vec!["ls", "s3", "-p", "logs/"],
            "--prefix requires a bucket target",
        ),
        (
            vec!["ls", "s3", "--start-after", "a"],
            "--start-after requires a bucket target",
        ),
        (
            vec!["ls", "s3", "-a", "a", "--json"],
            "--start-after requires a bucket target",
        ),
        (
            vec!["ls", "s3", "--older-than", "30d"],
            "--older-than requires a bucket or prefix target",
        ),
    ] {
        let output = run_s3m_with_config(config.path(), &args);

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{args:?} should fail");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(output.stdout.is_empty(), "{args:?} printed to stdout");
    }
    any_request.assert();
}

#[test]
fn test_ls_rejects_invalid_arguments_without_requests() {
    let mut server = Server::new();
    let any_request = server.mock("GET", Matcher::Any).expect(0).create();
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");

    for args in [
        vec!["ls", "s3/bucket", "--number"],
        vec!["ls", "s3/bucket", "--number", "-1"],
        vec!["ls", "s3/bucket", "-n", "1.5"],
        vec!["ls", "s3/bucket", "-n", "ten"],
        vec!["ls", "s3/bucket", "-n", "18446744073709551616"],
        vec!["ls", "s3/bucket", "--multipart", "--older-than", "30d"],
        vec!["ls", "s3/bucket/logs/", "--prefix", "other/"],
    ] {
        let output = run_s3m_with_config(config.path(), &args);

        assert!(!output.status.success(), "{args:?} should fail");
        assert!(output.stdout.is_empty(), "{args:?} printed to stdout");
    }
    any_request.assert();
}

#[test]
fn test_ls_older_than_rejects_invalid_durations_without_requests() {
    let mut server = Server::new();
    let any_request = server.mock("GET", Matcher::Any).expect(0).create();
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");

    for (value, message) in [
        ("30x", "Invalid duration '30x'. Expected Nd, Nh, or Nm"),
        ("0d", "Duration must be greater than zero"),
        ("0", "Invalid duration '0'. Expected Nd, Nh, or Nm"),
        ("", "Invalid duration ''. Expected Nd, Nh, or Nm"),
        ("d", "Expected a number followed by d, h, or m"),
        ("1.5d", "Expected a number followed by d, h, or m"),
        (
            "99999999999999999999d",
            "Expected a number followed by d, h, or m",
        ),
        ("-1d", "unexpected argument '-1'"),
    ] {
        let args = ["ls", "s3/bucket", "--older-than", value];
        let output = run_s3m_with_config(config.path(), &args);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(output.stdout.is_empty(), "{args:?} printed to stdout");
    }
    any_request.assert();
}

#[test]
fn test_ls_listing_error_exits_nonzero_without_json() {
    let mut server = Server::new();
    let _list = server
        .mock("GET", "/bucket")
        .match_query(Matcher::UrlEncoded("list-type".into(), "2".into()))
        .with_status(403)
        .with_body("<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
        .create();

    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = run_s3m_with_config(config.path(), &["ls", "s3/bucket", "-n", "2", "--json"]);

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "");
    assert!(String::from_utf8_lossy(&output.stderr).contains("AccessDenied"));
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
        .mock("GET", "/?max-buckets=10000")
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
