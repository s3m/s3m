//! E2E tests for `rm` (delete object) command

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, create_config_file, get_s3m_binary, run_s3m_with_minio};
use mockito::{Matcher, Server};
use s3m::s3::{Credentials, Region, S3, actions::CreateMultipartUpload};
use secrecy::SecretString;
use std::fmt::Write as _;
use std::io::Write;
use std::process::{Command, Output};
use tempfile::NamedTempFile;

/// A bucket this test file creates and removes; it never addresses a bucket it
/// did not create.
struct OwnedBucket<'a> {
    minio: &'a MinioContext,
    name: String,
}

impl<'a> OwnedBucket<'a> {
    async fn create(minio: &'a MinioContext, label: &str) -> Self {
        let name = format!("task7-rm-{label}-{:08x}", rand::random::<u32>());
        minio.create_bucket(&name).await.expect("Bucket creation");
        Self { minio, name }
    }
}

impl Drop for OwnedBucket<'_> {
    fn drop(&mut self) {
        let out = run_s3m_with_minio(
            self.minio,
            &["rm", "-b", "--recursive", &format!("s3/{}", self.name)],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        // Some tests delete their own bucket as the behavior under test.
        let removed = out.status.success() || stderr.contains("NoSuchBucket");
        assert!(
            removed || std::thread::panicking(),
            "cleanup of owned bucket {} failed: {stderr}",
            self.name
        );
    }
}

fn payload_file(content: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(content).expect("write");
    file.flush().expect("flush");
    file
}

fn upload_keys(minio: &MinioContext, bucket: &str, path: &str, keys: &[&str]) {
    for key in keys {
        let out = run_s3m_with_minio(minio, &[path, &format!("s3/{bucket}/{key}")]);
        assert!(out.status.success(), "upload {key}: {:?}", out);
    }
}

#[tokio::test]
async fn test_e2e_delete_object() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::create(&minio, "single").await;

    let file = payload_file(b"File to be deleted");
    let path = file.path().to_str().expect("Invalid path");
    upload_keys(&minio, &bucket.name, path, &["to-delete.txt", "keep.txt"]);

    let delete_output = run_s3m_with_minio(
        &minio,
        &["rm", &format!("s3/{}/to-delete.txt", bucket.name)],
    );
    assert!(delete_output.status.success(), "{:?}", delete_output);
    assert_eq!(
        String::from_utf8_lossy(&delete_output.stdout),
        "to-delete.txt: deleted\n"
    );
    assert_eq!(listed_keys(&minio, &bucket.name), vec!["keep.txt"]);
}

fn listed_keys(minio: &MinioContext, bucket: &str) -> Vec<String> {
    let out = run_s3m_with_minio(minio, &["ls", &format!("s3/{bucket}")]);
    assert!(out.status.success(), "ls should succeed");
    let mut keys: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.split_once("] "))
        .filter_map(|(_, rest)| rest.split_whitespace().last().map(str::to_string))
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn test_e2e_delete_prefix_recursive_is_literal_and_keeps_bucket() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::create(&minio, "prefix").await;
    let bucket = owned.name.as_str();

    let file = payload_file(b"x");
    let path = file.path().to_str().expect("path");

    upload_keys(
        &minio,
        bucket,
        path,
        &[
            "integration/18/",
            "integration/18/file1.rpm",
            "integration/18/el9/file2.rpm",
            "integration/18/el10/file3.rpm",
            "integration/19/file4.rpm",
            "integration/180/file5.rpm",
            "integration/18-other/file6.rpm",
            "integration/18",
        ],
    );

    let out = run_s3m_with_minio(
        &minio,
        &["rm", &format!("s3/{bucket}/integration/18/"), "--recursive"],
    );
    assert!(out.status.success(), "{:?}", out);

    let remaining = listed_keys(&minio, bucket);
    assert_eq!(
        remaining,
        vec![
            "integration/18",
            "integration/18-other/file6.rpm",
            "integration/180/file5.rpm",
            "integration/19/file4.rpm",
        ]
    );
    // The bucket must still exist: uploading into it (without recreating it) works.
    let out = run_s3m_with_minio(&minio, &[path, &format!("s3/{bucket}/after-delete.txt")]);
    assert!(out.status.success(), "bucket must still exist: {:?}", out);
    let out = run_s3m_with_minio(&minio, &["rm", &format!("s3/{bucket}/after-delete.txt")]);
    assert!(out.status.success(), "{:?}", out);

    // Nonexistent prefix is a successful no-op.
    let out = run_s3m_with_minio(
        &minio,
        &["rm", &format!("s3/{bucket}/nothing/here/"), "--recursive"],
    );
    assert!(out.status.success(), "{:?}", out);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "No objects found under prefix 'nothing/here/'; nothing deleted.\n"
    );
    assert_eq!(listed_keys(&minio, bucket).len(), 4);

    // Empty prefix and prefix + --bucket are rejected without deleting anything.
    for args in [
        vec!["rm", &format!("s3/{bucket}"), "--recursive"],
        vec![
            "rm",
            "-b",
            "--recursive",
            &format!("s3/{bucket}/integration/19/"),
        ],
    ] {
        let out = run_s3m_with_minio(&minio, &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {:?}", out);
    }
    assert_eq!(listed_keys(&minio, bucket).len(), 4);
}

#[tokio::test]
async fn test_e2e_recursive_short_alias_matches_exact_prefix_only() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::create(&minio, "alias").await;
    let bucket = owned.name.as_str();

    let file = payload_file(b"log line");
    let path = file.path().to_str().expect("path");
    upload_keys(
        &minio,
        bucket,
        path,
        &[
            "logs/",
            "logs/app.log",
            "logs/2026/11/app.log",
            "logs-old/app.log",
            "logsx/app.log",
        ],
    );

    let out = run_s3m_with_minio(&minio, &["rm", "-r", &format!("s3/{bucket}/logs/")]);
    assert!(out.status.success(), "{:?}", out);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "Deleted 3 objects under prefix 'logs/'.\n"
    );
    assert_eq!(
        listed_keys(&minio, bucket),
        vec!["logs-old/app.log", "logsx/app.log"]
    );
}

#[tokio::test]
async fn test_e2e_multiple_targets_delete_exact_keys_across_buckets() {
    let minio = MinioContext::get_or_start().await;
    let first = OwnedBucket::create(&minio, "multi-a").await;
    let second = OwnedBucket::create(&minio, "multi-b").await;

    let file = payload_file(b"multi");
    let path = file.path().to_str().expect("path");
    upload_keys(&minio, &first.name, path, &["a1", "a2", "a-keep"]);
    upload_keys(&minio, &second.name, path, &["b1", "b-keep"]);

    let out = run_s3m_with_minio(
        &minio,
        &[
            "rm",
            &format!("s3/{}/a1", first.name),
            &format!("s3/{}/b1", second.name),
            &format!("s3/{}/a2", first.name),
        ],
    );
    assert!(out.status.success(), "{:?}", out);

    assert_eq!(listed_keys(&minio, &first.name), vec!["a-keep"]);
    assert_eq!(listed_keys(&minio, &second.name), vec!["b-keep"]);
}

/// Abort through the CLI; aborting an upload that does not exist is refused
/// by the service, so a successful abort proves the upload was live.
fn abort(minio: &MinioContext, target: &str, flag: &str, upload_id: &str) -> Output {
    run_s3m_with_minio(minio, &["rm", target, flag, upload_id])
}

fn assert_no_such_upload(output: &Output) {
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("NoSuchUpload"),
        "{:?}",
        output
    );
}

#[tokio::test]
async fn test_e2e_abort_multipart_upload_long_and_short_flags() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::create(&minio, "abort").await;
    let bucket = owned.name.as_str();

    let s3 = S3::new(
        &Credentials::new(
            minio.access_key(),
            &SecretString::new(minio.secret_key().to_string().into()),
        ),
        &Region::Custom {
            name: String::new(),
            endpoint: minio.endpoint().to_string(),
        },
        Some(bucket.to_string()),
        false,
    );
    let first = CreateMultipartUpload::new("big-1.bin", None, None, None, None)
        .request(&s3)
        .await
        .unwrap()
        .upload_id;
    let second = CreateMultipartUpload::new("big-2.bin", None, None, None, None)
        .request(&s3)
        .await
        .unwrap()
        .upload_id;
    let first_target = format!("s3/{bucket}/big-1.bin");
    let second_target = format!("s3/{bucket}/big-2.bin");

    let out = abort(&minio, &first_target, "--abort", &first);
    assert!(out.status.success(), "{:?}", out);
    assert!(out.stdout.is_empty(), "{:?}", out);
    assert_no_such_upload(&abort(&minio, &first_target, "--abort", &first));

    // The other upload was untouched until its own abort (short flag).
    let out = abort(&minio, &second_target, "-a", &second);
    assert!(out.status.success(), "{:?}", out);
    assert_no_such_upload(&abort(&minio, &second_target, "-a", &second));
}

#[tokio::test]
async fn test_e2e_bucket_delete_requires_empty_bucket_unless_recursive() {
    let minio = MinioContext::get_or_start().await;
    let empty = OwnedBucket::create(&minio, "empty").await;
    let full = OwnedBucket::create(&minio, "full").await;

    let out = run_s3m_with_minio(&minio, &["rm", "--bucket", &format!("s3/{}", empty.name)]);
    assert!(out.status.success(), "{:?}", out);
    let gone = run_s3m_with_minio(&minio, &["ls", &format!("s3/{}", empty.name)]);
    assert_eq!(gone.status.code(), Some(1), "{:?}", gone);
    assert!(String::from_utf8_lossy(&gone.stderr).contains("NoSuchBucket"));

    let file = payload_file(b"keep me");
    let path = file.path().to_str().expect("path");
    upload_keys(&minio, &full.name, path, &["a.txt", "nested/b.txt"]);

    let refused = run_s3m_with_minio(&minio, &["rm", "-b", &format!("s3/{}", full.name)]);
    assert_eq!(refused.status.code(), Some(1), "{:?}", refused);
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("BucketNotEmpty"),
        "{:?}",
        refused
    );
    assert_eq!(
        listed_keys(&minio, &full.name),
        vec!["a.txt", "nested/b.txt"]
    );

    let out = run_s3m_with_minio(&minio, &["rm", "-b", "-r", &format!("s3/{}", full.name)]);
    assert!(out.status.success(), "{:?}", out);
    let gone = run_s3m_with_minio(&minio, &["ls", &format!("s3/{}", full.name)]);
    assert_eq!(gone.status.code(), Some(1), "{:?}", gone);
    assert!(String::from_utf8_lossy(&gone.stderr).contains("NoSuchBucket"));
}

#[tokio::test]
async fn test_e2e_bucket_delete_with_key_is_rejected_and_preserves_data() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::create(&minio, "key-safety").await;
    let bucket = owned.name.as_str();

    let file = payload_file(b"safety-check");
    let path = file.path().to_str().expect("path");

    upload_keys(
        &minio,
        bucket,
        path,
        &["file.txt", "keep.txt", "nested/a.dat", "nested/b.dat"],
    );
    assert_eq!(listed_keys(&minio, bucket).len(), 4);

    // Test rejection across various invocations combining -b with a key
    let target_file = format!("s3/{bucket}/file.txt");
    let target_nested_dir = format!("s3/{bucket}/nested/");
    let target_nested_file = format!("s3/{bucket}/nested/a.dat");
    let target_bucket = format!("s3/{bucket}");

    let attempts: [&[&str]; 5] = [
        &["rm", "-b", &target_file],
        &["rm", "--bucket", &target_file],
        &["rm", "-b", "--recursive", &target_nested_dir],
        &["rm", "-b", "-r", &target_nested_file],
        &["rm", &target_file, "-b"],
    ];
    for args in attempts {
        let out = run_s3m_with_minio(&minio, args);
        assert!(
            !out.status.success(),
            "{args:?}: command should fail when -b is combined with a key"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("cannot be combined with an object key or prefix"),
            "{args:?}: expected safety error on stderr, got:\n{stderr}"
        );
    }

    // Multi-target attempt: bucket root first, key second
    let multi_out = run_s3m_with_minio(&minio, &["rm", "-b", &target_bucket, &target_file]);
    assert!(!multi_out.status.success());
    let multi_stderr = String::from_utf8_lossy(&multi_out.stderr);
    assert!(
        multi_stderr.contains("Bucket deletion expects exactly one bucket target"),
        "multi_stderr: {multi_stderr}"
    );

    // Critical assertion: ALL objects must still exist (nothing was deleted)
    let remaining = listed_keys(&minio, bucket);
    assert_eq!(
        remaining,
        vec!["file.txt", "keep.txt", "nested/a.dat", "nested/b.dat"],
        "All objects must survive rejected -b commands"
    );

    // Critical assertion: bucket must still be intact and functional
    let check = run_s3m_with_minio(&minio, &["ls", &format!("s3/{bucket}")]);
    assert!(
        check.status.success(),
        "bucket must still exist and be readable"
    );
}

// ---------------------------------------------------------------------------
// Request-shape and rejection tests against a local mock service
// ---------------------------------------------------------------------------

fn run_against(server: &Server, args: &[&str]) -> Output {
    let config = create_config_file(&server.url(), "minioadmin", "minioadmin");
    let output = Command::new(get_s3m_binary())
        .arg("--config")
        .arg(config.path())
        .args(args)
        .output()
        .expect("run s3m");
    drop(config);
    output
}

/// Mocks that fail the test (on `assert`) if the binary sends any request.
fn forbid_requests(server: &mut Server) -> Vec<mockito::Mock> {
    ["GET", "HEAD", "PUT", "POST", "DELETE"]
        .into_iter()
        .map(|method| {
            server
                .mock(method, Matcher::Any)
                .with_status(500)
                .expect(0)
                .create()
        })
        .collect()
}

fn requested_keys(body: &str) -> Vec<String> {
    body.split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Key>").map(|(key, _)| key.to_string()))
        .collect()
}

fn delete_body_is(expected: &'static [&'static str]) -> impl Fn(&mockito::Request) -> bool {
    move |request| {
        let body = request.utf8_lossy_body().unwrap_or_default();
        requested_keys(&body) == expected
    }
}

#[test]
fn test_rm_rejects_invalid_targets_and_mode_combinations_without_requests() {
    let mut server = Server::new();
    let none = forbid_requests(&mut server);

    let cases: &[(&[&str], i32, &str)] = &[
        (&["rm", "s3/bucket"], 1, "file name missing"),
        (
            &["rm", "s3/bucket/p/", "-r", "--abort", "id"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/p/", "-r", "--version-id", "v1"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/p/", "-r", "--older-than", "1d"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/k", "--version-id", "v1", "-b"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/k", "--version-id", "v1", "--abort", "id"],
            2,
            "cannot be used with",
        ),
        (
            &[
                "rm",
                "s3/bucket/k",
                "--version-id",
                "v1",
                "--older-than",
                "1d",
            ],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/k", "--bypass-governance", "-a", "id"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "-b", "s3/bucket", "--older-than", "1d"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/k", "--abort", "id", "--older-than", "1d"],
            2,
            "cannot be used with",
        ),
        (
            &["rm", "s3/bucket/a/", "s3/bucket/b/", "--older-than", "1d"],
            1,
            "--older-than expects exactly one bucket or prefix target",
        ),
        (
            &["rm", "s3/bucket/a/", "s3/bucket/b/", "-r"],
            1,
            "--recursive expects exactly one prefix target",
        ),
        (
            &["rm", "s3/bucket/k", "s3/bucket"],
            1,
            "Object target missing key: s3/bucket",
        ),
        (
            &["rm", "s3/bucket/a", "s3/bucket/b", "--abort", "id"],
            1,
            "Abort multipart upload expects exactly one object target",
        ),
        (
            &["rm", "s3/bucket/k", "--older-than", "0d"],
            2,
            "Duration must be greater than zero",
        ),
        (
            &["rm", "s3/bucket/k", "--older-than", "999999999999999d"],
            2,
            "Duration is too large",
        ),
    ];

    for (args, code, message) in cases {
        let out = run_against(&server, args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(*code), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(out.stdout.is_empty(), "{args:?}: {:?}", out.stdout);
    }
    for mock in none {
        mock.assert();
    }
}

const OLD: &str = "2000-01-01T00:00:00.000Z";
const FUTURE: &str = "2999-01-01T00:00:00.000Z";

fn list_page(entries: &[(&str, &str)]) -> String {
    let contents = entries.iter().fold(String::new(), |mut out, (key, modified)| {
        let _ = write!(
            out,
            "<Contents><Key>{key}</Key><LastModified>{modified}</LastModified><ETag>\"e\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"
        );
        out
    });
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bucket</Name><Prefix>logs/</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"#
    )
}

fn logs_listing(server: &mut Server, entries: &[(&str, &str)]) -> mockito::Mock {
    server
        .mock("GET", "/bucket")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("list-type".into(), "2".into()),
            Matcher::UrlEncoded("prefix".into(), "logs/".into()),
            Matcher::UrlEncoded("max-keys".into(), "1000".into()),
        ]))
        .with_status(200)
        .with_header("content-type", "application/xml")
        .with_body(list_page(entries))
        .expect(1)
        .create()
}

#[test]
fn test_rm_older_than_deletes_only_aged_objects_in_one_batch() {
    let mut server = Server::new();
    let list = logs_listing(
        &mut server,
        &[
            ("logs/old-1", OLD),
            ("logs/fresh", FUTURE),
            ("logs/old-2", OLD),
        ],
    );
    let delete = server
        .mock("POST", "/bucket")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_request(delete_body_is(&["logs/old-1", "logs/old-2"]))
        .with_status(200)
        .with_body(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>"#)
        .expect(1)
        .create();

    let out = run_against(&server, &["rm", "s3/bucket/logs/", "--older-than", "1d"]);

    assert!(out.status.success(), "{:?}", out);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "Matched 2 objects; deleted 2 objects.\n"
    );
    list.assert();
    delete.assert();
}

#[test]
fn test_rm_older_than_beyond_supported_dates_matches_nothing() {
    let mut server = Server::new();
    let list = logs_listing(&mut server, &[("logs/old-1", OLD)]);
    let delete = server
        .mock("POST", "/bucket")
        .match_query(Matcher::Any)
        .expect(0)
        .create();

    let out = run_against(
        &server,
        &["rm", "s3/bucket/logs/", "--older-than", "100000000d"],
    );

    assert!(out.status.success(), "{:?}", out);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "Matched 0 objects; deleted 0 objects.\n"
    );
    list.assert();
    delete.assert();
}

#[test]
fn test_rm_partial_delete_failure_names_failed_keys_and_stops() {
    let mut server = Server::new();
    let first = server
        .mock("POST", "/bucket-a")
        .match_query(Matcher::UrlEncoded("delete".into(), String::new()))
        .match_request(delete_body_is(&["k1", "k2"]))
        .with_status(200)
        .with_body(
            r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult><Deleted><Key>k1</Key></Deleted><Error><Key>k2</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error></DeleteResult>"#,
        )
        .expect(1)
        .create();
    let second = server
        .mock("POST", "/bucket-b")
        .match_query(Matcher::Any)
        .expect(0)
        .create();

    let out = run_against(
        &server,
        &["rm", "s3/bucket-a/k1", "s3/bucket-b/k3", "s3/bucket-a/k2"],
    );

    assert_eq!(out.status.code(), Some(1), "{:?}", out);
    assert!(out.stdout.is_empty(), "{:?}", out);
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(
            "DeleteObjects returned 1 object error(s):\n - k2: Access Denied (AccessDenied)"
        ),
        "{:?}",
        out
    );
    first.assert();
    second.assert();
}
