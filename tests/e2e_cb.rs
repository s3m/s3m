//! E2E tests for `cb` (create bucket) command

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, run_s3m_with_minio};
use std::fs;
use tempfile::TempDir;

const ACLS: [&str; 7] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
];

/// A bucket this test run created; it is removed even when the test fails.
struct OwnedBucket<'a> {
    minio: &'a MinioContext,
    name: String,
}

impl<'a> OwnedBucket<'a> {
    fn new(minio: &'a MinioContext, label: &str) -> Self {
        Self {
            minio,
            name: format!("e2e-cb-{label}-{:016x}", rand::random::<u64>()),
        }
    }
}

impl Drop for OwnedBucket<'_> {
    fn drop(&mut self) {
        let result = run_s3m_with_minio(
            self.minio,
            &["rm", "-b", "--recursive", &format!("s3/{}", self.name)],
        );
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            result.status.success() || std::thread::panicking(),
            "owned bucket cleanup {} failed: {error}",
            self.name
        );
    }
}

fn listed_buckets(minio: &MinioContext) -> Vec<String> {
    let output = run_s3m_with_minio(minio, &["ls", "s3", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let listing: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    listing
        .get("buckets")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .iter()
        .map(|bucket| {
            bucket
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap()
                .to_string()
        })
        .collect()
}

/// `cb` prints only the provider's `Location` header, if it returns one.
fn assert_created(output: &std::process::Output, args: &[&str]) {
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.lines().all(|line| line.starts_with("location: ")),
        "{args:?}: {stdout}"
    );
}

#[tokio::test]
async fn test_e2e_create_bucket_and_recreate_keeps_objects() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::new(&minio, "recreate");
    let target = format!("s3/{}", bucket.name);
    assert!(!listed_buckets(&minio).contains(&bucket.name));

    let args = ["cb", target.as_str()];
    assert_created(&run_s3m_with_minio(&minio, &args), &args);
    assert!(listed_buckets(&minio).contains(&bucket.name));

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("keep.txt");
    fs::write(&source, b"survives a repeated cb").unwrap();
    let object = format!("{target}/keep.txt");
    let upload = run_s3m_with_minio(&minio, &["-q", source.to_str().unwrap(), &object]);
    assert!(upload.status.success());

    // S3 answers 409 BucketAlreadyOwnedByYou outside us-east-1; RustFS and
    // us-east-1 answer 200. Either way the existing contents must survive.
    let again = run_s3m_with_minio(&minio, &args);
    let stderr = String::from_utf8_lossy(&again.stderr);
    assert!(
        again.status.success() || stderr.contains("BucketAlreadyOwnedByYou"),
        "{stderr}"
    );
    let restored = dir.path().join("restored.txt");
    let download = run_s3m_with_minio(&minio, &["get", &object, restored.to_str().unwrap()]);
    assert!(
        download.status.success(),
        "{}",
        String::from_utf8_lossy(&download.stderr)
    );
    assert_eq!(fs::read(restored).unwrap(), b"survives a repeated cb");
}

#[tokio::test]
async fn test_e2e_create_bucket_accepts_every_canned_acl() {
    let minio = MinioContext::get_or_start().await;

    for acl in ACLS {
        for flag in ["--acl", "-a"] {
            let bucket = OwnedBucket::new(&minio, "acl");
            let target = format!("s3/{}", bucket.name);
            let args = ["cb", target.as_str(), flag, acl];

            assert_created(&run_s3m_with_minio(&minio, &args), &args);
            assert!(listed_buckets(&minio).contains(&bucket.name), "{args:?}");
        }
    }
}

#[tokio::test]
async fn test_e2e_create_bucket_rejects_invalid_names() {
    let minio = MinioContext::get_or_start().await;

    for (target, message) in [
        ("s3/Invalid_Bucket", "Invalid bucket name 'Invalid_Bucket'"),
        ("s3/ab", "Must be 3-63 characters long"),
        ("s3", "Bucket name missing"),
    ] {
        let output = run_s3m_with_minio(&minio, &["cb", target]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{target}: {stderr}");
        assert!(stderr.contains(message), "{target}: {stderr}");
        assert!(output.stdout.is_empty(), "{target}");
    }
}
