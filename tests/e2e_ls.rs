//! E2E tests for `ls` (list objects) command

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, run_s3m_with_minio};
use std::io::Write;
use tempfile::NamedTempFile;

/// A bucket this test created; dropping it removes the bucket and its objects.
struct OwnedBucket<'a> {
    minio: &'a MinioContext,
    name: String,
}

impl<'a> OwnedBucket<'a> {
    async fn create(minio: &'a MinioContext, label: &str) -> Self {
        let name = format!("e2e-ls-{label}-{:016x}", rand::random::<u64>());
        minio.create_bucket(&name).await.expect("Bucket creation");
        Self { minio, name }
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

fn listed_json_keys(output: &std::process::Output) -> Vec<String> {
    assert!(
        output.status.success(),
        "ls failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON stdout");
    stdout
        .get("objects")
        .and_then(serde_json::Value::as_array)
        .expect("objects array")
        .iter()
        .map(|object| object["key"].as_str().expect("key").to_string())
        .collect()
}

#[tokio::test]
async fn test_e2e_ls_number_is_a_total_and_combines_with_prefix() {
    let minio = MinioContext::get_or_start().await;
    let owned = OwnedBucket::create(&minio, "number").await;
    let bucket = &owned.name;

    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(b"ls --number").expect("Write failed");
    file.flush().expect("Flush failed");
    let file_path = file.path().to_str().expect("Invalid path");
    for key in ["a.txt", "logs/1.txt", "logs/2.txt", "logs/3.txt", "z.txt"] {
        let upload = run_s3m_with_minio(&minio, &[file_path, &format!("s3/{bucket}/{key}")]);
        assert!(upload.status.success(), "upload {key} failed");
    }

    let limited = run_s3m_with_minio(
        &minio,
        &["ls", &format!("s3/{bucket}"), "--number", "2", "--json"],
    );
    let prefixed = run_s3m_with_minio(
        &minio,
        &[
            "ls",
            &format!("s3/{bucket}"),
            "--prefix",
            "logs/",
            "-n",
            "2",
            "--json",
        ],
    );

    assert_eq!(listed_json_keys(&limited), ["a.txt", "logs/1.txt"]);
    assert_eq!(listed_json_keys(&prefixed), ["logs/1.txt", "logs/2.txt"]);
}

#[tokio::test]
async fn test_e2e_list_objects() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-list-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Upload a test file first
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Test file for listing")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/test-list-file.txt", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Upload should succeed before list"
    );

    // List objects in bucket
    let list_output = run_s3m_with_minio(&minio, &["ls", &format!("s3/{}", bucket_name)]);

    let stdout = String::from_utf8_lossy(&list_output.stdout);

    println!("LS stdout: {}", stdout);

    // Should contain the uploaded file name
    assert!(
        stdout.contains("test-list-file.txt"),
        "List output should contain uploaded file"
    );
}
