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

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
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
