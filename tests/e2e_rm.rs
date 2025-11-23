//! E2E tests for `rm` (delete object) command

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
async fn test_e2e_delete_object() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-delete-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Upload a test file first
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"File to be deleted")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/to-delete.txt", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Upload should succeed before delete"
    );

    // Delete the object
    let delete_output = run_s3m_with_minio(&minio, &["rm", &s3_uri]);

    let stdout = String::from_utf8_lossy(&delete_output.stdout);
    let stderr = String::from_utf8_lossy(&delete_output.stderr);

    println!("DELETE stdout: {}", stdout);
    println!("DELETE stderr: {}", stderr);

    // Delete should succeed (or object already doesn't exist)
    assert!(
        delete_output.status.success(),
        "Delete operation should succeed"
    );
}
