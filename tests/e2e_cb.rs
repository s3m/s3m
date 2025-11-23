//! E2E tests for `cb` (create bucket) command

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, run_s3m_with_minio};

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_create_bucket() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-test-bucket";

    // Create bucket using s3m (format: s3/<bucket-name> where 's3' is the host from config.yml)
    let output = run_s3m_with_minio(&minio, &["cb", &format!("s3/{}", bucket_name)]);

    // May succeed or return error if bucket exists
    let stderr = String::from_utf8_lossy(&output.stderr);
    let success = output.status.success()
        || stderr.contains("BucketAlreadyOwnedByYou")
        || stderr.contains("BucketAlreadyExists");

    assert!(success, "Bucket creation should succeed or already exist");
}
