//! Miscellaneous e2e tests for s3m binary
//!
//! Tests for:
//! - Binary version
//! - Binary help
//! - Show command (display config hosts)

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::{MinioContext, run_s3m, run_s3m_with_minio};

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_binary_version() {
    // Simple test to verify binary works
    let output = run_s3m(&["--version"]);

    assert!(output.status.success(), "s3m --version should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("s3m"),
        "Version output should contain 's3m'"
    );
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_binary_help() {
    let output = run_s3m(&["--help"]);

    assert!(output.status.success(), "s3m --help should succeed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage: s3m"),
        "Help should contain usage information"
    );
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_show_command() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-show-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Test 'show' command which displays configured hosts
    let output = run_s3m_with_minio(&minio, &["show"]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("SHOW stdout: {}", stdout);
    println!("SHOW stderr: {}", stderr);

    // The show command should display the configured host
    assert!(
        stdout.contains("s3") || stderr.contains("s3"),
        "Show command should display configured hosts"
    );
}
