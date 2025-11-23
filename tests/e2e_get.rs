//! E2E tests for `get` (download/retrieve) command
//!
//! Tests for:
//! - Basic get operation
//! - Download and decrypt encrypted files
//! - Download and decompress compressed files

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc,
    clippy::indexing_slicing
)]

mod common;

use common::{
    MinioContext, calculate_file_hash, create_config_file_with_options,
    create_test_file_with_content, get_s3m_binary, run_s3m_with_minio,
};
use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_put_and_get_object() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-get-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create and upload a test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Test content for get operation")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/get-test.txt", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);
    assert!(upload_output.status.success(), "Upload should succeed");

    // Download the file
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("downloaded.txt");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);

    assert!(download_output.status.success(), "Download should succeed");

    // Verify downloaded content
    let downloaded_content = std::fs::read_to_string(&download_path).expect("Failed to read");
    assert_eq!(
        downloaded_content, "Test content for get operation",
        "Downloaded content should match"
    );
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_decrypt_and_verify_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-decrypt-verify";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024 * 30, "DECRYPT_VERIFY_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/decrypt-test.dat", bucket_name);
    let enc_key = "fedcba09876543210987654321098765";

    // Create config with encryption
    let config_file = create_config_file_with_options(
        minio.endpoint(),
        minio.access_key(),
        minio.secret_key(),
        Some(enc_key),
        false,
    );
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let binary = get_s3m_binary();

    // Upload encrypted (via config)
    let upload_output = Command::new(&binary)
        .args(["--config", config_path, file_path, &s3_uri])
        .output()
        .expect("Failed to execute upload");

    assert!(
        upload_output.status.success(),
        "Upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    // Download and decrypt (using get subcommand with config)
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let decrypted_path = download_dir.path().join("decrypted.dat");
    let decrypted_path_str = decrypted_path.to_str().expect("Invalid path");

    let s3_uri_enc = format!("{}.enc", s3_uri);
    let download_output = Command::new(&binary)
        .args([
            "--config",
            config_path,
            "get",
            &s3_uri_enc,
            decrypted_path_str,
        ])
        .output()
        .expect("Failed to execute download");

    assert!(
        download_output.status.success(),
        "Download and decrypt should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    // Verify decrypted hash matches original
    let decrypted_hash = calculate_file_hash(&decrypted_path);
    assert_eq!(
        original_hash, decrypted_hash,
        "Decrypted file hash should match original"
    );

    println!("✅ Decrypt verification: Hash matches - {}", original_hash);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_decompress_and_verify_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-decompress-verify";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024 * 60, "DECOMPRESS_VERIFY_AAAA_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/decompress-test.dat", bucket_name);

    // Upload compressed
    let upload_output = run_s3m_with_minio(&minio, &["--compress", file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Compressed upload should succeed"
    );

    // Download compressed file as-is (don't decompress)
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let compressed_path = download_dir.path().join("compressed.dat.zst");
    let compressed_path_str = compressed_path.to_str().expect("Invalid path");

    let s3_uri_zst = format!("{}.zst", s3_uri);
    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri_zst, compressed_path_str]);
    assert!(
        download_output.status.success(),
        "Download compressed file should succeed"
    );

    // Decompress using zstd command
    let decompressed_path = download_dir.path().join("decompressed.dat");
    let decompressed_path_str = decompressed_path.to_str().expect("Invalid path");

    let decompress_output = Command::new("zstd")
        .args(["-d", compressed_path_str, "-o", decompressed_path_str])
        .output()
        .expect("Failed to decompress (is zstd installed?)");

    assert!(
        decompress_output.status.success(),
        "Decompression should succeed"
    );

    // Verify decompressed hash matches original
    let decompressed_hash = calculate_file_hash(&decompressed_path);
    assert_eq!(
        original_hash, decompressed_hash,
        "Decompressed file hash should match original"
    );

    println!(
        "✅ Decompress verification: Hash matches - {}",
        original_hash
    );
}
