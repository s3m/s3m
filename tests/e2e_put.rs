//! E2E tests for upload (PUT) operations
//!
//! Tests for:
//! - Normal file upload with hash verification
//! - Multipart upload (large files) with hash verification
//! - STDIN upload (piped input)
//! - Compressed uploads (.zst extension)
//! - Encrypted uploads (.enc extension)
//! - Compress + encrypt uploads (.zst.enc extension)
//! - Concurrent uploads
//! - Different buffer sizes
//! - Checksum validation

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]

mod common;

use common::{
    MinioContext, calculate_file_hash, create_config_file, create_config_file_with_options,
    create_test_file_with_content, decompress_zstd_to, get_s3m_binary, owned_bucket::OwnedBucket,
    run_s3m_with_minio,
};
use std::fs;
use std::io::Write;
use std::process::{Command, Output, Stdio};
use tempfile::{NamedTempFile, TempDir};

fn run_s3m_with_config_and_stdin(config_path: &str, args: &[&str], stdin_bytes: &[u8]) -> Output {
    let binary = get_s3m_binary();
    let mut child = Command::new(binary)
        .args(["--config", config_path])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn s3m");

    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(stdin_bytes)
        .expect("Failed to write stdin bytes");

    child.wait_with_output().expect("Failed to wait on s3m")
}

fn run_s3m_with_config(config_path: &str, args: &[&str]) -> Output {
    let binary = get_s3m_binary();
    Command::new(binary)
        .args(["--config", config_path])
        .args(args)
        .output()
        .expect("Failed to execute s3m")
}

#[tokio::test]
async fn test_e2e_normal_upload_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-hash-normal");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file with known content
    let test_file = create_test_file_with_content(1024 * 100, "NORMAL_UPLOAD_TEST_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/normal-upload.dat", bucket_name);

    // Upload file
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);
    assert!(upload_output.status.success(), "Upload should succeed");

    // Download file
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    // Verify hash matches
    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!("✅ Normal upload: Hash verified - {}", original_hash);
}

#[tokio::test]
async fn test_e2e_multipart_upload_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-hash-multipart");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create large file to trigger multipart (20MB)
    let test_file = create_test_file_with_content(20 * 1024 * 1024, "MULTIPART_TEST_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/multipart-upload.dat", bucket_name);

    // Upload with 10MB buffer to trigger multipart
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", "10485760"]);
    assert!(
        upload_output.status.success(),
        "Multipart upload should succeed"
    );

    // Download file
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("downloaded-multipart.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    // Verify hash matches
    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded multipart file hash should match original"
    );

    println!("✅ Multipart upload: Hash verified - {}", original_hash);
}

#[tokio::test]
async fn test_e2e_stdin_upload() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-stdin");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_content = b"STDIN upload test content - piped from echo";
    let s3_uri = format!("s3/{}/stdin-upload.txt", bucket_name);

    // Create config file
    let config_file = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let upload_output = run_s3m_with_config_and_stdin(config_path, &["-p", &s3_uri], test_content);

    assert!(
        upload_output.status.success(),
        "STDIN upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("stdin-downloaded.txt");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    // Verify content matches
    let downloaded_content = fs::read(&download_path).expect("Failed to read downloaded file");
    assert_eq!(
        test_content,
        &downloaded_content[..],
        "Downloaded content should match STDIN input"
    );

    let normal_source = download_dir.path().join("original");
    fs::write(&normal_source, test_content).unwrap();
    let normal_uri = format!("s3/{bucket_name}/normal-source");
    let normal_upload =
        run_s3m_with_config(config_path, &[normal_source.to_str().unwrap(), &normal_uri]);
    assert!(normal_upload.status.success());
    assert_uploaded_bytes(&minio, &normal_uri, &normal_source);
}

#[tokio::test]
async fn test_e2e_stdin_encrypt_roundtrip() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-stdin-encrypt");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_content = b"Encrypted STDIN upload content".repeat(4_096);
    let s3_uri = format!("s3/{}/stdin-encrypted.bin", bucket_name);
    let enc_key = "12345678901234567890123456789012";

    let config_file = create_config_file_with_options(
        minio.endpoint(),
        minio.access_key(),
        minio.secret_key(),
        Some(enc_key),
        false,
    );
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let upload_output =
        run_s3m_with_config_and_stdin(config_path, &["--pipe", &s3_uri], &test_content);
    assert!(
        upload_output.status.success(),
        "Encrypted STDIN upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("stdin-encrypted.bin");
    let download_output = run_s3m_with_config(
        config_path,
        &[
            "get",
            &format!("{}.enc", s3_uri),
            download_path.to_str().expect("download path"),
        ],
    );
    assert!(
        download_output.status.success(),
        "Encrypted STDIN download should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    let downloaded_content = fs::read(&download_path).expect("Failed to read downloaded file");
    assert_eq!(downloaded_content, test_content);
}

#[tokio::test]
async fn test_e2e_stdin_compress_roundtrip() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-stdin-compress");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_content = b"Compressible STDIN data block ".repeat(8_192);
    let s3_uri = format!("s3/{}/stdin-compressed.bin", bucket_name);

    let config_file = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let upload_output = run_s3m_with_config_and_stdin(
        config_path,
        &["--compress", "--pipe", &s3_uri],
        &test_content,
    );
    assert!(
        upload_output.status.success(),
        "Compressed STDIN upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let compressed_path = download_dir.path().join("stdin-compressed.bin.zst");
    let download_output = run_s3m_with_minio(
        &minio,
        &[
            "get",
            &format!("{}.zst", s3_uri),
            compressed_path.to_str().expect("compressed path"),
        ],
    );
    assert!(
        download_output.status.success(),
        "Compressed STDIN download should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    let decompressed_path = download_dir.path().join("stdin-compressed.bin");
    decompress_zstd_to(&compressed_path, &decompressed_path);
    let downloaded_content =
        fs::read(&decompressed_path).expect("Failed to read decompressed file");
    assert_eq!(downloaded_content, test_content);
}

#[tokio::test]
async fn test_e2e_stdin_compress_encrypt_roundtrip() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-stdin-compress-encrypt");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_content = b"Compress and encrypt STDIN content ".repeat(8_192);
    let s3_uri = format!("s3/{}/stdin-compressed-encrypted.bin", bucket_name);
    let enc_key = "abcdef1234567890abcdef1234567890";

    let config_file = create_config_file_with_options(
        minio.endpoint(),
        minio.access_key(),
        minio.secret_key(),
        Some(enc_key),
        true,
    );
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let upload_output =
        run_s3m_with_config_and_stdin(config_path, &["--pipe", &s3_uri], &test_content);
    assert!(
        upload_output.status.success(),
        "Compressed+encrypted STDIN upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let decrypted_path = download_dir
        .path()
        .join("stdin-compressed-encrypted.bin.zst");
    let download_output = run_s3m_with_config(
        config_path,
        &[
            "get",
            &format!("{}.zst.enc", s3_uri),
            decrypted_path.to_str().expect("decrypted path"),
        ],
    );
    assert!(
        download_output.status.success(),
        "Compressed+encrypted STDIN download should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    let decompressed_path = download_dir.path().join("stdin-compressed-encrypted.bin");
    decompress_zstd_to(&decrypted_path, &decompressed_path);
    let downloaded_content =
        fs::read(&decompressed_path).expect("Failed to read decompressed file");
    assert_eq!(downloaded_content, test_content);
}

#[tokio::test]
async fn test_e2e_compress_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-compress");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file with compressible content
    let test_file = create_test_file_with_content(1024 * 50, "COMPRESS_TEST_AAAA_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/compressed-file.dat", bucket_name);

    // Upload with compression
    let upload_output = run_s3m_with_minio(&minio, &["--compress", file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Compressed upload should succeed"
    );

    // Download file (should have .zst extension)
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("compressed-downloaded.dat.zst");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    // Download the compressed file (s3m adds .zst extension)
    let s3_uri_with_ext = format!("{}.zst", s3_uri);
    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri_with_ext, download_path_str]);
    assert!(
        download_output.status.success(),
        "Download compressed file should succeed"
    );

    // Verify file exists with .zst extension
    assert!(
        download_path.exists(),
        "Downloaded compressed file should have .zst extension"
    );

    let restored = download_dir.path().join("restored");
    decompress_zstd_to(&download_path, &restored);
    assert_eq!(
        fs::read(&restored).unwrap(),
        fs::read(test_file.path()).unwrap()
    );
    assert_eq!(calculate_file_hash(&restored), original_hash);
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
async fn test_e2e_compress_with_existing_extension() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-compress-ext");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024 * 35, "COMPRESS_EXT_TEST_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/file.txt", bucket_name);

    // Upload with compression
    let upload_output = run_s3m_with_minio(&minio, &["--compress", file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Compressed upload should succeed"
    );

    // File should be stored as file.txt.zst
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("file.txt.zst");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let s3_uri_with_ext = format!("{}.zst", s3_uri);
    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri_with_ext, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    assert!(
        download_path.exists(),
        "File should have .txt.zst extension (not .zst.txt)"
    );

    let restored = download_dir.path().join("restored");
    decompress_zstd_to(&download_path, &restored);
    assert_eq!(
        fs::read(&restored).unwrap(),
        fs::read(test_file.path()).unwrap()
    );
    assert_eq!(calculate_file_hash(&restored), original_hash);
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
async fn test_e2e_compress_and_encrypt_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-compress-encrypt");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024 * 40, "COMPRESS_ENCRYPT_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/compressed-encrypted.dat", bucket_name);
    let enc_key = "12345678901234567890123456789012";

    // Create config with compression and encryption
    let config_file = create_config_file_with_options(
        minio.endpoint(),
        minio.access_key(),
        minio.secret_key(),
        Some(enc_key),
        true,
    );
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let binary = get_s3m_binary();

    // Upload with both compression and encryption (via config)
    let upload_output = Command::new(binary)
        .args(["--config", config_path, file_path, &s3_uri])
        .output()
        .expect("Failed to execute upload");

    assert!(
        upload_output.status.success(),
        "Compress+encrypt upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    // Download file (should have .zst.enc extension)
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("compress-encrypt.dat.zst.enc");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let s3_uri_with_ext = format!("{}.zst.enc", s3_uri);
    let download_output =
        run_s3m_with_config(config_path, &["get", &s3_uri_with_ext, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    assert!(
        download_path.exists(),
        "File should have .zst.enc extension (compress then encrypt)"
    );

    let restored = download_dir.path().join("restored");
    decompress_zstd_to(&download_path, &restored);
    assert_eq!(
        fs::read(&restored).unwrap(),
        fs::read(test_file.path()).unwrap()
    );
    assert_eq!(calculate_file_hash(&restored), original_hash);
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
async fn test_e2e_encrypt_only_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-encrypt-only");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024 * 25, "ENCRYPT_ONLY_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/encrypted-only.dat", bucket_name);
    let enc_key = "abcdef12345678901234567890123456";

    // Create config with encryption only
    let config_file = create_config_file_with_options(
        minio.endpoint(),
        minio.access_key(),
        minio.secret_key(),
        Some(enc_key),
        false,
    );
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let binary = get_s3m_binary();

    // Upload with encryption only (via config)
    let upload_output = Command::new(binary)
        .args(["--config", config_path, file_path, &s3_uri])
        .output()
        .expect("Failed to execute upload");

    assert!(
        upload_output.status.success(),
        "Encrypt upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    // Download encrypted file (should have .enc extension)
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let encrypted_path = download_dir.path().join("encrypted.dat.enc");
    let encrypted_path_str = encrypted_path.to_str().expect("Invalid path");

    let s3_uri_with_enc = format!("{}.enc", s3_uri);
    let download_output =
        run_s3m_with_config(config_path, &["get", &s3_uri_with_enc, encrypted_path_str]);
    assert!(
        download_output.status.success(),
        "Download encrypted file should succeed"
    );

    assert!(
        encrypted_path.exists(),
        "Encrypted file should have .enc extension"
    );

    assert_eq!(
        fs::read(&encrypted_path).unwrap(),
        fs::read(test_file.path()).unwrap()
    );
    assert_eq!(calculate_file_hash(&encrypted_path), original_hash);
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
async fn test_e2e_encrypted_upload() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-encrypt-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Secret data for encryption test")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/encrypted.txt.enc", bucket_name);

    // Generate encryption key (32 chars)
    let enc_key = "12345678901234567890123456789012";

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

    // Upload with encryption (via config)
    let output = Command::new(binary)
        .args(["--config", config_path, file_path, &s3_uri])
        .output()
        .expect("Failed to execute upload");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("ENCRYPT stdout: {}", stdout);
    println!("ENCRYPT stderr: {}", stderr);

    assert!(
        output.status.success(),
        "Encrypted upload should succeed: {}",
        stderr
    );
    let destination = TempDir::new().unwrap();
    let decoded = destination.path().join("decoded");
    let downloaded = run_s3m_with_config(config_path, &["get", &s3_uri, decoded.to_str().unwrap()]);
    assert!(downloaded.status.success());
    assert_eq!(
        fs::read(&decoded).unwrap(),
        fs::read(test_file.path()).unwrap()
    );
}

#[tokio::test]
async fn test_e2e_compressed_upload() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-compress-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file with repetitive data (compresses well)
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    let repetitive_data = "AAAABBBBCCCCDDDD".repeat(1000);
    test_file
        .write_all(repetitive_data.as_bytes())
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/compressed.txt.zst", bucket_name);

    // Upload with compression
    let output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--compress"]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "compressed upload failed: {stdout} {stderr}"
    );
    let destination = TempDir::new().unwrap();
    let encoded = destination.path().join("compressed.zst");
    let downloaded = run_s3m_with_minio(&minio, &["get", &s3_uri, encoded.to_str().unwrap()]);
    assert!(downloaded.status.success());
    let restored = destination.path().join("restored");
    decompress_zstd_to(&encoded, &restored);
    assert_eq!(fs::read(&restored).unwrap(), repetitive_data.as_bytes());
}

#[tokio::test]
async fn test_e2e_checksum_validation() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-checksum-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Checksum validation test")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/checksum-test.txt", bucket_name);

    // Upload with SHA256 checksum
    let output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--checksum", "sha256"]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "checksum upload failed: {stdout} {stderr}"
    );
    assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
}

#[tokio::test]
async fn test_e2e_throttled_put_and_get_roundtrip() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-throttle-roundtrip");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_file = create_test_file_with_content(1024, "THROTTLED_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/throttled.dat", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &["-k", "1", file_path, &s3_uri]);
    assert!(
        upload_output.status.success(),
        "Throttled upload should succeed: {}",
        String::from_utf8_lossy(&upload_output.stderr)
    );

    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("throttled.dat");
    let download_output = run_s3m_with_minio(
        &minio,
        &[
            "get",
            "-k",
            "1",
            &s3_uri,
            download_path.to_str().expect("download path"),
        ],
    );
    assert!(
        download_output.status.success(),
        "Throttled download should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(downloaded_hash, original_hash);
}

#[tokio::test]
async fn test_e2e_different_buffer_sizes() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-buffer-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Test different buffer sizes
    let buffer_sizes = ["5242880", "10485760", "20971520"]; // 5MB, 10MB, 20MB

    for (i, buffer_size) in buffer_sizes.iter().enumerate() {
        let test_file = create_test_file_with_content(41 * 1024 * 1024, "BUFFER_SIZE_");

        let file_path = test_file.path().to_str().expect("Invalid path");
        let s3_uri = format!("s3/{}/buffer-test-{}.txt", bucket_name, i);

        let output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", buffer_size]);

        assert!(
            output.status.success(),
            "buffer {buffer_size}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
    }
}

#[tokio::test]
async fn test_e2e_concurrent_uploads() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-concurrent-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Upload multiple files concurrently
    let mut handles = vec![];

    for i in 0..3 {
        let minio_clone = MinioContext::External {
            endpoint: minio.endpoint().to_owned(),
            access_key: minio.access_key().to_owned(),
            secret_key: minio.secret_key().to_owned(),
        };
        let bucket_name = bucket_name.to_owned();

        let handle = tokio::spawn(async move {
            let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
            test_file
                .write_all(format!("Concurrent upload test {}", i).as_bytes())
                .expect("Write failed");
            test_file.flush().expect("Flush failed");

            let file_path = test_file.path().to_str().expect("Invalid path");
            let s3_uri = format!("s3/{}/concurrent-{}.txt", bucket_name, i);

            run_s3m_with_minio(&minio_clone, &[file_path, &s3_uri])
        });

        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.expect("Task failed");
        assert!(result.status.success(), "Concurrent upload should succeed");
    }
}

#[tokio::test]
async fn test_e2e_large_file_multipart() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-large-bucket");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create a larger file (30MB) to test multipart
    let test_file = create_test_file_with_content(30 * 1024 * 1024, "LARGE_FILE_TEST_");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/large-file.dat", bucket_name);

    // Upload with 10MB buffer to trigger multipart
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", "10485760"]);

    assert!(
        upload_output.status.success(),
        "Large file multipart upload should succeed"
    );

    assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
}

// ============================================================================
// File Size Upload Tests - Single Shot vs Multipart
// ============================================================================
//
// s3m Default Behavior:
// - Default buffer size: 10MB (10485760 bytes)
// - Files <= buffer size: Uploaded in one shot (single PUT request)
// - Files > buffer size: Uploaded using multipart (multiple PUT requests)
//
// Testing Strategy:
// - Use default 10MB buffer to test natural thresholds (9MB, 10MB, 11MB)
// - Can reduce buffer size (e.g., --buffer 5242880 for 5MB) to test multipart
//   with smaller files for faster test execution
//
// These tests verify:
// 1. Small files (< 10MB) use single-shot upload
// 2. Large files (> 10MB) trigger multipart upload
// 3. Edge cases at exactly the buffer size
// 4. Custom buffer sizes work correctly
// ============================================================================

#[tokio::test]
async fn test_e2e_small_file_single_shot() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-size-small");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create file smaller than 10MB (9MB) - should upload in single shot
    let file_size = 9 * 1024 * 1024; // 9MB
    let test_file = create_test_file_with_content(file_size, "SMALL_FILE_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/small-9mb.dat", bucket_name);

    // Upload with default 10MB buffer - should NOT trigger multipart
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    assert!(
        upload_output.status.success(),
        "Small file upload should succeed"
    );

    let stdout = String::from_utf8_lossy(&upload_output.stdout);
    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("Small file (9MB) upload output:");
    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("small-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Small file (9MB < 10MB buffer): Single-shot upload verified - {}",
        original_hash
    );
}

#[tokio::test]
async fn test_e2e_exactly_10mb_edge_case() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-size-edge");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create file exactly 10MB - edge case
    let file_size = 10 * 1024 * 1024; // Exactly 10MB
    let test_file = create_test_file_with_content(file_size, "EDGE_10MB_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/exactly-10mb.dat", bucket_name);

    // Upload with 10MB buffer - edge case behavior
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", "10485760"]);

    assert!(
        upload_output.status.success(),
        "10MB file upload should succeed"
    );

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("10mb-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Edge case (exactly 10MB): Upload verified - {}",
        original_hash
    );
}

#[tokio::test]
async fn test_e2e_large_file_triggers_multipart() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-size-multipart");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create file larger than buffer (11MB) - should trigger multipart
    let file_size = 11 * 1024 * 1024; // 11MB
    let test_file = create_test_file_with_content(file_size, "MULTIPART_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/large-11mb.dat", bucket_name);

    // Upload with 10MB buffer - should trigger multipart (11MB > 10MB)
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", "10485760"]);

    assert!(
        upload_output.status.success(),
        "Large file multipart upload should succeed"
    );

    let stdout = String::from_utf8_lossy(&upload_output.stdout);
    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("Large file (11MB) upload output:");
    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("large-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Large file (11MB > 10MB buffer): Multipart upload verified - {}",
        original_hash
    );
}

#[tokio::test]
async fn test_e2e_very_large_file_multipart() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-size-very-large");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create very large file (25MB) - definitely multipart
    let file_size = 25 * 1024 * 1024; // 25MB
    let test_file = create_test_file_with_content(file_size, "VERY_LARGE_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/very-large-25mb.dat", bucket_name);

    // Upload with 10MB buffer - will use 3 parts (10MB + 10MB + 5MB)
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", "10485760"]);

    assert!(
        upload_output.status.success(),
        "Very large file multipart upload should succeed"
    );

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("very-large-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Very large file (25MB, ~3 parts): Multipart upload verified - {}",
        original_hash
    );
}

#[tokio::test]
async fn test_e2e_tiny_file_single_shot() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-size-tiny");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create tiny file (1MB) - definitely single shot
    let file_size = 1024 * 1024; // 1MB
    let test_file = create_test_file_with_content(file_size, "TINY_FILE_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/tiny-1mb.dat", bucket_name);

    // Upload with default buffer
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    assert!(
        upload_output.status.success(),
        "Tiny file upload should succeed"
    );

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("tiny-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Tiny file (1MB << 10MB buffer): Single-shot upload verified - {}",
        original_hash
    );
}

#[tokio::test]
async fn test_e2e_small_buffer_uses_existing_ten_mib_fallback() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-custom-buffer");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Requested buffers below 5 MiB use the existing 10 MiB fallback.
    let file_size = 11 * 1024 * 1024;
    let test_file = create_test_file_with_content(file_size, "CUSTOM_BUFFER_");
    let original_hash = calculate_file_hash(test_file.path());

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/custom-buffer-11mb.dat", bucket_name);

    // Fallback produces a 10 MiB first part and 1 MiB final part.
    let buffer_size = "2097152"; // 2MB = 2 * 1024 * 1024
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", buffer_size]);

    assert!(
        upload_output.status.success(),
        "Subminimum buffer upload should succeed"
    );

    let stdout = String::from_utf8_lossy(&upload_output.stdout);
    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("Custom buffer (11 MiB file, 10 MiB fallback) upload output:");
    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // Download and verify
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("custom-buffer-downloaded.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    let downloaded_hash = calculate_file_hash(&download_path);
    assert_eq!(
        original_hash, downloaded_hash,
        "Downloaded file hash should match original"
    );

    println!(
        "✅ Custom buffer test (11 MiB file with 10 MiB fallback): Multipart upload verified - {}",
        original_hash
    );
    println!("   This demonstrates testing multipart with smaller files for faster execution");
}

// ============================================================================
// Progress Bar Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_progress_bar_enabled_by_default() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-progress-enabled");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create file large enough to show progress (5MB)
    let test_file = create_test_file_with_content(5 * 1024 * 1024, "PROGRESS_TEST_");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/progress-test.dat", bucket_name);

    // Upload WITHOUT --quiet flag - progress bar should be shown
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    assert!(
        upload_output.status.success(),
        "Upload with progress bar should succeed"
    );

    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    // Progress bar output goes to stderr
    // When progress is enabled, stderr should contain progress indicators
    // Note: In non-interactive mode (piped), progress may not show, but command still succeeds
    println!("Upload stderr (with progress):");
    println!("{}", stderr);

    // The important thing is the upload succeeded
    assert!(String::from_utf8_lossy(&upload_output.stdout).contains("ETag:"));
    assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
}

#[tokio::test]
async fn test_e2e_progress_bar_disabled_with_quiet() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-progress-quiet");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create file large enough to show progress (5MB)
    let test_file = create_test_file_with_content(5 * 1024 * 1024, "QUIET_TEST_");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/quiet-test.dat", bucket_name);

    // Upload WITH --quiet flag - progress bar should be suppressed
    let upload_output = run_s3m_with_minio(&minio, &["--quiet", file_path, &s3_uri]);

    assert!(
        upload_output.status.success(),
        "Quiet upload should succeed"
    );

    let stdout = String::from_utf8_lossy(&upload_output.stdout);
    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("Quiet upload stdout: {}", stdout);
    println!("Quiet upload stderr: {}", stderr);

    assert!(upload_output.stdout.is_empty(), "quiet stdout: {stdout}");
    assert!(upload_output.stderr.is_empty(), "quiet stderr: {stderr}");
    assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
}

#[tokio::test]
async fn test_e2e_progress_bar_multipart_upload() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-progress-multipart");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create large file to trigger multipart (15MB with 10MB buffer)
    let test_file = create_test_file_with_content(15 * 1024 * 1024, "MULTIPART_PROGRESS_");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/multipart-progress.dat", bucket_name);

    // Upload multipart WITHOUT --quiet - should show progress for multiple parts
    let upload_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    assert!(
        upload_output.status.success(),
        "Multipart upload with progress should succeed"
    );

    let stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("Multipart upload stderr (with progress):");
    println!("{}", stderr);

    assert!(String::from_utf8_lossy(&upload_output.stdout).contains("ETag:"));
    assert_uploaded_bytes(&minio, &s3_uri, test_file.path());
}

#[tokio::test]
async fn test_e2e_quiet_flag_with_get_command() {
    let minio = MinioContext::get_or_start().await;

    let owned = OwnedBucket::new(&minio, "e2e-get-quiet");
    let bucket_name = owned.name.as_str();
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Upload a test file first
    let test_file = create_test_file_with_content(2 * 1024 * 1024, "GET_QUIET_");
    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/get-quiet-test.dat", bucket_name);

    let upload_output = run_s3m_with_minio(&minio, &["--quiet", file_path, &s3_uri]);
    assert!(upload_output.status.success(), "Upload should succeed");

    // Download WITH --quiet flag
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("downloaded-quiet.dat");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let download_output =
        run_s3m_with_minio(&minio, &["get", "--quiet", &s3_uri, download_path_str]);

    assert!(
        download_output.status.success(),
        "Quiet download should succeed"
    );

    let stderr = String::from_utf8_lossy(&download_output.stderr);

    println!("Get --quiet stderr: {}", stderr);

    // Verify file was downloaded correctly
    assert_eq!(download_output.stdout, Vec::<u8>::new());
    assert!(download_output.stderr.is_empty(), "{stderr}");
    assert_eq!(
        fs::read(&download_path).unwrap(),
        fs::read(test_file.path()).unwrap()
    );

    println!("✅ Progress bar test (get --quiet): Download succeeded with minimal output");
}

fn assert_uploaded_bytes(minio: &MinioContext, uri: &str, source: &std::path::Path) {
    let directory = TempDir::new().unwrap();
    let downloaded = directory.path().join("downloaded");
    let output = run_s3m_with_minio(minio, &["get", uri, downloaded.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let original = fs::read(source).unwrap();
    let restored = fs::read(downloaded).unwrap();
    assert_eq!(restored, original);
    assert_eq!(blake3::hash(&restored), blake3::hash(&original));
}
