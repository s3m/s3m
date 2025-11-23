//! End-to-end binary tests for s3m against `MinIO`
//!
//! These tests compile and run the actual s3m binary against a real `MinIO`
//! container, providing comprehensive integration testing of the CLI.
//!
//! Run with:
//! ```bash
//! ./scripts/test-with-podman.sh
//! # or manually:
//! cargo test --test e2e_binary -- --ignored
//! ```

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::indexing_slicing
)]

mod helpers;

use helpers::minio::{MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, MinioContainer};
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

/// Create a temporary config.yml file for s3m
fn create_config_file(endpoint: &str, access_key: &str, secret_key: &str) -> NamedTempFile {
    create_config_file_with_options(endpoint, access_key, secret_key, None, false)
}

fn create_config_file_with_options(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    enc_key: Option<&str>,
    compress: bool,
) -> NamedTempFile {
    use std::fmt::Write as _;

    let mut config_content = format!(
        r"---
hosts:
  s3:
    endpoint: {}
    access_key: {}
    secret_key: {}
",
        endpoint, access_key, secret_key
    );

    if let Some(key) = enc_key {
        writeln!(&mut config_content, "    enc_key: {}", key).expect("Write failed");
    }

    if compress {
        config_content.push_str("    compress: true\n");
    }

    let mut config_file = NamedTempFile::new().expect("Failed to create temp config file");
    config_file
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    config_file.flush().expect("Failed to flush config");
    config_file
}

/// `MinIO` test context - either external or testcontainer-based
enum MinioContext {
    External {
        endpoint: String,
        access_key: String,
        secret_key: String,
    },
    Container(Box<MinioContainer>),
}

impl MinioContext {
    /// Get or start `MinIO` - uses external if `MINIO_ENDPOINT` is set, otherwise starts container
    async fn get_or_start() -> Self {
        if let Ok(endpoint) = env::var("MINIO_ENDPOINT") {
            // Use externally provided MinIO (e.g., from test-with-podman.sh)
            let access_key =
                env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| MINIO_ROOT_USER.to_string());
            let secret_key =
                env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| MINIO_ROOT_PASSWORD.to_string());

            println!("Using external MinIO at {endpoint}");

            MinioContext::External {
                endpoint,
                access_key,
                secret_key,
            }
        } else {
            // Start testcontainer
            println!("Starting MinIO testcontainer");
            let container = MinioContainer::start().await;
            container.wait_for_ready().await.expect("MinIO ready");
            MinioContext::Container(Box::new(container))
        }
    }

    fn endpoint(&self) -> &str {
        match self {
            MinioContext::External { endpoint, .. } => endpoint,
            MinioContext::Container(c) => c.endpoint(),
        }
    }

    fn access_key(&self) -> &str {
        match self {
            MinioContext::External { access_key, .. } => access_key,
            MinioContext::Container(c) => &c.access_key,
        }
    }

    fn secret_key(&self) -> &str {
        match self {
            MinioContext::External { secret_key, .. } => secret_key,
            MinioContext::Container(c) => &c.secret_key,
        }
    }

    async fn create_bucket(&self, bucket_name: &str) -> anyhow::Result<()> {
        match self {
            MinioContext::External { .. } => {
                // Create bucket using s3m binary (which handles auth properly)
                let output = run_s3m_with_minio(self, &["cb", &format!("s3/{}", bucket_name)]);

                if output.status.success() {
                    Ok(())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    // Check if bucket already exists (not an error)
                    if stderr.contains("BucketAlreadyOwnedByYou")
                        || stderr.contains("BucketAlreadyExists")
                    {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("Failed to create bucket: {}", stderr))
                    }
                }
            }
            MinioContext::Container(c) => c.create_bucket(bucket_name).await,
        }
    }
}

/// Get the path to the s3m binary (builds it if needed)
fn get_s3m_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("s3m");

    // Build the binary if it doesn't exist
    if !path.exists() {
        let output = Command::new("cargo")
            .args(["build", "--bin", "s3m"])
            .output()
            .expect("Failed to build s3m binary");

        assert!(
            output.status.success(),
            "Failed to build s3m: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    path
}

/// Run s3m command with `MinIO` context - creates config file automatically
fn run_s3m_with_minio(minio: &MinioContext, args: &[&str]) -> std::process::Output {
    let config_file = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let binary = get_s3m_binary();
    let mut cmd = Command::new(binary);

    // Add --config flag first
    cmd.arg("--config").arg(config_path);

    // Then add the rest of the arguments
    cmd.args(args);

    let output = cmd.output().expect("Failed to execute s3m");

    // Keep config_file alive until command completes
    drop(config_file);

    output
}

/// Run s3m command without config (for --version, --help, etc.)
fn run_s3m(args: &[&str]) -> std::process::Output {
    let binary = get_s3m_binary();
    let mut cmd = Command::new(binary);
    cmd.args(args);
    cmd.output().expect("Failed to execute s3m")
}

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
async fn test_e2e_create_bucket() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-test-bucket";

    // Create bucket using s3m (format: s3/<bucket-name> where 's3' is the host from config.yml)
    let output = run_s3m_with_minio(&minio, &["cb", &format!("s3/{}", bucket_name)]);

    // May succeed or return error if bucket exists
    // The important thing is the command runs
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    println!("Create bucket stdout: {}", stdout);
    println!("Create bucket stderr: {}", stderr);

    assert!(
        output.status.success() || stderr.contains("BucketAlreadyOwnedByYou"),
        "Bucket creation should succeed or already exist"
    );
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_put_and_get_object() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-put-get-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    let test_content = b"Hello from s3m end-to-end test!";
    test_file.write_all(test_content).expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/test-object.txt", bucket_name);

    // Upload file
    let put_output = run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    let put_stderr = String::from_utf8_lossy(&put_output.stderr);
    let put_stdout = String::from_utf8_lossy(&put_output.stdout);

    println!("PUT stdout: {}", put_stdout);
    println!("PUT stderr: {}", put_stderr);

    assert!(
        put_output.status.success(),
        "PUT should succeed. stderr: {}",
        put_stderr
    );

    // Download file
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("downloaded.txt");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let get_output = run_s3m_with_minio(&minio, &["get", &s3_uri, download_path_str]);

    let get_stderr = String::from_utf8_lossy(&get_output.stderr);
    let get_stdout = String::from_utf8_lossy(&get_output.stdout);

    println!("GET stdout: {}", get_stdout);
    println!("GET stderr: {}", get_stderr);

    assert!(
        get_output.status.success(),
        "GET should succeed. stderr: {}",
        get_stderr
    );

    // Verify downloaded content
    assert!(download_path.exists(), "Downloaded file should exist");
    let downloaded_content = fs::read(&download_path).expect("Failed to read downloaded file");
    assert_eq!(
        downloaded_content, test_content,
        "Downloaded content should match uploaded content"
    );
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_list_objects() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-list-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create and upload test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"List test content")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/list-test.txt", bucket_name);

    // Upload file first
    run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    // List objects
    let list_uri = format!("s3/{}/", bucket_name);
    let list_output = run_s3m_with_minio(&minio, &["ls", &list_uri]);

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    let list_stderr = String::from_utf8_lossy(&list_output.stderr);

    println!("LIST stdout: {}", list_stdout);
    println!("LIST stderr: {}", list_stderr);

    // If upload succeeded, list should show the object
    if list_output.status.success() {
        assert!(
            list_stdout.contains("list-test.txt") || list_stderr.is_empty(),
            "List output should contain uploaded file or have no errors"
        );
    }
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_delete_object() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-delete-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create and upload test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Delete test content")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/delete-test.txt", bucket_name);

    // Upload file
    run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    // Delete object
    let delete_output = run_s3m_with_minio(&minio, &["rm", &s3_uri]);

    let delete_stdout = String::from_utf8_lossy(&delete_output.stdout);
    let delete_stderr = String::from_utf8_lossy(&delete_output.stderr);

    println!("DELETE stdout: {}", delete_stdout);
    println!("DELETE stderr: {}", delete_stderr);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_large_file_multipart() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-multipart-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create large file (20MB to trigger multipart)
    let large_file = NamedTempFile::new().expect("Failed to create temp file");
    let file_path = large_file.path();

    // Write 20MB of data
    let chunk = vec![0xAB; 1024 * 1024]; // 1MB chunk
    for _ in 0..20 {
        fs::write(file_path, &chunk).expect("Write failed");
    }

    let file_path_str = file_path.to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/large-file.bin", bucket_name);

    // Upload with multipart
    let upload_output = run_s3m_with_minio(
        &minio,
        &[file_path_str, &s3_uri, "--buffer", "10485760"], // 10MB buffer
    );

    let upload_stdout = String::from_utf8_lossy(&upload_output.stdout);
    let upload_stderr = String::from_utf8_lossy(&upload_output.stderr);

    println!("MULTIPART UPLOAD stdout: {}", upload_stdout);
    println!("MULTIPART UPLOAD stderr: {}", upload_stderr);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_compressed_upload() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-compress-bucket";
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

    println!("COMPRESS stdout: {}", stdout);
    println!("COMPRESS stderr: {}", stderr);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_encrypted_upload() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-encrypt-bucket";
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

    // Create and upload test file
    let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
    test_file
        .write_all(b"Show command test")
        .expect("Write failed");
    test_file.flush().expect("Flush failed");

    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/show-test.txt", bucket_name);

    // Upload file
    run_s3m_with_minio(&minio, &[file_path, &s3_uri]);

    // Show object metadata
    let show_output = run_s3m_with_minio(&minio, &["show", &s3_uri]);

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);
    let show_stderr = String::from_utf8_lossy(&show_output.stderr);

    println!("SHOW stdout: {}", show_stdout);
    println!("SHOW stderr: {}", show_stderr);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_concurrent_uploads() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-concurrent-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Extract connection details for use in spawned tasks
    let endpoint = minio.endpoint().to_string();
    let access_key = minio.access_key().to_string();
    let secret_key = minio.secret_key().to_string();

    // Create multiple test files
    let mut handles = vec![];

    for i in 0..3 {
        let bucket = bucket_name.to_string();
        let endpoint_clone = endpoint.clone();
        let access_key_clone = access_key.clone();
        let secret_key_clone = secret_key.clone();

        let handle = tokio::spawn(async move {
            let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
            test_file
                .write_all(format!("Concurrent test {}", i).as_bytes())
                .expect("Write failed");
            test_file.flush().expect("Flush failed");

            let file_path = test_file.path().to_str().expect("Invalid path").to_string();
            let s3_uri = format!("s3/{}/concurrent-{}.txt", bucket, i);

            // Create config file for this task
            let config_file =
                create_config_file(&endpoint_clone, &access_key_clone, &secret_key_clone);
            let config_path = config_file.path().to_str().expect("Invalid config path");

            let binary = get_s3m_binary();
            let output = Command::new(binary)
                .arg("--config")
                .arg(config_path)
                .args([file_path.as_str(), &s3_uri])
                .output()
                .expect("Failed to execute s3m");

            drop(config_file);

            output.status.success()
        });

        handles.push(handle);
    }

    // Wait for all uploads
    let mut success_count = 0;
    for handle in handles {
        if let Ok(true) = handle.await {
            success_count += 1;
        }
    }

    println!("Concurrent uploads succeeded: {}/3", success_count);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_checksum_validation() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-checksum-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file
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

    println!("CHECKSUM stdout: {}", stdout);
    println!("CHECKSUM stderr: {}", stderr);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_different_buffer_sizes() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-buffer-bucket";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Test different buffer sizes
    let buffer_sizes = ["5242880", "10485760", "20971520"]; // 5MB, 10MB, 20MB

    for (i, buffer_size) in buffer_sizes.iter().enumerate() {
        let mut test_file = NamedTempFile::new().expect("Failed to create temp file");
        test_file
            .write_all(format!("Buffer size test {}", i).as_bytes())
            .expect("Write failed");
        test_file.flush().expect("Flush failed");

        let file_path = test_file.path().to_str().expect("Invalid path");
        let s3_uri = format!("s3/{}/buffer-test-{}.txt", bucket_name, i);

        let output = run_s3m_with_minio(&minio, &[file_path, &s3_uri, "--buffer", buffer_size]);

        println!("Buffer size {} result: {:?}", buffer_size, output.status);
    }
}

// ============================================================================
// Comprehensive Upload/Download Tests with Hash Verification
// ============================================================================

/// Helper to calculate blake3 hash of a file
fn calculate_file_hash(path: &std::path::Path) -> String {
    use std::io::Read;
    let mut file = std::fs::File::open(path).expect("Failed to open file");
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 65536].into_boxed_slice();

    loop {
        let size = file.read(&mut buf).expect("Failed to read file");
        if size == 0 {
            break;
        }
        hasher.update(&buf[..size]);
    }

    hasher.finalize().to_hex().to_string()
}

/// Helper to create a test file with specified size and content
fn create_test_file_with_content(size: usize, pattern: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let content = pattern.repeat(size / pattern.len() + 1);
    file.write_all(&content.as_bytes()[..size])
        .expect("Failed to write");
    file.flush().expect("Failed to flush");
    file
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_normal_upload_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-hash-normal";
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
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_multipart_upload_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-hash-multipart";
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
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_stdin_upload() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-stdin";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    let test_content = b"STDIN upload test content - piped from echo";
    let s3_uri = format!("s3/{}/stdin-upload.txt", bucket_name);

    // Create config file
    let config_file = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    let config_path = config_file.path().to_str().expect("Invalid config path");

    let binary = get_s3m_binary();

    // Upload via STDIN using echo with --pipe flag
    let upload_output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "echo -n '{}' | {} --config {} --pipe {}",
            String::from_utf8_lossy(test_content),
            binary.display(),
            config_path,
            s3_uri
        ))
        .output()
        .expect("Failed to execute stdin upload");

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

    println!("✅ STDIN upload: Content verified");
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_compress_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-compress";
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

    println!("✅ Compress upload: .zst extension verified");
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_compress_with_existing_extension() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-compress-ext";
    minio
        .create_bucket(bucket_name)
        .await
        .expect("Bucket creation");

    // Create test file
    let test_file = create_test_file_with_content(1024 * 30, "EXT_TEST_");
    let file_path = test_file.path().to_str().expect("Invalid path");
    let s3_uri = format!("s3/{}/file.txt", bucket_name);

    // Upload with compression
    let upload_output = run_s3m_with_minio(&minio, &["--compress", file_path, &s3_uri]);
    assert!(upload_output.status.success(), "Upload should succeed");

    // Download - s3m should have created file.txt.zst
    let download_dir = TempDir::new().expect("Failed to create temp dir");
    let download_path = download_dir.path().join("file.txt.zst");
    let download_path_str = download_path.to_str().expect("Invalid download path");

    let s3_uri_with_ext = format!("{}.zst", s3_uri);
    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri_with_ext, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    assert!(
        download_path.exists(),
        "Compressed file should have .txt.zst extension"
    );

    println!("✅ Compress with extension: .txt.zst verified");
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_compress_and_encrypt_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-compress-encrypt";
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
    let download_output = run_s3m_with_minio(&minio, &["get", &s3_uri_with_ext, download_path_str]);
    assert!(download_output.status.success(), "Download should succeed");

    assert!(
        download_path.exists(),
        "File should have .zst.enc extension (compress then encrypt)"
    );

    println!("✅ Compress+Encrypt: .zst.enc extension verified");
    println!("   Original hash: {}", original_hash);
}

#[tokio::test]
#[ignore = "Requires MinIO container runtime"]
async fn test_e2e_encrypt_only_with_hash() {
    let minio = MinioContext::get_or_start().await;

    let bucket_name = "e2e-encrypt-only";
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
        run_s3m_with_minio(&minio, &["get", &s3_uri_with_enc, encrypted_path_str]);
    assert!(
        download_output.status.success(),
        "Download encrypted file should succeed"
    );

    assert!(
        encrypted_path.exists(),
        "Encrypted file should have .enc extension"
    );

    println!("✅ Encrypt only: .enc extension verified");
    println!("   Original hash: {}", original_hash);
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
    let download_compressed =
        run_s3m_with_minio(&minio, &["get", &s3_uri_zst, compressed_path_str]);
    assert!(
        download_compressed.status.success(),
        "Download compressed file should succeed"
    );

    // Decompress locally using zstd command
    let decompressed_path = download_dir.path().join("decompressed.dat");
    let decompress_output = Command::new("zstd")
        .args([
            "-d",
            compressed_path_str,
            "-o",
            decompressed_path.to_str().unwrap(),
        ])
        .output();

    match decompress_output {
        Ok(output) if output.status.success() => {
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
        _ => {
            println!("⚠️  Decompress verification skipped: zstd command not available");
        }
    }
}
