//! E2E tests for `get` (download/retrieve) command
//!
//! Tests for:
//! - Basic get operation
//! - Download and decrypt encrypted files
//! - Download and decompress compressed files
//! - Overwrite refusal and --force, metadata and versions without a body
//! - Service failures and presigned `share` URLs served by the provider

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc,
    clippy::indexing_slicing
)]

mod common;

use common::{
    MinioContext, calculate_file_hash, create_config_file, create_config_file_with_options,
    create_test_file_with_content, decompress_zstd_to, get_s3m_binary, run_s3m_with_minio,
};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};
use tempfile::{NamedTempFile, TempDir};

/// A bucket this test run created; it is removed even when the test fails.
struct OwnedBucket<'a> {
    minio: &'a MinioContext,
    name: String,
}

impl<'a> OwnedBucket<'a> {
    async fn create(minio: &'a MinioContext, label: &str) -> Self {
        let name = format!("e2e-get-{label}-{:016x}", rand::random::<u64>());
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

/// Run s3m from `cwd`, where `get` writes when no destination is given.
fn run_in(minio: &MinioContext, cwd: &Path, args: &[&str]) -> Output {
    let config = create_config_file(minio.endpoint(), minio.access_key(), minio.secret_key());
    Command::new(get_s3m_binary())
        .current_dir(cwd)
        .arg("--config")
        .arg(config.path())
        .args(args)
        .output()
        .expect("Failed to execute s3m")
}

fn assert_success(output: &Output, args: &[&str]) {
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test]
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

    // Decompress without depending on a system zstd binary.
    let decompressed_path = download_dir.path().join("decompressed.dat");
    decompress_zstd_to(&compressed_path, &decompressed_path);

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

#[tokio::test]
async fn test_e2e_get_refuses_existing_file_unless_forced() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::create(&minio, "force").await;
    let source = create_test_file_with_content(64 * 1024, "FORCE_OVERWRITE_");
    let uri = format!("s3/{}/force.dat", bucket.name);
    let upload_args = ["-q", source.path().to_str().unwrap(), uri.as_str()];
    assert_success(&run_s3m_with_minio(&minio, &upload_args), &upload_args);

    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("existing.dat");
    let local = b"local data that must survive a refused get";
    fs::write(&dest, local).unwrap();
    let dest_str = dest.to_str().unwrap();

    let refused = run_s3m_with_minio(&minio, &["get", &uri, dest_str]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&refused.stderr).contains("already exists"));
    assert_eq!(String::from_utf8_lossy(&refused.stdout), "");
    assert_eq!(fs::read(&dest).unwrap(), local);

    for flag in ["--force", "-f"] {
        fs::write(&dest, local).unwrap();
        let args = ["get", uri.as_str(), dest_str, flag, "-q"];
        let output = run_s3m_with_minio(&minio, &args);
        assert_success(&output, &args);
        assert!(output.stdout.is_empty(), "{args:?}");
        assert_eq!(
            calculate_file_hash(&dest),
            calculate_file_hash(source.path()),
            "{args:?}"
        );
    }
}

#[tokio::test]
async fn test_e2e_get_meta_and_versions_skip_the_body() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::create(&minio, "meta").await;
    let source = create_test_file_with_content(4096, "META_");
    let uri = format!("s3/{}/dir/meta.dat", bucket.name);
    let upload_args = [
        "-q",
        "-m",
        "owner=alice",
        source.path().to_str().unwrap(),
        &uri,
    ];
    assert_success(&run_s3m_with_minio(&minio, &upload_args), &upload_args);
    let cwd = TempDir::new().unwrap();

    let text = run_in(&minio, cwd.path(), &["get", &uri, "-m"]);
    assert_success(&text, &["get -m"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    let fields: Vec<(&str, &str)> = stdout
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name, value.trim()))
        .collect();
    assert!(fields.contains(&("content-length", "4096")), "{stdout}");
    assert!(fields.contains(&("x-amz-meta-owner", "alice")), "{stdout}");

    let json = run_in(&minio, cwd.path(), &["get", &uri, "--meta", "--json"]);
    assert_success(&json, &["get --meta --json"]);
    let meta: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(meta.get("bucket"), Some(&Value::from(bucket.name.as_str())));
    assert_eq!(meta.get("key"), Some(&Value::from("dir/meta.dat")));
    assert_eq!(meta.get("content_length"), Some(&Value::from(4096)));
    assert_eq!(
        meta.get("metadata"),
        Some(&serde_json::json!({"owner": "alice"}))
    );

    let versions = run_in(&minio, cwd.path(), &["get", &uri, "--versions", "--json"]);
    assert_success(&versions, &["get --versions --json"]);
    let listing: Value = serde_json::from_slice(&versions.stdout).unwrap();
    let entries_json = listing.get("versions").and_then(Value::as_array).unwrap();
    assert_eq!(entries_json.len(), 1, "{listing}");
    let version = entries_json.first().unwrap();
    assert_eq!(version.get("key"), Some(&Value::from("dir/meta.dat")));
    assert_eq!(version.get("size_bytes"), Some(&Value::from(4096)));
    assert_eq!(version.get("is_latest"), Some(&Value::from(true)));

    assert_eq!(entries(cwd.path()), Vec::<String>::new());
}

#[tokio::test]
async fn test_e2e_get_service_failures_leave_no_file() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::create(&minio, "failure").await;
    let source = create_test_file_with_content(1024, "DENIED_");
    let uri = format!("s3/{}/present.dat", bucket.name);
    let upload_args = ["-q", source.path().to_str().unwrap(), uri.as_str()];
    assert_success(&run_s3m_with_minio(&minio, &upload_args), &upload_args);
    let cwd = TempDir::new().unwrap();

    let missing = run_in(
        &minio,
        cwd.path(),
        &["get", &format!("s3/{}/absent.dat", bucket.name)],
    );
    assert_eq!(missing.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("Code: NoSuchKey"));

    let wrong_secret = create_config_file(minio.endpoint(), minio.access_key(), "wrong-secret");
    let denied = Command::new(get_s3m_binary())
        .current_dir(cwd.path())
        .arg("--config")
        .arg(wrong_secret.path())
        .args(["get", &uri])
        .output()
        .unwrap();
    assert_eq!(denied.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&denied.stderr).contains("403 Forbidden"));

    assert_eq!(String::from_utf8_lossy(&missing.stdout), "");
    assert_eq!(String::from_utf8_lossy(&denied.stdout), "");
    assert_eq!(entries(cwd.path()), Vec::<String>::new());
}

#[tokio::test]
async fn test_e2e_share_url_serves_exact_bytes_until_tampered() {
    let minio = MinioContext::get_or_start().await;
    let bucket = OwnedBucket::create(&minio, "share").await;
    let source = create_test_file_with_content(2048, "SHARED_");
    let uri = format!("s3/{}/dir/my file (1).txt", bucket.name);
    let upload_args = ["-q", source.path().to_str().unwrap(), uri.as_str()];
    assert_success(&run_s3m_with_minio(&minio, &upload_args), &upload_args);
    let expected = fs::read(source.path()).unwrap();

    for flags in [vec![], vec!["--expire", "43200"], vec!["-e", "604800"]] {
        let args = [vec!["share", uri.as_str()], flags].concat();
        let output = run_s3m_with_minio(&minio, &args);
        assert_success(&output, &args);
        let url = String::from_utf8(output.stdout).unwrap();
        let url = url.trim_end();

        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), 200, "{args:?}");
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            expected.as_slice()
        );

        // The expiry is signed: changing it must invalidate the URL.
        let expires = url
            .split(['?', '&'])
            .find(|pair| pair.starts_with("X-Amz-Expires="))
            .unwrap();
        let tampered = url.replace(expires, "X-Amz-Expires=3600");
        let rejected = reqwest::get(&tampered).await.unwrap();
        assert_eq!(rejected.status(), 403, "{tampered}");
    }
}
