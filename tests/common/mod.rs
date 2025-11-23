//! Common test helpers for e2e integration tests
//!
//! This module contains shared utilities used across all e2e test files:
//! - `MinioContext`: Manages `MinIO` test environment (external or container-based)
//! - Config file helpers: Create temporary s3m config files
//! - S3m binary helpers: Run s3m commands with proper setup
//! - Test file helpers: Create test files and calculate hashes

#![allow(dead_code, clippy::indexing_slicing)]

mod helpers;

pub use helpers::minio::{MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, MinioContainer};

use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use tempfile::NamedTempFile;

/// Create a temporary config.yml file for s3m
pub fn create_config_file(endpoint: &str, access_key: &str, secret_key: &str) -> NamedTempFile {
    create_config_file_with_options(endpoint, access_key, secret_key, None, false)
}

/// Create a temporary config.yml file with encryption and/or compression options
pub fn create_config_file_with_options(
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
pub enum MinioContext {
    External {
        endpoint: String,
        access_key: String,
        secret_key: String,
    },
    Container(Box<MinioContainer>),
}

impl MinioContext {
    /// Get or start `MinIO` - uses external if `MINIO_ENDPOINT` is set, otherwise starts container
    pub async fn get_or_start() -> Self {
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

    pub fn endpoint(&self) -> &str {
        match self {
            MinioContext::External { endpoint, .. } => endpoint,
            MinioContext::Container(c) => c.endpoint(),
        }
    }

    pub fn access_key(&self) -> &str {
        match self {
            MinioContext::External { access_key, .. } => access_key,
            MinioContext::Container(c) => &c.access_key,
        }
    }

    pub fn secret_key(&self) -> &str {
        match self {
            MinioContext::External { secret_key, .. } => secret_key,
            MinioContext::Container(c) => &c.secret_key,
        }
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> anyhow::Result<()> {
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
pub fn get_s3m_binary() -> PathBuf {
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
pub fn run_s3m_with_minio(minio: &MinioContext, args: &[&str]) -> std::process::Output {
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
pub fn run_s3m(args: &[&str]) -> std::process::Output {
    let binary = get_s3m_binary();
    let mut cmd = Command::new(binary);
    cmd.args(args);
    cmd.output().expect("Failed to execute s3m")
}

/// Helper to calculate blake3 hash of a file
///
/// Uses the production `blake3()` function from `s3m::s3::tools` to ensure
/// tests verify the actual code users will run (eating our own dog food).
pub fn calculate_file_hash(path: &std::path::Path) -> String {
    s3m::s3::tools::blake3(path).expect("Failed to calculate blake3 hash")
}

/// Helper to create a test file with specified size and content pattern
pub fn create_test_file_with_content(size: usize, pattern: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let content = pattern.repeat(size / pattern.len() + 1);
    file.write_all(&content.as_bytes()[..size])
        .expect("Failed to write");
    file.flush().expect("Failed to flush");
    file
}
