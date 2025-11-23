//! `MinIO` test helper for integration tests
//!
//! Provides utilities to spin up a `MinIO` container for S3 API testing.
//! Supports both Docker and Podman container runtimes.
//!
//! ## Usage with Podman
//!
//! Set the following environment variables before running tests:
//! ```bash
//! export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
//! export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/$(id -u)/podman/podman.sock
//! cargo test --test integration_minio_real
//! ```

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use s3m::s3::{Credentials, Region, S3};
use secrecy::SecretString;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// Default `MinIO` credentials for testing
pub const MINIO_ROOT_USER: &str = "minioadmin";
pub const MINIO_ROOT_PASSWORD: &str = "minioadmin";

/// `MinIO` test fixture that manages container lifecycle
pub struct MinioContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

impl MinioContainer {
    /// Start a new `MinIO` container
    ///
    /// This will:
    /// 1. Pull the `MinIO` image (if not cached)
    /// 2. Start the container with exposed ports
    /// 3. Wait for `MinIO` to be ready
    ///
    /// # Returns
    ///
    /// A `MinioContainer` instance with connection details
    pub async fn start() -> Self {
        let image = GenericImage::new("minio/minio", "latest")
            .with_wait_for(WaitFor::message_on_stderr("MinIO Object Storage Server"))
            .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
            .with_cmd(vec!["server", "/data", "--console-address", ":9001"]);

        let container = image
            .start()
            .await
            .expect("Failed to start MinIO container");

        let port = container
            .get_host_port_ipv4(ContainerPort::Tcp(9000))
            .await
            .expect("Failed to get MinIO port");

        let endpoint = format!("http://127.0.0.1:{}", port);

        // Give MinIO a moment to fully initialize
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        Self {
            container,
            endpoint,
            access_key: MINIO_ROOT_USER.to_string(),
            secret_key: MINIO_ROOT_PASSWORD.to_string(),
        }
    }

    /// Create an S3 client configured for this `MinIO` instance
    ///
    /// # Arguments
    ///
    /// * `bucket` - Optional bucket name to use
    ///
    /// # Returns
    ///
    /// An `S3` client configured to connect to the `MinIO` container
    #[allow(dead_code)]
    pub fn create_s3_client(&self, bucket: Option<String>) -> S3 {
        let credentials = Credentials::new(
            &self.access_key,
            &SecretString::new(self.secret_key.clone().into()),
        );

        // Parse the endpoint to get the region (MinIO doesn't care about region)
        let region = "us-east-1".parse::<Region>().expect("valid region");

        S3::new(&credentials, &region, bucket, false)
    }

    /// Get the `MinIO` endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Create a test bucket in `MinIO`
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - Name of the bucket to create
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    pub async fn create_bucket(&self, bucket_name: &str) -> anyhow::Result<()> {
        // Use reqwest to make a PUT request to create the bucket
        let url = format!("{}/{}", self.endpoint, bucket_name);
        let port = self.endpoint.split(':').next_back().unwrap_or("9000");
        let response = reqwest::Client::new()
            .put(&url)
            .header("Host", format!("127.0.0.1:{port}"))
            .send()
            .await?;

        if response.status().is_success() || response.status().as_u16() == 409 {
            // 409 means bucket already exists, which is fine
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Failed to create bucket: {} - {}",
                response.status(),
                response.text().await?
            ))
        }
    }

    /// List all buckets in `MinIO`
    #[allow(dead_code)]
    pub async fn list_buckets(&self) -> anyhow::Result<Vec<String>> {
        let url = &self.endpoint;
        let port = self.endpoint.split(':').next_back().unwrap_or("9000");
        let response = reqwest::Client::new()
            .get(url)
            .header("Host", format!("127.0.0.1:{port}"))
            .send()
            .await?;

        if response.status().is_success() {
            // Parse XML response to get bucket names
            // For now, just return empty vec as this is a helper
            Ok(Vec::new())
        } else {
            Err(anyhow::anyhow!(
                "Failed to list buckets: {}",
                response.status()
            ))
        }
    }

    /// Wait for `MinIO` to be ready to accept connections
    ///
    /// This is useful for ensuring the container is fully started
    /// before running tests.
    pub async fn wait_for_ready(&self) -> anyhow::Result<()> {
        use std::time::Duration;
        use tokio::time::sleep;

        let max_attempts = 30;
        let mut attempt = 0;

        while attempt < max_attempts {
            let url = format!("{}/minio/health/live", self.endpoint);
            match reqwest::Client::new().get(&url).send().await {
                Ok(response) if response.status().is_success() => {
                    return Ok(());
                }
                _ => {
                    attempt += 1;
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }

        Err(anyhow::anyhow!(
            "MinIO did not become ready after {} attempts",
            max_attempts
        ))
    }
}
