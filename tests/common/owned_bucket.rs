use super::{MinioContext, run_s3m_with_minio};

/// Every test owns a fresh bucket; cleanup never addresses an existing fixture.
pub struct OwnedBucket<'a> {
    minio: &'a MinioContext,
    pub name: String,
    cleanup: CleanupBoundary,
}

enum CleanupBoundary {
    Bucket,
    DisposableService,
}

impl<'a> OwnedBucket<'a> {
    pub fn new(minio: &'a MinioContext, label: &str) -> Self {
        let cleanup = if minio.owns_disposable_service() {
            CleanupBoundary::DisposableService
        } else {
            CleanupBoundary::Bucket
        };
        Self {
            minio,
            name: format!("s3m-test-{:016x}-{}", rand::random::<u64>(), label),
            cleanup,
        }
    }

    pub fn retained(minio: &'a MinioContext, label: &str) -> Self {
        assert!(
            minio.owns_disposable_service(),
            "COMPLIANCE fixtures require an owned disposable service before creating data; \
             use `just test-integration` or unset MINIO_ENDPOINT for managed containers. \
             External owners must supply S3M_DISPOSABLE_OBJECT_LOCK_ENDPOINT and \
             S3M_DISPOSABLE_OBJECT_LOCK_CONTAINER_ID and remove their service on every exit"
        );
        Self::new(minio, label)
    }
}

impl Drop for OwnedBucket<'_> {
    fn drop(&mut self) {
        match self.cleanup {
            CleanupBoundary::DisposableService => {
                eprintln!(
                    "owned bucket {}: cleanup belongs to disposable service at {}",
                    self.name,
                    self.minio.endpoint()
                );
            }
            CleanupBoundary::Bucket => {
                let result = run_s3m_with_minio(
                    self.minio,
                    &[
                        "rm",
                        "-b",
                        "--recursive",
                        "--bypass-governance",
                        &format!("s3/{}", self.name),
                    ],
                );
                let error = String::from_utf8_lossy(&result.stderr);
                // Some enforcement tests already remove their owned bucket.
                let removed = result.status.success() || error.contains("NoSuchBucket");
                eprintln!(
                    "owned bucket cleanup {}: exit={} {}",
                    self.name, result.status, error
                );
                assert!(
                    removed || std::thread::panicking(),
                    "owned bucket cleanup failed: {error}"
                );
            }
        }
    }
}
