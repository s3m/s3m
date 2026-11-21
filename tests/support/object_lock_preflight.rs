use mockito::{Matcher, Server};
use std::process::Command;

#[test]
fn retained_tests_reject_unowned_external_services_before_any_request() {
    let mut server = Server::new();
    let requests: Vec<_> = ["GET", "PUT", "POST", "DELETE", "HEAD"]
        .into_iter()
        .map(|method| server.mock(method, Matcher::Any).expect(0).create())
        .collect();
    let endpoint = server.url();

    for test in [
        "test_e2e_upload_lock_modes",
        "test_e2e_compliance_version_delete_cannot_be_bypassed",
    ] {
        for (owned_endpoint, id, diagnostic) in [
            (
                None,
                None,
                "COMPLIANCE fixtures require an owned disposable service",
            ),
            (
                Some(endpoint.as_str()),
                None,
                "disposable fixture must identify its exact owned container",
            ),
            (
                None,
                Some("untrusted-id"),
                "disposable fixtures require S3M_DISPOSABLE_OBJECT_LOCK_ENDPOINT",
            ),
            (
                Some(endpoint.as_str()),
                Some(""),
                "owned container ID must be present",
            ),
            (
                Some("http://127.0.0.1:1"),
                Some("untrusted-id"),
                "disposable fixture endpoint must match the actual service",
            ),
        ] {
            let mut child = Command::new(std::env::current_exe().expect("test executable"));
            child
                .args([test, "--exact", "--nocapture"])
                .env("MINIO_ENDPOINT", &endpoint)
                .env("MINIO_ACCESS_KEY", "minioadmin")
                .env("MINIO_SECRET_KEY", "minioadmin")
                .env_remove("S3M_DISPOSABLE_OBJECT_LOCK_ENDPOINT")
                .env_remove("S3M_DISPOSABLE_OBJECT_LOCK_CONTAINER_ID");
            if let Some(value) = owned_endpoint {
                child.env("S3M_DISPOSABLE_OBJECT_LOCK_ENDPOINT", value);
            }
            if let Some(value) = id {
                child.env("S3M_DISPOSABLE_OBJECT_LOCK_CONTAINER_ID", value);
            }
            let output = child.output().expect("run retained test");
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{test}: {output:?}");
            assert!(error.contains(diagnostic), "{test}: {output:?}");
            for request in &requests {
                request.assert();
            }
        }
    }
}
