#![allow(clippy::expect_used, clippy::unwrap_used, clippy::missing_panics_doc)]

#[path = "support/minio_runtime.rs"]
mod minio_runtime;

use minio_runtime::{MinioRuntime, lock_runtime_env, resolve_minio_runtime};
use std::env;
use std::fs;
use tempfile::TempDir;

#[test]
fn resolve_uses_external_minio_when_endpoint_is_set() {
    let _env_guard = TestEnv::new();

    TestEnv::set("MINIO_ENDPOINT", "http://127.0.0.1:9000");
    TestEnv::set("MINIO_ACCESS_KEY", "access");
    TestEnv::set("MINIO_SECRET_KEY", "secret");

    let runtime = resolve_minio_runtime("default-access", "default-secret");

    assert_eq!(
        runtime,
        MinioRuntime::External(minio_runtime::ExternalMinioConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
        })
    );
}

#[test]
fn resolve_autoconfigures_podman_socket_when_available() {
    let _env_guard = TestEnv::new();
    let tempdir = TempDir::new().expect("tempdir");
    let socket = tempdir.path().join("podman/podman.sock");
    fs::create_dir_all(socket.parent().expect("socket parent")).expect("socket dir");
    fs::write(&socket, "").expect("socket file");

    TestEnv::remove("MINIO_ENDPOINT");
    TestEnv::remove("MINIO_ACCESS_KEY");
    TestEnv::remove("MINIO_SECRET_KEY");
    TestEnv::remove("DOCKER_HOST");
    TestEnv::remove("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE");
    TestEnv::remove("TESTCONTAINERS_RYUK_DISABLED");
    TestEnv::set(
        "XDG_RUNTIME_DIR",
        tempdir.path().to_str().expect("utf8 tempdir"),
    );

    let runtime = resolve_minio_runtime("default-access", "default-secret");

    assert_eq!(
        runtime,
        MinioRuntime::ManagedContainer {
            podman_socket: Some(socket.clone()),
        }
    );
    assert_eq!(
        env::var("DOCKER_HOST").expect("docker host"),
        format!("unix://{}", socket.display())
    );
    assert_eq!(
        env::var("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").expect("socket override"),
        socket.display().to_string()
    );
    assert_eq!(
        env::var("TESTCONTAINERS_RYUK_DISABLED").expect("ryuk"),
        "true"
    );
}

#[test]
fn resolve_does_not_override_existing_container_runtime_env() {
    let _env_guard = TestEnv::new();
    let tempdir = TempDir::new().expect("tempdir");
    let socket = tempdir.path().join("podman/podman.sock");
    fs::create_dir_all(socket.parent().expect("socket parent")).expect("socket dir");
    fs::write(&socket, "").expect("socket file");

    TestEnv::remove("MINIO_ENDPOINT");
    TestEnv::remove("MINIO_ACCESS_KEY");
    TestEnv::remove("MINIO_SECRET_KEY");
    TestEnv::remove("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE");
    TestEnv::remove("TESTCONTAINERS_RYUK_DISABLED");
    TestEnv::set(
        "XDG_RUNTIME_DIR",
        tempdir.path().to_str().expect("utf8 tempdir"),
    );
    TestEnv::set("DOCKER_HOST", "unix:///already-configured.sock");

    let runtime = resolve_minio_runtime("default-access", "default-secret");

    assert_eq!(
        runtime,
        MinioRuntime::ManagedContainer {
            podman_socket: None,
        }
    );
    assert_eq!(
        env::var("DOCKER_HOST").expect("docker host"),
        "unix:///already-configured.sock"
    );
    assert!(env::var("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").is_err());
}

struct TestEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
    original: Vec<(String, Option<String>)>,
}

impl TestEnv {
    fn new() -> Self {
        let keys = [
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "DOCKER_HOST",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
            "TESTCONTAINERS_RYUK_DISABLED",
            "XDG_RUNTIME_DIR",
            "HOME",
        ];

        let original = keys
            .into_iter()
            .map(|key| (key.to_string(), env::var(key).ok()))
            .collect();

        Self {
            _guard: lock_runtime_env(),
            original,
        }
    }

    fn set(key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
    }

    fn remove(key: &str) {
        unsafe {
            env::remove_var(key);
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for (key, value) in &self.original {
            match value {
                Some(value) => unsafe {
                    env::set_var(key, value);
                },
                None => unsafe {
                    env::remove_var(key);
                },
            }
        }
    }
}
