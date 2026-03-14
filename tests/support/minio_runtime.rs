use std::env;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalMinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MinioRuntime {
    External(ExternalMinioConfig),
    ManagedContainer { podman_socket: Option<PathBuf> },
}

#[allow(dead_code)]
pub const STARTUP_HINT: &str = "Set MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY to use an external MinIO, or start a Docker/Podman socket before running the e2e suite.";

#[allow(dead_code)]
static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[allow(dead_code)]
pub fn lock_runtime_env() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn resolve_minio_runtime(default_access_key: &str, default_secret_key: &str) -> MinioRuntime {
    if let Ok(endpoint) = env::var("MINIO_ENDPOINT") {
        let access_key =
            env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| default_access_key.to_string());
        let secret_key =
            env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| default_secret_key.to_string());

        return MinioRuntime::External(ExternalMinioConfig {
            endpoint,
            access_key,
            secret_key,
        });
    }

    MinioRuntime::ManagedContainer {
        podman_socket: autoconfigure_podman_socket(),
    }
}

fn autoconfigure_podman_socket() -> Option<PathBuf> {
    if env::var_os("DOCKER_HOST").is_some()
        || env::var_os("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").is_some()
    {
        return None;
    }

    let socket = detect_podman_socket()?;
    let docker_host = format!("unix://{}", socket.display());

    // Test-only support code: update process env so testcontainers picks up Podman automatically.
    unsafe {
        env::set_var("DOCKER_HOST", &docker_host);
        env::set_var("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", &socket);
        env::set_var("TESTCONTAINERS_RYUK_DISABLED", "true");
    }

    Some(socket)
}

fn detect_podman_socket() -> Option<PathBuf> {
    podman_socket_candidates()
        .into_iter()
        .find(|candidate| candidate.exists())
}

fn podman_socket_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(runtime_dir) = env::var_os("XDG_RUNTIME_DIR") {
        push_candidate(
            &mut candidates,
            PathBuf::from(runtime_dir).join("podman/podman.sock"),
        );
    }

    if let Ok(entries) = std::fs::read_dir("/run/user") {
        for entry in entries.flatten() {
            push_candidate(&mut candidates, entry.path().join("podman/podman.sock"));
        }
    }

    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        push_candidate(
            &mut candidates,
            home.join(".local/share/containers/podman/machine/podman.sock"),
        );
        push_candidate(
            &mut candidates,
            home.join(".config/containers/podman/machine/qemu/podman.sock"),
        );
    }

    candidates
}

fn push_candidate(candidates: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !candidates.iter().any(|existing| existing == &candidate) {
        candidates.push(candidate);
    }
}
