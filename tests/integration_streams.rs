#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

mod common;

use common::{MinioContext, calculate_file_hash, get_s3m_binary};
use s3m::{
    s3::{Credentials, Region, S3, actions::CreateMultipartUpload},
    stream::{
        db::Db,
        iterator::PartIterator,
        state::{StreamMetadata, StreamMode, state_dir, write_metadata},
    },
};
use secrecy::SecretString;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::tempdir;

fn write_config(home: &Path) -> PathBuf {
    let config_dir = home.join(".config").join("s3m");
    fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.yml");
    fs::write(
        &config_path,
        r"---
hosts:
  s3:
    region: us-east-1
    access_key: XXX
    secret_key: YYY
",
    )
    .unwrap();
    config_path
}

fn write_minio_config(home: &Path, minio: &MinioContext) -> PathBuf {
    let config_dir = home.join(".config").join("s3m");
    fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.yml");
    fs::write(
        &config_path,
        format!(
            r"---
hosts:
  s3:
    endpoint: {}
    access_key: {}
    secret_key: {}
",
            minio.endpoint(),
            minio.access_key(),
            minio.secret_key()
        ),
    )
    .unwrap();
    config_path
}

fn test_s3() -> S3 {
    S3::new(
        &Credentials::new("AKIAIOSFODNN7EXAMPLE", &SecretString::new("secret".into())),
        &"us-east-1".parse::<Region>().unwrap(),
        Some("bucket".to_string()),
        false,
    )
}

fn minio_s3(minio: &MinioContext, bucket: &str) -> S3 {
    S3::new(
        &Credentials::new(
            minio.access_key(),
            &SecretString::new(minio.secret_key().to_string().into()),
        ),
        &Region::Custom {
            name: String::new(),
            endpoint: minio.endpoint().to_string(),
        },
        Some(bucket.to_string()),
        false,
    )
}

fn create_resumable_state(home: &Path, id: &str) {
    let s3m_dir = home.join(".config").join("s3m");
    let source = home.join("source.bin");
    fs::write(&source, b"hello world").unwrap();

    let file_mtime = fs::metadata(&source)
        .unwrap()
        .modified()
        .unwrap_or_else(|_| SystemTime::now())
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let db = Db::new(&test_s3(), "key", id, file_mtime, &s3m_dir).unwrap();
    db.save_upload_id("upload-1").unwrap();
    db.create_part(1, 0, 11, None).unwrap();
    db.db_parts().unwrap().flush().unwrap();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    write_metadata(
        &s3m_dir,
        &StreamMetadata {
            version: 1,
            id: id.to_string(),
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            source_path: source,
            checksum: id.to_string(),
            file_size: 11,
            file_mtime,
            part_size: 11,
            db_key: db.state_key().to_string(),
            created_at,
            updated_at: Some(created_at),
            pipe: false,
            compress: false,
            encrypt: false,
            mode: StreamMode::FileMultipart,
        },
    )
    .unwrap();
}

fn create_broken_state(home: &Path, id: &str) {
    let s3m_dir = home.join(".config").join("s3m");
    let broken_dir = state_dir(&s3m_dir, id);
    fs::create_dir_all(&broken_dir).unwrap();
    fs::write(broken_dir.join("state.yml"), "not: [valid").unwrap();
}

fn run_streams(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(get_s3m_binary())
        .env("HOME", home)
        .args(args)
        .output()
        .unwrap()
}

async fn create_live_resumable_state(
    home: &Path,
    minio: &MinioContext,
    id: &str,
    bucket: &str,
    key: &str,
) -> PathBuf {
    let s3m_dir = home.join(".config").join("s3m");
    let source = home.join("resume-source.bin");
    let content = b"resume upload payload ".repeat(350_000);
    fs::write(&source, &content).unwrap();

    let file_mtime = fs::metadata(&source)
        .unwrap()
        .modified()
        .unwrap_or_else(|_| SystemTime::now())
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let checksum = calculate_file_hash(&source);
    let file_size = fs::metadata(&source).unwrap().len();
    let part_size = 5 * 1024 * 1024_u64;

    let s3 = minio_s3(minio, bucket);
    let create = CreateMultipartUpload::new(key, None, None, None)
        .request(&s3)
        .await
        .unwrap();
    let db = Db::new(&s3, key, id, file_mtime, &s3m_dir).unwrap();
    db.save_upload_id(&create.upload_id).unwrap();
    for (number, seek, chunk) in PartIterator::new(file_size, part_size) {
        db.create_part(number, seek, chunk, None).unwrap();
    }
    db.db_parts().unwrap().flush().unwrap();

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    write_metadata(
        &s3m_dir,
        &StreamMetadata {
            version: 1,
            id: id.to_string(),
            host: "s3".to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            source_path: source.clone(),
            checksum,
            file_size,
            file_mtime,
            part_size,
            db_key: db.state_key().to_string(),
            created_at,
            updated_at: Some(created_at),
            pipe: false,
            compress: false,
            encrypt: false,
            mode: StreamMode::FileMultipart,
        },
    )
    .unwrap();

    source
}

#[test]
fn test_streams_ls_lists_broken_state_with_header() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");

    let output = run_streams(home.path(), &["streams"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ID"));
    assert!(stdout.contains("STATUS"));
    assert!(stdout.contains("broken-id"));
    assert!(!stdout.contains("[broken-id]"));
    assert!(stdout.contains("broken"));
}

#[test]
fn test_streams_show_broken_state() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");

    let output = run_streams(home.path(), &["streams", "show", "broken-id"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("id: broken-id"));
    assert!(stdout.contains("status: broken"));
    assert!(stdout.contains("error:"));
}

#[test]
fn test_streams_clean_removes_broken_and_keeps_resumable() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");
    create_resumable_state(home.path(), "good-id");

    let output = run_streams(home.path(), &["streams", "clean"]);

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Removed 1 stream state entry"));
    assert!(stdout.contains("Kept 1 active/resumable entry"));
    assert!(!state_dir(&home.path().join(".config").join("s3m"), "broken-id").exists());
    assert!(state_dir(&home.path().join(".config").join("s3m"), "good-id").exists());
}

#[test]
fn test_legacy_clean_removes_broken_and_keeps_resumable() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");
    create_resumable_state(home.path(), "good-id");

    let output = run_streams(home.path(), &["--clean"]);

    assert!(output.status.success());
    assert!(!state_dir(&home.path().join(".config").join("s3m"), "broken-id").exists());
    assert!(state_dir(&home.path().join(".config").join("s3m"), "good-id").exists());
}

#[test]
fn test_streams_resume_unknown_id_fails_clearly() {
    let home = tempdir().unwrap();
    write_config(home.path());

    let output = run_streams(home.path(), &["streams", "resume", "missing"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Unknown stream state id: missing"));
}

#[tokio::test]
async fn test_streams_resume_completes_live_multipart_upload() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "streams-resume-live";
    let key = "resumed.bin";
    minio.create_bucket(bucket).await.unwrap();

    let home = tempdir().unwrap();
    write_minio_config(home.path(), &minio);
    let source_path =
        create_live_resumable_state(home.path(), &minio, "resume-live", bucket, key).await;

    let output = run_streams(home.path(), &["streams", "resume", "resume-live"]);
    assert!(
        output.status.success(),
        "resume should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let download_path = home.path().join("resumed-download.bin");
    let download_output = Command::new(get_s3m_binary())
        .env("HOME", home.path())
        .args([
            "get",
            &format!("s3/{bucket}/{key}"),
            download_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        download_output.status.success(),
        "download should succeed: {}",
        String::from_utf8_lossy(&download_output.stderr)
    );

    assert_eq!(
        calculate_file_hash(&source_path),
        calculate_file_hash(&download_path)
    );
}

#[test]
fn test_streams_ls_json_lists_broken_state() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");

    let output = run_streams(home.path(), &["streams", "ls", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["streams"][0]["id"], "broken-id");
    assert_eq!(stdout["streams"][0]["status"], "broken");
}

#[test]
fn test_streams_show_json_broken_state() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");

    let output = run_streams(home.path(), &["streams", "show", "broken-id", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["id"], "broken-id");
    assert_eq!(stdout["status"], "broken");
    assert!(
        stdout["error"]
            .as_str()
            .unwrap()
            .contains("did not find expected")
    );
}

#[test]
fn test_streams_clean_json_output() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");
    create_resumable_state(home.path(), "good-id");

    let output = run_streams(home.path(), &["streams", "clean", "--json"]);

    assert!(output.status.success());
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["removed"][0], "broken-id");
    assert_eq!(stdout["kept"][0], "good-id");
}
