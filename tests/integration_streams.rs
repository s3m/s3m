#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]

mod common;

use common::{MinioContext, calculate_file_hash, get_s3m_binary};
use rkyv::{rancor::Error as RkyvError, to_bytes};
use s3m::{
    cli::globals::GlobalArgs,
    s3::{
        Credentials, Region, S3,
        actions::{CreateMultipartUpload, UploadPart},
    },
    stream::{
        db::Db,
        iterator::PartIterator,
        part::Part,
        state::{StreamMetadata, StreamMode, state_dir, write_metadata},
    },
};
use secrecy::SecretString;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
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

/// Write a file-multipart stream state with one pending part; `upload_id` is
/// `None` for a state whose upload was never created.
fn create_state(home: &Path, id: &str, upload_id: Option<&str>) -> (StreamMetadata, Db) {
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
    if let Some(upload_id) = upload_id {
        db.save_upload_id(upload_id).unwrap();
    }
    db.create_part(1, 0, 11, None).unwrap();
    db.db_parts().unwrap().flush().unwrap();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let metadata = StreamMetadata {
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
    };
    write_metadata(&s3m_dir, &metadata).unwrap();
    (metadata, db)
}

fn create_resumable_state(home: &Path, id: &str) {
    create_state(home, id, Some("upload-1"));
}

fn create_valid_resumable_state(home: &Path, id: &str) -> PathBuf {
    let s3m_dir = home.join(".config").join("s3m");
    let source = home.join("source-valid.bin");
    fs::write(&source, b"hello world").unwrap();

    let file_mtime = fs::metadata(&source)
        .unwrap()
        .modified()
        .unwrap_or_else(|_| SystemTime::now())
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let checksum = calculate_file_hash(&source);
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
            source_path: source.clone(),
            checksum,
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

    source
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
    let create = CreateMultipartUpload::new(key, None, None, None, None)
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

async fn create_partially_uploaded_live_resumable_state(
    home: &Path,
    minio: &MinioContext,
    id: &str,
    bucket: &str,
    key: &str,
) -> PathBuf {
    let s3m_dir = home.join(".config").join("s3m");
    let source = home.join("resume-source-partial.bin");
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
    let create = CreateMultipartUpload::new(key, None, None, None, None)
        .request(&s3)
        .await
        .unwrap();
    let db = Db::new(&s3, key, id, file_mtime, &s3m_dir).unwrap();
    db.save_upload_id(&create.upload_id).unwrap();

    let mut parts = PartIterator::new(file_size, part_size);
    let (first_number, first_seek, first_chunk) = parts.next().unwrap();
    let first_etag = UploadPart::new(
        key,
        &source,
        first_number,
        &create.upload_id,
        first_seek,
        first_chunk,
        None,
    )
    .request(&s3, &GlobalArgs::new())
    .await
    .unwrap();

    let first_part = Part::new(first_number, first_seek, first_chunk, None).set_etag(first_etag);
    let first_bytes = to_bytes::<RkyvError>(&first_part).unwrap();
    db.db_uploaded()
        .unwrap()
        .insert(first_number.to_be_bytes(), first_bytes.as_slice())
        .unwrap();

    for (number, seek, chunk) in PartIterator::new(file_size, part_size) {
        if number == first_number {
            continue;
        }
        db.create_part(number, seek, chunk, None).unwrap();
    }
    db.flush().unwrap();

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
fn test_legacy_clean_reports_cleanup_failure() {
    let home = tempdir().unwrap();
    write_config(home.path());
    // A file where the streams directory belongs makes the state scan fail.
    let streams_path = home.path().join(".config").join("s3m").join("streams");
    fs::write(&streams_path, "not a directory").unwrap();

    for args in [&["--clean"][..], &["streams", "clean"]] {
        let output = run_streams(home.path(), args);

        assert!(!output.status.success(), "{args:?} reported success");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Error:"),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(streams_path.is_file());
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

fn s3m_dir(home: &Path) -> PathBuf {
    home.join(".config").join("s3m")
}

fn show_json(home: &Path, id: &str) -> Value {
    let output = run_streams(home, &["streams", "show", id, "--json"]);
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

/// `show` output without `updated_at`: an entry lacking metadata reports its
/// directory mtime, which moves whenever a scan opens the state database.
fn stable_show_json(home: &Path, id: &str) -> Value {
    let mut shown = show_json(home, id);
    shown.as_object_mut().unwrap().remove("updated_at");
    shown
}

/// Run a resume that must fail with `message`, and prove the saved state is
/// reported identically afterwards (nothing was removed or rewritten).
fn assert_resume_fails_and_state_survives(home: &Path, args: &[&str], id: &str, message: &str) {
    let state_file = state_dir(&s3m_dir(home), id).join("state.yml");
    let state_before = fs::read(&state_file).unwrap();
    let shown_before = stable_show_json(home, id);

    let output = run_streams(home, args);

    assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
    assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(message), "{args:?}: {stderr}");
    assert_eq!(fs::read(&state_file).unwrap(), state_before);
    assert_eq!(stable_show_json(home, id), shown_before);
}

fn set_mtime(path: &Path, mtime: SystemTime) {
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

#[test]
fn test_streams_resume_fails_when_source_file_changed() {
    let home = tempdir().unwrap();
    write_config(home.path());
    let source = create_valid_resumable_state(home.path(), "changed-id");
    let saved_mtime = fs::metadata(&source).unwrap().modified().unwrap();
    let resume = ["streams", "resume", "changed-id"];

    // Same bytes, different modification time.
    set_mtime(&source, saved_mtime + Duration::from_secs(1));
    assert_resume_fails_and_state_survives(
        home.path(),
        &resume,
        "changed-id",
        "Source file changed since the multipart state was created",
    );

    // Same length and modification time, different bytes.
    fs::write(&source, b"HELLO WORLD").unwrap();
    set_mtime(&source, saved_mtime);
    assert_resume_fails_and_state_survives(
        home.path(),
        &resume,
        "changed-id",
        "Source file checksum no longer matches saved stream state",
    );

    // Source removed.
    fs::remove_file(&source).unwrap();
    assert_resume_fails_and_state_survives(
        home.path(),
        &resume,
        "changed-id",
        "Source file missing or unreadable",
    );
}

#[test]
fn test_streams_resume_refuses_unusable_state_and_keeps_it() {
    let home = tempdir().unwrap();
    write_config(home.path());
    create_broken_state(home.path(), "broken-id");
    create_state(home.path(), "no-upload-id", None);
    create_valid_resumable_state(home.path(), "good-id");

    assert_resume_fails_and_state_survives(
        home.path(),
        &["streams", "resume", "broken-id"],
        "broken-id",
        "Stream state broken-id is not resumable (status: broken)",
    );
    assert_eq!(
        show_json(home.path(), "no-upload-id")["upload_id"],
        Value::Null
    );
    assert_resume_fails_and_state_survives(
        home.path(),
        &["streams", "resume", "no-upload-id"],
        "no-upload-id",
        "Stream state no-upload-id is not resumable (status: broken)",
    );
    for args in [
        &["streams", "resume", "good-id", "--json"][..],
        &["streams", "--json", "resume", "good-id"],
    ] {
        assert_resume_fails_and_state_survives(
            home.path(),
            args,
            "good-id",
            "--json is not supported with `s3m streams resume`",
        );
    }
}

#[test]
fn test_streams_rejects_incomplete_invocations() {
    let home = tempdir().unwrap();
    write_config(home.path());

    let cases: &[(&[&str], i32, &str)] = &[
        (&["streams", "show"], 2, "required arguments"),
        (&["streams", "resume"], 2, "required arguments"),
        (&["streams", "frob"], 2, "unrecognized subcommand 'frob'"),
        (
            &["streams", "show", "missing"],
            1,
            "Unknown stream state id: missing",
        ),
    ];
    for (args, code, message) in cases {
        let output = run_streams(home.path(), args);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(*code), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(output.stdout.is_empty(), "{args:?}");
    }
}

#[test]
fn test_streams_default_verb_matches_ls() {
    let home = tempdir().unwrap();
    write_config(home.path());
    // Both entries carry metadata timestamps, so listing order and rendered
    // times do not depend on when the directories were last touched.
    create_state(home.path(), "no-upload-id", None);
    create_resumable_state(home.path(), "good-id");

    let default = run_streams(home.path(), &["streams"]);
    let ls = run_streams(home.path(), &["streams", "ls"]);

    assert!(default.status.success(), "{default:?}");
    assert!(ls.status.success(), "{ls:?}");
    assert_eq!(default.stdout, ls.stdout);
    let stdout = String::from_utf8_lossy(&ls.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "{stdout}");
    assert!(
        lines[0].contains("ID") && lines[0].contains("UPLOAD_ID"),
        "{stdout}"
    );
    let good = lines
        .iter()
        .find(|line| line.starts_with("good-id "))
        .unwrap();
    assert!(
        good.contains("upload-1") && good.contains("bucket/key"),
        "{good}"
    );
    let unusable = lines
        .iter()
        .find(|line| line.starts_with("no-upload-id "))
        .unwrap();
    assert!(unusable.contains("broken"), "{unusable}");
}

#[test]
fn test_streams_json_flag_placement_is_equivalent() {
    let home = tempdir().unwrap();
    write_config(home.path());
    // Both entries carry metadata timestamps, so listing order and rendered
    // times do not depend on when the directories were last touched.
    create_state(home.path(), "no-upload-id", None);
    create_resumable_state(home.path(), "good-id");

    let reference = run_streams(home.path(), &["streams", "ls", "--json"]);
    assert!(reference.status.success(), "{reference:?}");
    for args in [&["streams", "--json"][..], &["streams", "--json", "ls"]] {
        let output = run_streams(home.path(), args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert_eq!(output.stdout, reference.stdout, "{args:?}");
    }
    let listing: Value = serde_json::from_slice(&reference.stdout).unwrap();
    let mut ids: Vec<&str> = listing["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["good-id", "no-upload-id"]);

    let before = run_streams(home.path(), &["streams", "--json", "show", "good-id"]);
    assert!(before.status.success(), "{before:?}");
    assert_eq!(
        before.stdout,
        run_streams(home.path(), &["streams", "show", "good-id", "--json"]).stdout
    );
    let shown: Value = serde_json::from_slice(&before.stdout).unwrap();
    assert_eq!(shown["id"], "good-id");
    assert_eq!(shown["upload_id"], "upload-1");
    assert_eq!(shown["destination"], "bucket/key");
    assert_eq!(shown["file_size_bytes"], 11);
    assert_eq!(shown["parts_total"], 1);
    assert_eq!(shown["parts_pending"], 1);
    assert_eq!(shown["parts_uploaded"], 0);
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

#[tokio::test]
async fn test_streams_resume_completes_partially_uploaded_live_multipart_upload() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "streams-resume-partial-live";
    let key = "resumed-partial.bin";
    minio.create_bucket(bucket).await.unwrap();

    let home = tempdir().unwrap();
    write_minio_config(home.path(), &minio);
    let source_path = create_partially_uploaded_live_resumable_state(
        home.path(),
        &minio,
        "resume-partial-live",
        bucket,
        key,
    )
    .await;

    let output = run_streams(home.path(), &["streams", "resume", "resume-partial-live"]);
    assert!(
        output.status.success(),
        "resume should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let download_path = home.path().join("resumed-partial-download.bin");
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

#[path = "support/streams_resume.rs"]
mod streams_resume;

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
    let error = stdout["error"].as_str().unwrap();
    assert!(error.contains("unclosed bracket"), "{error}");
    // single line, without a source snippet
    assert!(!error.contains('\n'), "{error}");
}

#[test]
fn test_streams_clean_json_output() {
    for args in [
        &["streams", "clean", "--json"][..],
        &["streams", "--json", "clean"],
    ] {
        let home = tempdir().unwrap();
        write_config(home.path());
        create_broken_state(home.path(), "broken-id");
        create_resumable_state(home.path(), "good-id");

        let output = run_streams(home.path(), args);

        assert!(output.status.success(), "{args:?}: {output:?}");
        let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            stdout,
            serde_json::json!({"removed": ["broken-id"], "kept": ["good-id"]})
        );
    }
}

#[test]
fn test_streams_clean_keeps_unfinished_states_byte_for_byte() {
    let home = tempdir().unwrap();
    write_config(home.path());
    let base = s3m_dir(home.path());
    create_broken_state(home.path(), "broken-id");
    create_resumable_state(home.path(), "active-id");
    let day = 24 * 60 * 60;
    // Old and very old updates (resumable and stale) are still unfinished.
    for (id, upload_id, age) in [
        ("resumable-id", "upload-2", 2 * day),
        ("stale-id", "upload-3", 30 * day),
    ] {
        let (mut metadata, _db) = create_state(home.path(), id, Some(upload_id));
        metadata.updated_at = Some(metadata.created_at - age);
        write_metadata(&base, &metadata).unwrap();
    }
    // A finished upload (etag recorded) is removable.
    // Reuse the creating handle: reopening races sled's file lock release.
    let (_, db) = create_state(home.path(), "complete-id", Some("upload-4"));
    db.save_etag("\"etag-done\"").unwrap();
    db.flush().unwrap();
    drop(db);

    let kept = ["active-id", "resumable-id", "stale-id"];
    let state_bytes: Vec<Vec<u8>> = kept
        .iter()
        .map(|id| fs::read(state_dir(&base, id).join("state.yml")).unwrap())
        .collect();

    let output = run_streams(home.path(), &["streams", "clean", "--json"]);

    assert!(output.status.success(), "{output:?}");
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    let sorted = |field: &str| {
        let mut ids: Vec<String> = summary[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(sorted("removed"), ["broken-id", "complete-id"]);
    assert_eq!(sorted("kept"), kept);
    assert!(!state_dir(&base, "broken-id").exists());
    assert!(!state_dir(&base, "complete-id").exists());
    for (id, bytes) in kept.iter().zip(state_bytes) {
        assert_eq!(
            fs::read(state_dir(&base, id).join("state.yml")).unwrap(),
            bytes,
            "{id}"
        );
    }
}
