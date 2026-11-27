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

#[derive(Clone, Copy)]
enum Layout {
    Legacy,
    Current,
}

const LAYOUTS: [Layout; 2] = [Layout::Legacy, Layout::Current];

impl Layout {
    fn open(self, s3: &S3, key: &str, hash: &str, mtime: u128, base: &Path) -> Db {
        let db = match self {
            Self::Legacy => Db::open_existing(
                &state_dir(base, hash),
                format!("{} {key} {mtime}", &s3.hash()[..8]),
            ),
            Self::Current => Db::new(s3, key, hash, mtime, base),
        }
        .unwrap();
        assert_eq!(
            state_id(&db).starts_with("v2-"),
            matches!(self, Self::Current)
        );
        db
    }
}

fn state_id(db: &Db) -> String {
    db.path().file_name().unwrap().to_str().unwrap().to_owned()
}

/// sled can keep a state's file lock for a moment after its handle is dropped in this
/// process. An `s3m` started right away would list that state as locked (active), so
/// wait until the lock is free before running it.
fn wait_for_state_lock(home: &Path, id: &str) {
    let path = state_dir(&s3m_dir(home), id).join("db");
    let file = fs::File::options()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    for _ in 0..500 {
        // Released again when `file` is dropped.
        if file.try_lock().is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("the state lock of {id} was not released");
}

/// Write a file-multipart stream state with one pending part; `upload_id` is
/// `None` for a state whose upload was never created. The caller owns the handle:
/// after dropping it, call [`wait_for_state_lock`] before running `s3m`.
fn create_state(
    layout: Layout,
    home: &Path,
    id: &str,
    upload_id: Option<&str>,
) -> (StreamMetadata, Db) {
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
    let db = layout.open(&test_s3(), "key", id, file_mtime, &s3m_dir);
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
        id: state_id(&db),
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

fn create_resumable_state(layout: Layout, home: &Path, id: &str) -> String {
    let (metadata, db) = create_state(layout, home, id, Some("upload-1"));
    drop(db);
    wait_for_state_lock(home, &metadata.id);
    metadata.id
}

fn create_state_without_upload_id(layout: Layout, home: &Path, id: &str) -> String {
    let (metadata, db) = create_state(layout, home, id, None);
    drop(db);
    wait_for_state_lock(home, &metadata.id);
    metadata.id
}

fn create_valid_resumable_state(layout: Layout, home: &Path) -> (String, PathBuf) {
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
    let db = layout.open(&test_s3(), "key", &checksum, file_mtime, &s3m_dir);
    let id = state_id(&db);
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
            id: id.clone(),
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

    drop(db);
    wait_for_state_lock(home, &id);
    (id, source)
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
        .env("S3M_HOME", s3m_dir(home))
        .args(args)
        .output()
        .unwrap()
}

async fn create_live_resumable_state(
    layout: Layout,
    home: &Path,
    minio: &MinioContext,
    bucket: &str,
    key: &str,
) -> (String, PathBuf) {
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
    let db = layout.open(&s3, key, &checksum, file_mtime, &s3m_dir);
    let id = state_id(&db);
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
            id: id.clone(),
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

    drop(db);
    wait_for_state_lock(home, &id);
    (id, source)
}

async fn create_partially_uploaded_live_resumable_state(
    layout: Layout,
    home: &Path,
    minio: &MinioContext,
    bucket: &str,
    key: &str,
) -> (String, PathBuf) {
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
    let db = layout.open(&s3, key, &checksum, file_mtime, &s3m_dir);
    let id = state_id(&db);
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
            id: id.clone(),
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

    drop(db);
    wait_for_state_lock(home, &id);
    (id, source)
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
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        create_broken_state(home.path(), "broken-id");
        let good_id = create_resumable_state(layout, home.path(), "good-id");

        let output = run_streams(home.path(), &["streams", "clean"]);

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("Removed 1 stream state entry"));
        assert!(stdout.contains("Kept 1 active/resumable entry"));
        assert!(!state_dir(&home.path().join(".config").join("s3m"), "broken-id").exists());
        assert!(state_dir(&home.path().join(".config").join("s3m"), &good_id).exists());
    }
}

#[test]
fn test_legacy_clean_removes_broken_and_keeps_resumable() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        create_broken_state(home.path(), "broken-id");
        let good_id = create_resumable_state(layout, home.path(), "good-id");

        let output = run_streams(home.path(), &["--clean"]);

        assert!(output.status.success());
        assert!(!state_dir(&home.path().join(".config").join("s3m"), "broken-id").exists());
        assert!(state_dir(&home.path().join(".config").join("s3m"), &good_id).exists());
    }
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
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        let (changed_id, source) = create_valid_resumable_state(layout, home.path());
        let saved_mtime = fs::metadata(&source).unwrap().modified().unwrap();
        let resume = ["streams", "resume", &changed_id];

        // Same bytes, different modification time.
        set_mtime(&source, saved_mtime + Duration::from_secs(1));
        assert_resume_fails_and_state_survives(
            home.path(),
            &resume,
            &changed_id,
            "Source file changed since the multipart state was created",
        );

        // Same length and modification time, different bytes.
        fs::write(&source, b"HELLO WORLD").unwrap();
        set_mtime(&source, saved_mtime);
        assert_resume_fails_and_state_survives(
            home.path(),
            &resume,
            &changed_id,
            "Source file checksum no longer matches saved stream state",
        );

        // Source removed.
        fs::remove_file(&source).unwrap();
        assert_resume_fails_and_state_survives(
            home.path(),
            &resume,
            &changed_id,
            "Source file missing or unreadable",
        );
    }
}

// A host without keys can't sign: resume reports it before any request, unless
// --no-sign-request is given, which resume honours like every other command.
#[test]
fn test_streams_resume_checks_keys_unless_unsigned() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        let config_dir = home.path().join(".config").join("s3m");
        fs::create_dir_all(&config_dir).unwrap();
        // Nothing listens on port 1: an unsigned resume fails at the connection.
        fs::write(
            config_dir.join("config.yml"),
            "---\nhosts:\n  s3:\n    endpoint: http://127.0.0.1:1\n    region: us-east-1\n",
        )
        .unwrap();
        let (good_id, _) = create_valid_resumable_state(layout, home.path());
        let run = |args: &[&str]| {
            Command::new(get_s3m_binary())
                .env("HOME", home.path())
                .env("S3M_HOME", s3m_dir(home.path()))
                .env_remove("AWS_ACCESS_KEY_ID")
                .env_remove("AWS_SECRET_ACCESS_KEY")
                .args(args)
                .output()
                .unwrap()
        };

        let output = run(&["streams", "resume", &good_id]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(
            stderr.contains("hosts.s3 has no access_key and secret_key in the config"),
            "{stderr}"
        );

        let output = run(&[
            "-r",
            "1",
            "--no-sign-request",
            "streams",
            "resume",
            &good_id,
        ]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(!stderr.contains("has no access_key"), "{stderr}");
        assert!(stderr.contains("127.0.0.1:1"), "{stderr}");
    }
}

#[test]
fn test_streams_resume_refuses_unusable_state_and_keeps_it() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        create_broken_state(home.path(), "broken-id");
        let no_upload_id = create_state_without_upload_id(layout, home.path(), "no-upload-id");
        let (good_id, _) = create_valid_resumable_state(layout, home.path());

        assert_resume_fails_and_state_survives(
            home.path(),
            &["streams", "resume", "broken-id"],
            "broken-id",
            "Stream state broken-id is not resumable (status: broken)",
        );
        assert_eq!(
            show_json(home.path(), &no_upload_id)["upload_id"],
            Value::Null
        );
        assert_resume_fails_and_state_survives(
            home.path(),
            &["streams", "resume", &no_upload_id],
            &no_upload_id,
            &format!("Stream state {no_upload_id} is not resumable (status: broken)"),
        );
        for args in [
            &["streams", "resume", &good_id, "--json"][..],
            &["streams", "--json", "resume", &good_id],
        ] {
            assert_resume_fails_and_state_survives(
                home.path(),
                args,
                &good_id,
                "--json is not supported with `s3m streams resume`",
            );
        }
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
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        // Both entries carry metadata timestamps, so listing order and rendered
        // times do not depend on when the directories were last touched.
        let no_upload_id = create_state_without_upload_id(layout, home.path(), "no-upload-id");
        let good_id = create_resumable_state(layout, home.path(), "good-id");

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
            .find(|line| line.starts_with(&format!("{good_id} ")))
            .unwrap();
        assert!(
            good.contains("upload-1") && good.contains("bucket/key"),
            "{good}"
        );
        let unusable = lines
            .iter()
            .find(|line| line.starts_with(&format!("{no_upload_id} ")))
            .unwrap();
        assert!(unusable.contains("broken"), "{unusable}");
    }
}

#[test]
fn test_streams_json_flag_placement_is_equivalent() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        // Both entries carry metadata timestamps, so listing order and rendered
        // times do not depend on when the directories were last touched.
        let no_upload_id = create_state_without_upload_id(layout, home.path(), "no-upload-id");
        let good_id = create_resumable_state(layout, home.path(), "good-id");

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
        let mut expected = [good_id.as_str(), no_upload_id.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);

        let before = run_streams(home.path(), &["streams", "--json", "show", &good_id]);
        assert!(before.status.success(), "{before:?}");
        assert_eq!(
            before.stdout,
            run_streams(home.path(), &["streams", "show", &good_id, "--json"]).stdout
        );
        let shown: Value = serde_json::from_slice(&before.stdout).unwrap();
        assert_eq!(shown["id"], good_id);
        assert_eq!(shown["upload_id"], "upload-1");
        assert_eq!(shown["destination"], "bucket/key");
        assert_eq!(shown["file_size_bytes"], 11);
        assert_eq!(shown["parts_total"], 1);
        assert_eq!(shown["parts_pending"], 1);
        assert_eq!(shown["parts_uploaded"], 0);
    }
}

#[tokio::test]
async fn test_streams_resume_completes_live_multipart_upload() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "streams-resume-live";
    let key = "resumed.bin";
    minio.create_bucket(bucket).await.unwrap();

    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_minio_config(home.path(), &minio);
        let (id, source_path) =
            create_live_resumable_state(layout, home.path(), &minio, bucket, key).await;

        let output = run_streams(home.path(), &["streams", "resume", &id]);
        assert!(
            output.status.success(),
            "resume should succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let download_path = home.path().join("resumed-download.bin");
        let download_output = Command::new(get_s3m_binary())
            .env("HOME", home.path())
            .env("S3M_HOME", s3m_dir(home.path()))
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
}

#[tokio::test]
async fn test_streams_resume_completes_partially_uploaded_live_multipart_upload() {
    let minio = MinioContext::get_or_start().await;
    let bucket = "streams-resume-partial-live";
    let key = "resumed-partial.bin";
    minio.create_bucket(bucket).await.unwrap();

    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_minio_config(home.path(), &minio);
        let (id, source_path) = create_partially_uploaded_live_resumable_state(
            layout,
            home.path(),
            &minio,
            bucket,
            key,
        )
        .await;

        let output = run_streams(home.path(), &["streams", "resume", &id]);
        assert!(
            output.status.success(),
            "resume should succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let download_path = home.path().join("resumed-partial-download.bin");
        let download_output = Command::new(get_s3m_binary())
            .env("HOME", home.path())
            .env("S3M_HOME", s3m_dir(home.path()))
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
    for layout in LAYOUTS {
        for args in [
            &["streams", "clean", "--json"][..],
            &["streams", "--json", "clean"],
        ] {
            let home = tempdir().unwrap();
            write_config(home.path());
            create_broken_state(home.path(), "broken-id");
            let good_id = create_resumable_state(layout, home.path(), "good-id");

            let output = run_streams(home.path(), args);

            assert!(output.status.success(), "{args:?}: {output:?}");
            let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                stdout,
                serde_json::json!({"removed": ["broken-id"], "kept": [&good_id]})
            );
        }
    }
}

#[test]
fn test_streams_clean_keeps_unfinished_states_byte_for_byte() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        let base = s3m_dir(home.path());
        create_broken_state(home.path(), "broken-id");
        let active_id = create_resumable_state(layout, home.path(), "active-id");
        let day = 24 * 60 * 60;
        // Old and very old updates (resumable and stale) are still unfinished.
        let mut kept = vec![active_id];
        for (id, upload_id, age) in [
            ("resumable-id", "upload-2", 2 * day),
            ("stale-id", "upload-3", 30 * day),
        ] {
            let (mut metadata, db) = create_state(layout, home.path(), id, Some(upload_id));
            metadata.updated_at = Some(metadata.created_at - age);
            write_metadata(&base, &metadata).unwrap();
            drop(db);
            wait_for_state_lock(home.path(), &metadata.id);
            kept.push(metadata.id);
        }
        // A finished upload (etag recorded) is removable.
        // Reuse the creating handle: reopening races sled's file lock release.
        let (complete, db) = create_state(layout, home.path(), "complete-id", Some("upload-4"));
        db.save_etag("\"etag-done\"").unwrap();
        db.flush().unwrap();
        drop(db);
        wait_for_state_lock(home.path(), &complete.id);

        kept.sort();
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
        let mut removed = ["broken-id", &complete.id];
        removed.sort_unstable();
        assert_eq!(sorted("removed"), removed);
        assert_eq!(sorted("kept"), kept);
        assert!(!state_dir(&base, "broken-id").exists());
        assert!(!state_dir(&base, &complete.id).exists());
        for (id, bytes) in kept.iter().zip(state_bytes) {
            assert_eq!(
                fs::read(state_dir(&base, id).join("state.yml")).unwrap(),
                bytes,
                "{id}"
            );
        }
    }
}

#[test]
fn replacing_stream_metadata_leaves_previous_contents_intact() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        let (mut metadata, _db) = create_state(layout, home.path(), "entry", Some("open"));
        let base = s3m_dir(home.path());
        let dir = state_dir(&base, &metadata.id);
        let path = dir.join("state.yml");
        for touch in [false, true] {
            let previous = home.path().join(format!("previous-{touch}"));
            fs::hard_link(&path, &previous).unwrap();
            let before = fs::read(&previous).unwrap();
            if touch {
                s3m::stream::state::touch_metadata(&dir).unwrap();
            } else {
                metadata.updated_at = Some(1);
                write_metadata(&base, &metadata).unwrap();
            }
            assert_ne!(fs::read(&path).unwrap(), before);
            assert_eq!(fs::read(previous).unwrap(), before);
        }
    }
}

#[test]
fn clean_keeps_an_upload_with_torn_metadata() {
    for layout in LAYOUTS {
        let home = tempdir().unwrap();
        write_config(home.path());
        let torn = create_resumable_state(layout, home.path(), "torn");
        let dir = state_dir(&s3m_dir(home.path()), &torn);
        fs::write(dir.join("state.yml"), "id: [").unwrap();
        for args in [&["streams", "clean"][..], &["--clean"]] {
            assert!(run_streams(home.path(), args).status.success());
            assert!(dir.exists(), "{args:?} removed an open upload");
            let shown = show_json(home.path(), &torn);
            assert_eq!(shown["status"], "broken");
            assert_eq!(shown["upload_id"], "upload-1");
            assert_eq!(shown["parts_pending"], 1);
            let listing = run_streams(home.path(), &["streams", "ls"]);
            assert!(String::from_utf8_lossy(&listing.stdout).contains("saved upload id"));
        }
    }
}

#[path = "support/streams_destinations.rs"]
mod streams_destinations;

#[test]
fn clean_removes_completed_uploads_with_unreadable_metadata() {
    for layout in LAYOUTS {
        for args in [&["streams", "clean"][..], &["--clean"]] {
            for torn in [false, true] {
                let home = tempdir().unwrap();
                write_config(home.path());
                let (metadata, db) =
                    create_state(layout, home.path(), "done", Some("completed-uid"));
                db.db_parts().unwrap().clear().unwrap();
                db.save_etag("\"done-2\"").unwrap();
                let dir = db.path().to_owned();
                drop(db);
                let yaml = dir.join("state.yml");
                if torn {
                    fs::write(yaml, "id: [").unwrap();
                } else {
                    fs::remove_file(yaml).unwrap();
                }
                wait_for_state_lock(home.path(), &metadata.id);
                assert_eq!(show_json(home.path(), &metadata.id)["status"], "complete");
                assert!(run_streams(home.path(), args).status.success());
                assert!(!dir.exists(), "{args:?} kept {}", metadata.id);
            }
        }
    }
}

#[test]
fn saved_id_retention_reason_starts_with_text() {
    let home = tempdir().unwrap();
    write_config(home.path());
    let (metadata, db) = create_state(Layout::Current, home.path(), "broken", None);
    db.db().insert("another key 1", "still-open").unwrap();
    db.flush().unwrap();
    drop(db);
    wait_for_state_lock(home.path(), &metadata.id);
    let shown = show_json(home.path(), &metadata.id);
    assert_eq!(shown["status"], "broken");
    assert_eq!(
        shown["error"],
        "kept because the database has a saved upload id"
    );
}
