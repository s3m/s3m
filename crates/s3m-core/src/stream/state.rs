use crate::stream::{
    db::{DB_PARTS, DB_UPLOADED},
    part::Part,
};
use anyhow::{Context, Result};
use rkyv::{from_bytes, rancor::Error as RkyvError};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const STATE_FILE: &str = "state.yml";
const ACTIVE_SECONDS: u64 = 3_600;
const STALE_SECONDS: u64 = 60 * 60 * 24 * 7;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamMode {
    FileMultipart,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub version: u8,
    pub id: String,
    pub host: String,
    pub bucket: String,
    pub key: String,
    pub source_path: PathBuf,
    pub checksum: String,
    pub file_size: u64,
    pub file_mtime: u128,
    pub part_size: u64,
    pub db_key: String,
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: Option<u64>,
    pub pipe: bool,
    pub compress: bool,
    pub encrypt: bool,
    pub mode: StreamMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStatus {
    Active,
    Resumable,
    Stale,
    Broken,
    Complete,
}

impl StreamStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Resumable => "resumable",
            Self::Stale => "stale",
            Self::Broken => "broken",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub state_dir: PathBuf,
    pub metadata: Option<StreamMetadata>,
    pub status: StreamStatus,
    pub upload_id: Option<String>,
    pub etag: Option<String>,
    pub parts_total: usize,
    pub parts_pending: usize,
    pub parts_uploaded: usize,
    pub bytes_uploaded: u64,
    pub updated_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CleanSummary {
    pub removed: Vec<String>,
    pub kept: Vec<String>,
}

#[derive(Debug, Default)]
struct DbSnapshot {
    upload_id: Option<String>,
    etag: Option<String>,
    parts_pending: usize,
    parts_uploaded: usize,
    bytes_uploaded: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampTrust {
    Trusted,
    Legacy,
}

#[must_use]
pub fn streams_dir(base_path: &Path) -> PathBuf {
    base_path.join("streams")
}

#[must_use]
pub fn state_dir(base_path: &Path, id: &str) -> PathBuf {
    streams_dir(base_path).join(id)
}

#[must_use]
pub fn state_file_path(base_path: &Path, id: &str) -> PathBuf {
    state_dir(base_path, id).join(STATE_FILE)
}

/// # Errors
/// Will return `Err` if can not write the stream metadata
pub fn write_metadata(base_path: &Path, metadata: &StreamMetadata) -> Result<()> {
    let state_dir = state_dir(base_path, &metadata.id);
    fs::create_dir_all(&state_dir)?;
    let path = state_dir.join(STATE_FILE);
    let mut metadata = metadata.clone();
    if metadata.updated_at.is_none() {
        metadata.updated_at = Some(metadata.created_at);
    }
    fs::write(path, serde_saphyr::to_string(&metadata)?)?;
    Ok(())
}

/// # Errors
/// Will return `Err` if the persisted stream metadata can not be refreshed
pub fn touch_metadata(state_dir: &Path) -> Result<()> {
    let path = state_dir.join(STATE_FILE);
    if !path.exists() {
        return Ok(());
    }

    let mut metadata = load_metadata(state_dir)?;
    metadata.updated_at = Some(now_secs());
    fs::write(path, serde_saphyr::to_string(&metadata)?)?;
    Ok(())
}

/// # Errors
/// Will return `Err` if scanning the streams directory fails
pub fn scan_streams(base_path: &Path) -> Result<Vec<StreamEntry>> {
    let streams_dir = streams_dir(base_path);
    if !streams_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for dir in fs::read_dir(&streams_dir)? {
        let dir = dir?;
        if !dir.file_type()?.is_dir() {
            continue;
        }

        let state_dir = dir.path();
        let id = dir.file_name().to_string_lossy().to_string();
        entries.push(load_entry(&id, &state_dir));
    }

    entries.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(entries)
}

fn load_entry(id: &str, state_dir: &Path) -> StreamEntry {
    let (metadata, metadata_error) = match load_metadata(state_dir) {
        Ok(metadata) => (Some(metadata), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let trusted_updated_at = metadata.as_ref().and_then(|metadata| metadata.updated_at);
    let updated_at = trusted_updated_at.or_else(|| directory_timestamp(state_dir));

    match inspect_db(state_dir, metadata.as_ref()) {
        Ok(snapshot) => {
            let parts_total = snapshot.parts_pending + snapshot.parts_uploaded;
            let status = if snapshot.etag.is_some() {
                StreamStatus::Complete
            } else if snapshot.upload_id.is_none() || metadata.is_none() {
                StreamStatus::Broken
            } else {
                determine_status(
                    updated_at,
                    if trusted_updated_at.is_some() {
                        TimestampTrust::Trusted
                    } else {
                        TimestampTrust::Legacy
                    },
                )
            };
            StreamEntry {
                id: id.to_string(),
                state_dir: state_dir.to_path_buf(),
                metadata,
                status,
                upload_id: snapshot.upload_id,
                etag: snapshot.etag,
                parts_total,
                parts_pending: snapshot.parts_pending,
                parts_uploaded: snapshot.parts_uploaded,
                bytes_uploaded: snapshot.bytes_uploaded,
                updated_at,
                error: metadata_error,
            }
        }
        Err(error) => {
            let error_message = metadata_error.unwrap_or_else(|| error.to_string());
            let status = if metadata.is_some() && is_db_lock_error(&error_message) {
                StreamStatus::Active
            } else {
                StreamStatus::Broken
            };

            StreamEntry {
                id: id.to_string(),
                state_dir: state_dir.to_path_buf(),
                metadata,
                status,
                upload_id: None,
                etag: None,
                parts_total: 0,
                parts_pending: 0,
                parts_uploaded: 0,
                bytes_uploaded: 0,
                updated_at,
                error: Some(error_message),
            }
        }
    }
}

fn determine_status(updated_at: Option<u64>, timestamp_trust: TimestampTrust) -> StreamStatus {
    let age = updated_at.and_then(|updated| now_secs().checked_sub(updated));
    if timestamp_trust == TimestampTrust::Legacy {
        return if age.is_some_and(|age| age >= STALE_SECONDS) {
            StreamStatus::Stale
        } else {
            StreamStatus::Resumable
        };
    }

    if age.is_some_and(|age| age <= ACTIVE_SECONDS) {
        StreamStatus::Active
    } else if age.is_some_and(|age| age >= STALE_SECONDS) {
        StreamStatus::Stale
    } else {
        StreamStatus::Resumable
    }
}

fn inspect_db(state_dir: &Path, metadata: Option<&StreamMetadata>) -> Result<DbSnapshot> {
    let db = sled::Config::new()
        .path(state_dir)
        .use_compression(false)
        .mode(sled::Mode::LowSpace)
        .open()?;

    let db_key = metadata
        .map(|metadata| metadata.db_key.clone())
        .or_else(|| infer_db_key(&db))
        .context("missing stream db key")?;

    let upload_id = db
        .get(db_key.as_bytes())?
        .map(|value| String::from_utf8(value.to_vec()))
        .transpose()?;
    let etag = db
        .get(format!("etag {db_key}").as_bytes())?
        .map(|value| String::from_utf8(value.to_vec()))
        .transpose()?;

    let pending_tree = db.open_tree(DB_PARTS)?;
    let uploaded_tree = db.open_tree(DB_UPLOADED)?;

    let mut snapshot = DbSnapshot {
        upload_id,
        etag,
        parts_pending: pending_tree.len(),
        parts_uploaded: uploaded_tree.len(),
        bytes_uploaded: 0,
    };

    for item in uploaded_tree.iter().values() {
        let value = item?;
        let part: Part = from_bytes::<Part, RkyvError>(&value)?;
        snapshot.bytes_uploaded += part.get_chunk();
    }

    Ok(snapshot)
}

fn infer_db_key(db: &sled::Db) -> Option<String> {
    let mut keys = db
        .iter()
        .keys()
        .filter_map(Result::ok)
        .filter_map(|key| String::from_utf8(key.to_vec()).ok())
        .filter(|key| !key.starts_with("etag "))
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    if keys.len() == 1 { keys.pop() } else { None }
}

fn load_metadata(state_dir: &Path) -> Result<StreamMetadata> {
    let file = fs::File::open(state_dir.join(STATE_FILE))?;
    // Keep errors single-line; they are shown in stream listings.
    let options = serde_saphyr::options! { with_snippet: false };
    Ok(serde_saphyr::from_reader_with_options(file, options)?)
}

fn directory_timestamp(state_dir: &Path) -> Option<u64> {
    let metadata = fs::metadata(state_dir).ok()?;
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn is_db_lock_error(error: &str) -> bool {
    error.contains("could not acquire lock")
}

/// # Errors
/// Will return `Err` if cleaning the streams directory fails
pub fn clean_streams(base_path: &Path) -> Result<CleanSummary> {
    let mut summary = CleanSummary::default();

    for entry in scan_streams(base_path)? {
        if matches!(entry.status, StreamStatus::Broken | StreamStatus::Complete) {
            fs::remove_dir_all(&entry.state_dir)?;
            summary.removed.push(entry.id);
        } else {
            summary.kept.push(entry.id);
        }
    }

    Ok(summary)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::{
        s3::{Credentials, Region, S3},
        stream::db::Db,
    };
    use secrecy::SecretString;
    use tempfile::tempdir;

    fn test_s3() -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("bucket".to_string()),
            false,
        )
    }

    #[test]
    fn test_scan_valid_state() {
        let dir = tempdir().unwrap();
        let s3 = test_s3();
        let db = Db::new(&s3, "key", "stream-1", 1, dir.path()).unwrap();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 10, None).unwrap();
        db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            dir.path(),
            &StreamMetadata {
                version: 1,
                id: "stream-1".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "stream-1".to_string(),
                file_size: 10,
                file_mtime: 1,
                part_size: 10,
                db_key: db.state_key().to_string(),
                created_at: now_secs(),
                updated_at: Some(now_secs()),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();
        drop(db);

        let entries = scan_streams(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "stream-1");
        assert!(
            matches!(
                entries[0].status,
                StreamStatus::Active | StreamStatus::Resumable
            ),
            "{:?}",
            entries[0]
        );
        assert_eq!(entries[0].parts_total, 1);
        assert_eq!(entries[0].upload_id.as_deref(), Some("upload-1"));
    }

    #[test]
    fn test_scan_locked_state_is_active_not_broken() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "locked", 1, dir.path()).unwrap();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 10, None).unwrap();
        db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            dir.path(),
            &StreamMetadata {
                version: 1,
                id: "locked".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "locked".to_string(),
                file_size: 10,
                file_mtime: 1,
                part_size: 10,
                db_key: db.state_key().to_string(),
                created_at: now_secs(),
                updated_at: Some(now_secs()),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();

        let entries = scan_streams(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, StreamStatus::Active);
        assert!(
            entries[0]
                .error
                .as_deref()
                .unwrap()
                .contains("could not acquire lock")
        );
    }

    #[test]
    fn test_scan_malformed_state() {
        let dir = tempdir().unwrap();
        let state_dir = state_dir(dir.path(), "broken");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join(STATE_FILE), "not: [valid").unwrap();

        let entries = scan_streams(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "broken");
        assert_eq!(entries[0].status, StreamStatus::Broken);
        assert!(entries[0].error.is_some());
        assert!(
            entries[0]
                .error
                .as_deref()
                .unwrap()
                .contains("unclosed bracket")
        );
        // stream listings show the error on one line, without a source snippet
        assert!(!entries[0].error.as_deref().unwrap().contains('\n'));
    }

    #[test]
    fn test_scan_empty_state_is_broken() {
        let dir = tempdir().unwrap();
        let state_dir = state_dir(dir.path(), "empty");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join(STATE_FILE), "").unwrap();

        let entries = scan_streams(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, StreamStatus::Broken);
        assert!(entries[0].metadata.is_none());
        assert!(entries[0].error.is_some());
    }

    #[test]
    fn test_load_metadata_written_by_serde_yaml_ng() {
        // state.yml exactly as written by s3m <= 0.20.1 (serde_yaml_ng), so
        // streams saved by older versions can still be resumed
        let dir = tempdir().unwrap();
        let state_dir = state_dir(dir.path(), "stream-1");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join(STATE_FILE),
            "version: 1\nid: stream-1\nhost: s3\nbucket: bucket\nkey: 'logs/2026: Q1 #1.csv'\nsource_path: '/data/my files/ünï: yes.csv'\nchecksum: '0123'\nfile_size: 10485760\nfile_mtime: 1760000000123456789\npart_size: 5242880\ndb_key: yes\ncreated_at: 1760000000\nupdated_at: 1760000100\npipe: false\ncompress: true\nencrypt: true\nmode: FileMultipart\n",
        )
        .unwrap();

        let metadata = load_metadata(&state_dir).unwrap();

        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.id, "stream-1");
        assert_eq!(metadata.host, "s3");
        assert_eq!(metadata.bucket, "bucket");
        assert_eq!(metadata.key, "logs/2026: Q1 #1.csv");
        assert_eq!(
            metadata.source_path,
            PathBuf::from("/data/my files/ünï: yes.csv")
        );
        assert_eq!(metadata.checksum, "0123");
        assert_eq!(metadata.file_size, 10_485_760);
        assert_eq!(metadata.file_mtime, 1_760_000_000_123_456_789);
        assert_eq!(metadata.part_size, 5_242_880);
        assert_eq!(metadata.db_key, "yes");
        assert_eq!(metadata.created_at, 1_760_000_000);
        assert_eq!(metadata.updated_at, Some(1_760_000_100));
        assert!(!metadata.pipe);
        assert!(metadata.compress);
        assert!(metadata.encrypt);
        assert_eq!(metadata.mode, StreamMode::FileMultipart);
    }

    #[test]
    fn test_write_metadata_round_trip() {
        let dir = tempdir().unwrap();
        let file_mtime = u128::from(u64::MAX) + 1;
        let tricky = [
            "yes",
            "no",
            "true",
            "null",
            "~",
            "0123",
            "1e3",
            "",
            " lead",
            "trail ",
            "a: b",
            "#hash",
            "- dash",
            "multi\nline",
            "tab\there",
            "'q'",
            "\"dq\"",
            "[x]",
            "{y}",
            "*a",
            "&b",
            "!t",
        ];

        for value in tricky {
            write_metadata(
                dir.path(),
                &StreamMetadata {
                    version: 1,
                    id: "stream-1".to_string(),
                    host: value.to_string(),
                    bucket: value.to_string(),
                    key: value.to_string(),
                    source_path: PathBuf::from(format!("/tmp/{value}")),
                    checksum: value.to_string(),
                    file_size: u64::MAX,
                    file_mtime,
                    part_size: 10,
                    db_key: value.to_string(),
                    created_at: 1,
                    updated_at: None,
                    pipe: true,
                    compress: false,
                    encrypt: true,
                    mode: StreamMode::FileMultipart,
                },
            )
            .unwrap();

            let metadata = load_metadata(&state_dir(dir.path(), "stream-1")).unwrap();

            assert_eq!(metadata.host, value);
            assert_eq!(metadata.bucket, value);
            assert_eq!(metadata.key, value);
            assert_eq!(metadata.source_path, PathBuf::from(format!("/tmp/{value}")));
            assert_eq!(metadata.checksum, value);
            assert_eq!(metadata.db_key, value);
            assert_eq!(metadata.file_size, u64::MAX);
            assert_eq!(metadata.file_mtime, file_mtime);
            // write_metadata fills a missing updated_at from created_at
            assert_eq!(metadata.updated_at, Some(1));
            assert!(metadata.pipe);
            assert!(!metadata.compress);
            assert!(metadata.encrypt);
        }
    }

    #[test]
    fn test_clean_removes_broken_and_keeps_resumable() {
        let dir = tempdir().unwrap();
        let resumable_db = Db::new(&test_s3(), "key", "good", 1, dir.path()).unwrap();
        resumable_db.save_upload_id("upload-1").unwrap();
        resumable_db.create_part(1, 0, 10, None).unwrap();
        resumable_db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            dir.path(),
            &StreamMetadata {
                version: 1,
                id: "good".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "good".to_string(),
                file_size: 10,
                file_mtime: 1,
                part_size: 10,
                db_key: resumable_db.state_key().to_string(),
                created_at: now_secs(),
                updated_at: Some(now_secs()),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();

        let broken_dir = state_dir(dir.path(), "broken");
        fs::create_dir_all(&broken_dir).unwrap();
        fs::write(broken_dir.join(STATE_FILE), "not: [valid").unwrap();

        let summary = clean_streams(dir.path()).unwrap();
        assert_eq!(summary.removed, vec!["broken".to_string()]);
        assert_eq!(summary.kept, vec!["good".to_string()]);
        assert!(state_dir(dir.path(), "good").exists());
        assert!(!state_dir(dir.path(), "broken").exists());
    }

    #[test]
    fn test_touch_metadata_updates_persisted_timestamp() {
        let dir = tempdir().unwrap();
        let created_at = now_secs().saturating_sub(10);
        write_metadata(
            dir.path(),
            &StreamMetadata {
                version: 1,
                id: "stream-1".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "stream-1".to_string(),
                file_size: 10,
                file_mtime: 1,
                part_size: 10,
                db_key: "db-key".to_string(),
                created_at,
                updated_at: Some(created_at),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();

        touch_metadata(&state_dir(dir.path(), "stream-1")).unwrap();
        let metadata = load_metadata(&state_dir(dir.path(), "stream-1")).unwrap();

        assert!(metadata.updated_at.unwrap() >= created_at);
    }

    #[test]
    fn test_scan_legacy_state_without_updated_at_is_not_active() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "legacy", 1, dir.path()).unwrap();
        let db_key = db.state_key().to_string();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 10, None).unwrap();
        db.flush().unwrap();
        drop(db);

        let legacy_dir = state_dir(dir.path(), "legacy");
        fs::write(
            legacy_dir.join(STATE_FILE),
            format!(
                "version: 1\nid: legacy\nhost: s3\nbucket: bucket\nkey: key\nsource_path: /tmp/source\nchecksum: legacy\nfile_size: 10\nfile_mtime: 1\npart_size: 10\ndb_key: {db_key}\ncreated_at: 1\npipe: false\ncompress: false\nencrypt: false\nmode: FileMultipart\n"
            ),
        )
        .unwrap();

        let entries = scan_streams(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "legacy");
        assert!(!matches!(entries[0].status, StreamStatus::Active));
    }
}
