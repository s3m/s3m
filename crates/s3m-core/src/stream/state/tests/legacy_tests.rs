//! On-disk records from 0.24.0 and the pre-v2 sandbox.
use super::*;

const PART_SIZE: u64 = 5 * 1024 * 1024;

struct Fixture {
    home: tempfile::TempDir,
    hash: String,
    s3: S3,
}

impl Fixture {
    fn new() -> Self {
        Self {
            home: tempdir().unwrap(),
            hash: blake3::hash(&vec![0; usize::try_from(PART_SIZE).unwrap() + 1])
                .to_hex()
                .to_string(),
            s3: test_s3(),
        }
    }

    fn dir(&self) -> PathBuf {
        state_dir(self.home.path(), &self.hash)
    }

    fn key(&self, object: &str, mtime: u128) -> String {
        format!("{} {object} {mtime}", &self.s3.hash()[..8])
    }

    fn put(&self, object: &str, mtime: u128) -> Result<Db> {
        Db::new(&self.s3, object, &self.hash, mtime, self.home.path())
    }

    // Bypass destination selection just as the old writer did. Each multipart
    // put overwrote the one YAML file. A plain PUT wrote only its ETag record.
    fn multipart(&self, object: &str, mtime: u128, checksum_record: bool, complete: bool) {
        let key = self.key(object, mtime);
        let storage = open_state(&self.dir()).unwrap();
        write_metadata(
            self.home.path(),
            &StreamMetadata {
                version: 1,
                id: self.hash.clone(),
                host: "s3".into(),
                bucket: "bucket".into(),
                key: object.into(),
                source_path: self.home.path().join("source"),
                checksum: self.hash.clone(),
                file_size: PART_SIZE + 1,
                file_mtime: mtime,
                part_size: PART_SIZE,
                db_key: key.clone(),
                created_at: now_secs(),
                updated_at: Some(now_secs()),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();
        if checksum_record {
            storage.insert(format!("checksum {key}"), "").unwrap();
        }
        storage
            .insert(&key, format!("uid-{mtime}").as_bytes())
            .unwrap();
        let parts = storage.open_tree(DB_PARTS).unwrap();
        storage.open_tree(DB_UPLOADED).unwrap();
        if complete {
            storage.insert(format!("etag {key}"), "\"done-2\"").unwrap();
        } else {
            for (number, seek, chunk) in [(1_u16, 0, PART_SIZE), (2, PART_SIZE, 1)] {
                let bytes =
                    rkyv::to_bytes::<RkyvError>(&Part::new(number, seek, chunk, None)).unwrap();
                parts
                    .insert(number.to_be_bytes(), bytes.as_slice())
                    .unwrap();
            }
        }
        storage.flush().unwrap();
    }

    fn complete_pair(&self, second: &str, etag: &str) {
        wait_for_state_lock(&self.dir());
        let entries = scan_streams(self.home.path()).unwrap();
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].status, StreamStatus::Complete);
        for (object, mtime) in [("first", 1), (second, 2)] {
            let db = self.put(object, mtime).unwrap();
            assert_eq!(db.cached_etag().unwrap().as_deref(), Some(etag));
            let path = db.path().to_owned();
            drop(db);
            wait_for_state_lock(&path);
        }
        let summary = clean_streams(self.home.path()).unwrap();
        assert!(summary.removed.contains(&self.hash));
        assert_eq!(summary.kept, Vec::<String>::new());
    }
}

#[test]
fn completed_legacy_single_puts_are_removable_and_rerunnable() {
    let f = Fixture::new();
    let storage = open_state(&f.dir()).unwrap();
    for (object, mtime) in [("first", 1), ("second", 2)] {
        storage
            .insert(format!("etag {}", f.key(object, mtime)), "\"done\"")
            .unwrap();
    }
    storage.flush().unwrap();
    drop(storage);
    f.complete_pair("second", "\"done\"");
}

#[test]
fn completed_legacy_multiparts_are_removable_and_rerunnable() {
    for checksum_record in [false, true] {
        for second in ["first", "second"] {
            let f = Fixture::new();
            f.multipart("first", 1, checksum_record, true);
            f.multipart(second, 2, checksum_record, true);
            f.complete_pair(second, "\"done-2\"");
        }
    }
}

#[test]
fn unfinished_legacy_mtime_owns_the_parts() {
    for checksum_record in [false, true] {
        let f = Fixture::new();
        f.multipart("key", 1, checksum_record, true);
        f.multipart("key", 2, checksum_record, false);
        let open = f.put("key", 2).unwrap();
        assert_eq!(open.upload_id().unwrap().as_deref(), Some("uid-2"));
        assert_eq!(open.path(), f.dir());
        assert_eq!(open.pending_part_ranges().unwrap().len(), 2);
        drop(open);
        let done = f.put("key", 1).unwrap();
        assert!(done.db_parts().unwrap().is_empty());
        assert_eq!(done.cached_etag().unwrap().as_deref(), Some("\"done-2\""));
        drop(done);
        wait_for_state_lock(&f.dir());
        let entries = scan_streams(f.home.path()).unwrap();
        let entry = entries.iter().find(|e| e.id == f.hash).unwrap();
        assert_eq!(entry.status, StreamStatus::Active);
        assert_eq!(entry.parts_pending, 2);
        assert_eq!(clean_streams(f.home.path()).unwrap().kept, [f.hash]);
    }
}

#[test]
fn pending_legacy_abort_still_owns_the_parts() {
    let f = Fixture::new();
    f.multipart("done", 1, true, true);
    f.multipart("pending", 2, true, false);
    let key = f.key("pending", 2);
    let storage = open_state(&f.dir()).unwrap();
    storage
        .insert(format!("etag {key}"), "\"replacement\"")
        .unwrap();
    storage.insert(format!("abort {key}"), "1").unwrap();
    storage.flush().unwrap();
    drop(storage);
    let db = f.put("pending", 2).unwrap();
    assert!(db.abort_pending().unwrap());
    assert_eq!(db.upload_id().unwrap().as_deref(), Some("uid-2"));
    drop(db);
    wait_for_state_lock(&f.dir());
    assert_eq!(clean_streams(f.home.path()).unwrap().kept, [f.hash]);
}

#[test]
fn locked_legacy_upload_only_blocks_its_own_destination() {
    let f = Fixture::new();
    f.multipart("active", 1, true, false);
    let held = f.put("active", 1).unwrap();
    let other = f.put("other", 1).unwrap();
    assert_ne!(held.path(), other.path());
    assert!(other.upload_id().unwrap().is_none());
    assert!(is_lock_error(&f.put("active", 1).unwrap_err().to_string()));
    fs::remove_file(f.dir().join(STATE_FILE)).unwrap();
    assert!(is_lock_error(&f.put("unknown", 1).unwrap_err().to_string()));
}
