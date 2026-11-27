use crate::{
    s3::{
        S3, actions,
        checksum::{Checksum, ChecksumAlgorithm},
    },
    stream::{legacy, part::Part, state},
};
use anyhow::{Result, ensure};
use rkyv::{from_bytes, rancor::Error as RkyvError, to_bytes};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

pub const DB_PARTS: &str = "parts";
pub const DB_UPLOADED: &str = "uploaded parts";

/// How long to wait for the state lock before reporting it as held: sled can keep
/// the lock for a moment after a handle to the same state is dropped in this
/// process (a background flush still owns the file). A lock held by another s3m
/// that is uploading outlasts the wait and is still reported.
const LOCK_ATTEMPTS: u32 = 10;
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);

pub(crate) fn is_lock_error(error: &str) -> bool {
    error.contains("could not acquire lock")
}

/// Open the sled database holding an upload's state, waiting briefly for its lock.
///
/// # Errors
///
/// Will return `Err` if the database can not be opened, or if its lock is still
/// held after a short wait
pub(crate) fn open_state(path: &Path) -> sled::Result<sled::Db> {
    open_state_with(path, LOCK_ATTEMPTS, LOCK_RETRY_DELAY)
}

/// Open an upload's state without waiting for its lock: for listing the states,
/// where a held lock means another s3m is uploading and waiting would only slow
/// the listing down.
///
/// # Errors
///
/// Will return `Err` if the database can not be opened or its lock is held
pub(crate) fn open_state_once(path: &Path) -> sled::Result<sled::Db> {
    open_state_with(path, 1, Duration::ZERO)
}

fn open_state_with(path: &Path, attempts: u32, delay: Duration) -> sled::Result<sled::Db> {
    let mut attempt = 1;
    loop {
        let result = try_open_state(path);
        match result {
            Err(error) if attempt < attempts && is_busy(&error) => {
                attempt += 1;
                sleep(delay);
            }
            result => return result,
        }
    }
}

pub(super) fn is_busy(error: &sled::Error) -> bool {
    matches!(error, sled::Error::Io(e) if e.kind() == io::ErrorKind::WouldBlock)
        || is_lock_error(&error.to_string())
}

/// Outside the directory being removed so Windows can delete every file in it.
pub(super) fn cleanup_lock(streams: &Path) -> io::Result<File> {
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(streams.join(".clean.lock"))
}

const CLEANING: &str = "stream cleanup in progress";

fn try_open_state(path: &Path) -> sled::Result<sled::Db> {
    // A cleaner takes the exclusive lock only from its final inspection until
    // unlink. The shared lock covers this open attempt, never a running upload
    // or the sleep between sled lock retries.
    let _opening = if let Some(streams) = path.parent().filter(|p| p.ends_with("streams")) {
        fs::create_dir_all(streams)?;
        let lock = cleanup_lock(streams)?;
        lock.try_lock_shared().map_err(|error| match error {
            fs::TryLockError::WouldBlock => io::Error::new(io::ErrorKind::WouldBlock, CLEANING),
            fs::TryLockError::Error(error) => error,
        })?;
        Some(lock)
    } else {
        None
    };
    sled::Config::new()
        .path(path)
        .use_compression(false)
        .mode(sled::Mode::LowSpace)
        .open()
}

#[derive(Debug, Clone)]
pub struct Db {
    storage: sled::Db,
    key: String,
    state_dir: PathBuf,
}

impl Db {
    /// Resolve a physical directory name. For a handle opened by [`Db::new`],
    /// [`Db::path`] gives the selected directory, including legacy state.
    #[must_use]
    pub fn state_dir(path: &Path, id: &str) -> PathBuf {
        state::state_dir(path, id)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not create the db
    pub fn new(s3: &S3, key: &str, checksum: &str, mtime: u128, path: &Path) -> Result<Self> {
        let state_key = format!("{} {key} {mtime}", s3.upload_host());
        let state_dir = Self::destination_dir(path, checksum, &state_key);
        if state_dir.try_exists()? {
            return Self::open(&state_dir, state_key);
        }
        let previous_key = format!("{} {key} {mtime}", s3.previous_upload_host());
        if previous_key != state_key {
            let previous_dir = Self::destination_dir(path, checksum, &previous_key);
            if previous_dir.try_exists()? {
                return Self::open(&previous_dir, previous_key);
            }
        }

        let mut completed_etag = None;
        let old_dir = Self::state_dir(path, checksum);
        let host: String = s3.hash().chars().take(8).collect();
        let old_key = format!("{host} {key} {mtime}");
        if old_dir.try_exists()?
            && let Some(storage) = legacy::open_for_key(&old_dir, &old_key)?
        {
            let metadata = state::load_metadata(&old_dir).ok();
            let owners = legacy::unfinished_owners(&storage, metadata.as_ref())?;
            if owners.contains(&old_key) {
                ensure!(
                    owners.len() == 1,
                    "{}: {}",
                    old_dir.display(),
                    legacy::SHARED
                );
                return Ok(Self {
                    storage,
                    key: old_key,
                    state_dir: old_dir,
                });
            }
            // A completed key can still have a cached ETag and multipart ID.
            // Carry only the ETag for the HEAD check into its own directory.
            completed_etag = storage.get(format!("etag {old_key}"))?;
        }

        let db = Self::open(&state_dir, state_key)?;
        if let Some(etag) = completed_etag
            && db.storage.is_empty()
        {
            db.storage.insert(format!("etag {}", db.key), etag)?;
            db.storage.flush()?;
        }
        Ok(db)
    }

    fn destination_dir(base: &Path, checksum: &str, key: &str) -> PathBuf {
        let mut hash = blake3::Hasher::new();
        hash.update(checksum.as_bytes());
        hash.update(&[0]);
        hash.update(key.as_bytes());
        Self::state_dir(base, &format!("v2-{}", hash.finalize()))
    }

    fn open(path: &Path, key: String) -> Result<Self> {
        Ok(Self {
            storage: open_state(path)?,
            key,
            state_dir: path.to_owned(),
        })
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open an existing db
    pub fn open_existing(path: &Path, key: String) -> Result<Self> {
        let storage = open_state(path)?;
        let metadata = state::load_metadata(path).ok();
        let owners = legacy::unfinished_owners(&storage, metadata.as_ref())?;
        ensure!(owners.len() <= 1, "{}: {}", path.display(), legacy::SHARED);
        ensure!(
            owners.contains(&key)
                || (owners.is_empty()
                    && (storage.is_empty() || legacy::is_complete(&storage, &key)?)),
            "upload state belongs to another destination"
        );
        Ok(Self {
            storage,
            key,
            state_dir: path.to_path_buf(),
        })
    }

    /// The directory holding this upload's database and metadata.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.state_dir
    }

    #[must_use]
    pub const fn db(&self) -> &sled::Db {
        &self.storage
    }

    #[must_use]
    pub fn state_key(&self) -> &str {
        &self.key
    }

    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn check(&self) -> Result<Option<String>> {
        let etag = &self
            .storage
            .get(format!("etag {}", self.key).as_bytes())?
            .map(|s| String::from_utf8(s.to_vec()).map(|s| format!("ETag: {s}")))
            .transpose()?;
        Ok(etag.clone())
    }

    /// The `ETag` saved by the last completed upload of this file to this key, as the
    /// server returned it (usually quoted).
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn cached_etag(&self) -> Result<Option<String>> {
        Ok(self
            .storage
            .get(format!("etag {}", self.key).as_bytes())?
            .map(|s| String::from_utf8(s.to_vec()))
            .transpose()?)
    }

    /// Forget a completed upload of this file to this key: its saved `ETag` and its
    /// multipart `upload_id`. The next upload then starts from scratch instead of
    /// reporting the cached `ETag` or resuming an upload the server already completed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not update the db
    pub fn forget_upload(&self) -> Result<()> {
        self.storage
            .remove(format!("etag {}", self.key).as_bytes())?;
        self.remove_multipart_records()?;
        self.flush()?;
        Ok(())
    }

    /// Record that the multipart upload saved for this file was replaced by a
    /// single-part upload but could not be aborted: it is still open on the server.
    /// The saved `ETag` is kept (the object is stored), the state doesn't count as
    /// complete (`streams clean` keeps it), and the next run retries the abort.
    ///
    /// # Errors
    /// Returns an error if the saved state cannot be updated.
    pub fn mark_abort_pending(&self) -> Result<()> {
        self.storage
            .insert(format!("abort {}", self.key).as_bytes(), &b"1"[..])?;
        self.flush()?;
        Ok(())
    }

    /// Whether [`Db::mark_abort_pending`] recorded an upload that still has to be
    /// aborted.
    ///
    /// # Errors
    /// Returns an error if the saved state cannot be read.
    pub fn abort_pending(&self) -> Result<bool> {
        Ok(self
            .storage
            .contains_key(format!("abort {}", self.key).as_bytes())?)
    }

    /// Forget the saved `ETag` only, keeping the multipart upload id and parts (and a
    /// pending abort).
    ///
    /// # Errors
    /// Returns an error if the saved state cannot be updated.
    pub fn forget_etag(&self) -> Result<()> {
        self.storage
            .remove(format!("etag {}", self.key).as_bytes())?;
        self.flush()?;
        Ok(())
    }

    /// Clear the multipart upload id and parts while keeping the saved `ETag`.
    ///
    /// # Errors
    /// Returns an error if the saved state cannot be updated.
    pub fn forget_multipart_upload(&self) -> Result<()> {
        self.db_parts()?.clear()?;
        self.db_uploaded()?.clear()?;
        self.remove_multipart_records()?;
        self.flush()?;
        Ok(())
    }

    fn remove_multipart_records(&self) -> Result<()> {
        self.storage
            .remove(format!("checksum {}", self.key).as_bytes())?;
        self.storage
            .remove(format!("abort {}", self.key).as_bytes())?;
        self.storage.remove(&self.key)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn upload_id(&self) -> Result<Option<String>> {
        let uid = &self
            .storage
            .get(&self.key)?
            .map(|s| String::from_utf8(s.to_vec()))
            .transpose()?;
        Ok(uid.clone())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open the tree
    pub fn db_parts(&self) -> Result<sled::Tree> {
        Ok(self.storage.open_tree(DB_PARTS)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open the tree
    pub fn db_uploaded(&self) -> Result<sled::Tree> {
        Ok(self.storage.open_tree(DB_UPLOADED)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert the `upload_id`
    pub fn save_upload_id(&self, uid: &str) -> Result<Option<sled::IVec>> {
        let previous = self.storage.insert(&self.key, uid)?;
        self.storage.flush()?;
        state::touch_metadata(&self.state_dir)?;
        Ok(previous)
    }

    /// Record the additional checksum algorithm the multipart upload is created with
    /// (`None`: no additional checksum), so a resume uses the same one.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not update the db
    pub fn save_upload_checksum(&self, algorithm: Option<&ChecksumAlgorithm>) -> Result<()> {
        self.storage.insert(
            format!("checksum {}", self.key).as_bytes(),
            algorithm.map_or("", ChecksumAlgorithm::name),
        )?;
        self.storage.flush()?;
        Ok(())
    }

    /// The additional checksum algorithm recorded for the multipart upload in
    /// progress. The outer `None` means nothing was recorded (a state written by an
    /// older s3m); `Some(None)` means the upload has no additional checksum.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not query the db, or if the record is not a known
    /// algorithm
    pub fn upload_checksum(&self) -> Result<Option<Option<ChecksumAlgorithm>>> {
        let Some(saved) = self
            .storage
            .get(format!("checksum {}", self.key).as_bytes())?
        else {
            return Ok(None);
        };
        let name = String::from_utf8(saved.to_vec())?;
        if name.is_empty() {
            return Ok(Some(None));
        }
        name.parse::<ChecksumAlgorithm>()
            .map(|algorithm| Some(Some(algorithm)))
            .map_err(|()| anyhow::anyhow!("unknown checksum algorithm in the upload state: {name}"))
    }

    /// The additional checksum algorithm to continue the upload in progress with:
    /// the one it was created with, never what a later run asks for, because the
    /// server's upload is bound to it.
    ///
    /// The record made at creation decides. A state written by an older s3m has no
    /// record: its recorded parts decide then (they were sent with that algorithm, or
    /// with none), and `requested` only when no part was recorded yet.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn resume_checksum(
        &self,
        requested: Option<&ChecksumAlgorithm>,
    ) -> Result<Option<ChecksumAlgorithm>> {
        if let Some(saved) = self.upload_checksum()? {
            return Ok(saved);
        }
        if self.db_parts()?.is_empty() && self.db_uploaded()?.is_empty() {
            return Ok(requested.cloned());
        }
        self.parts_checksum_algorithm()
    }

    /// The checksum algorithm of the recorded parts, pending and uploaded: what a
    /// state without [`Db::upload_checksum`] still knows. `None` unless there are
    /// parts and every one of them has a checksum of the same algorithm.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not read the parts
    pub fn parts_checksum_algorithm(&self) -> Result<Option<ChecksumAlgorithm>> {
        let mut algorithm = None;
        for tree in [self.db_uploaded()?, self.db_parts()?] {
            for part in tree.iter().values() {
                let part = from_bytes::<Part, RkyvError>(&part?)?;
                let Some(checksum) = part.get_checksum() else {
                    return Ok(None);
                };
                match &algorithm {
                    None => algorithm = Some(checksum.algorithm),
                    Some(first) if *first != checksum.algorithm => return Ok(None),
                    Some(_) => {}
                }
            }
        }
        Ok(algorithm)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert the itag
    pub fn save_etag(&self, etag: &str) -> Result<Option<sled::IVec>> {
        let previous = self
            .storage
            .insert(format!("etag {}", self.key).as_bytes(), etag)?;
        self.storage.flush()?;
        state::touch_metadata(&self.state_dir)?;
        Ok(previous)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not `flush`
    pub fn flush(&self) -> Result<usize> {
        let flushed = self.storage.flush()?;
        state::touch_metadata(&self.state_dir)?;
        Ok(flushed)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert a `Part`
    pub fn create_part(
        &self,
        number: u16,
        seek: u64,
        chunk: u64,
        checksum: Option<Checksum>,
    ) -> Result<Option<sled::IVec>> {
        let part = Part::new(number, seek, chunk, checksum);
        let bytes = to_bytes::<RkyvError>(&part)?;
        let previous = self
            .db_parts()?
            .insert(number.to_be_bytes(), bytes.as_slice())?;
        state::touch_metadata(&self.state_dir)?;
        Ok(previous)
    }

    /// Record the parts to upload, `(number, seek, chunk)` each, in one atomic write:
    /// after a crash the state has all of them or none. A plan written part by part
    /// could be cut short, and an upload resumed from it would be completed without
    /// the missing parts (<https://github.com/s3m/s3m/issues/83>).
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not write the parts
    pub fn create_parts(
        &self,
        parts: impl IntoIterator<Item = (u16, u64, u64)>,
        checksum: Option<&Checksum>,
    ) -> Result<()> {
        let mut batch = sled::Batch::default();
        for (number, seek, chunk) in parts {
            let part = Part::new(number, seek, chunk, checksum.cloned());
            let bytes = to_bytes::<RkyvError>(&part)?;
            batch.insert(&number.to_be_bytes(), bytes.as_slice());
        }
        let tree = self.db_parts()?;
        tree.apply_batch(batch)?;
        tree.flush()?;
        state::touch_metadata(&self.state_dir)?;
        Ok(())
    }

    /// The byte range, `(seek, chunk)`, of every part still to upload, by part number.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not read the parts
    pub fn pending_part_ranges(&self) -> Result<BTreeMap<u16, (u64, u64)>> {
        Self::ranges(&self.db_parts()?)
    }

    /// The byte range, `(seek, chunk)`, of every uploaded part, by part number.
    ///
    /// # Errors
    ///
    /// Will return `Err` if can not read the parts
    pub fn uploaded_part_ranges(&self) -> Result<BTreeMap<u16, (u64, u64)>> {
        Self::ranges(&self.db_uploaded()?)
    }

    fn ranges(tree: &sled::Tree) -> Result<BTreeMap<u16, (u64, u64)>> {
        tree.iter()
            .values()
            .map(|part| {
                let part = from_bytes::<Part, RkyvError>(&part?)?;
                Ok((part.get_number(), (part.get_seek(), part.get_chunk())))
            })
            .collect()
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert a `Part`
    pub fn get_part(&self, number: u16) -> Result<Option<Part>> {
        let part = &self
            .db_parts()?
            .get(number.to_be_bytes())?
            .map(|part| from_bytes::<Part, RkyvError>(&part))
            .transpose()?;
        Ok(part.clone())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not create the `BTreeMap<u16, actions::Part>`
    pub fn uploaded_parts(&self) -> Result<BTreeMap<u16, actions::Part>> {
        // A part that can't be read is an error: left out, the upload would be
        // completed without it.
        self.db_uploaded()?
            .iter()
            .values()
            .map(|part| {
                let part = from_bytes::<Part, RkyvError>(&part?)?;
                Ok((
                    part.get_number(),
                    actions::Part {
                        etag: part.get_etag().to_string(),
                        number: part.get_number(),
                        checksum: part.get_checksum(),
                    },
                ))
            })
            .collect()
    }
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
    use crate::s3::{Region, S3, credentials::Credentials};
    use anyhow::Result;
    use secrecy::SecretString;
    use std::path::PathBuf;
    use tempfile::tempdir;

    // sled can keep the lock for a moment after a handle is dropped in this process;
    // opening the same state again waits for it instead of failing.
    #[test]
    fn test_open_state_waits_for_a_lock_that_is_being_released() {
        let dir = tempdir().unwrap();
        let held = open_state(dir.path()).unwrap();
        let release = std::thread::spawn(move || {
            sleep(Duration::from_millis(50));
            drop(held);
        });

        // A budget of several seconds, so a slow machine can't run out of attempts.
        let reopened = open_state_with(dir.path(), 500, Duration::from_millis(10));

        release.join().unwrap();
        assert!(reopened.is_ok(), "{:?}", reopened.err());
    }

    // A lock that stays held (another s3m is uploading) is still reported.
    #[test]
    fn test_open_state_reports_a_lock_that_stays_held() {
        let dir = tempdir().unwrap();
        let held = open_state(dir.path()).unwrap();

        let error = open_state(dir.path()).unwrap_err().to_string();
        assert!(is_lock_error(&error), "{error}");
        let error = open_state_once(dir.path()).unwrap_err().to_string();
        assert!(is_lock_error(&error), "{error}");
        drop(held);
    }

    // #77 review: the parts of an older state name an algorithm only when all of them
    // agree. A part without a checksum, or with another algorithm, means there is no
    // single algorithm to continue with.
    #[test]
    fn test_parts_checksum_algorithm_needs_every_part_to_agree() {
        let checksum = |algorithm| Some(Checksum::new(algorithm));
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "checksum", 1, dir.path()).unwrap();
        assert_eq!(db.parts_checksum_algorithm().unwrap(), None);

        db.create_part(1, 0, 5, checksum(ChecksumAlgorithm::Sha256))
            .unwrap();
        db.create_part(2, 5, 5, checksum(ChecksumAlgorithm::Sha256))
            .unwrap();
        assert_eq!(
            db.parts_checksum_algorithm().unwrap(),
            Some(ChecksumAlgorithm::Sha256)
        );

        db.create_part(3, 10, 5, None).unwrap();
        assert_eq!(db.parts_checksum_algorithm().unwrap(), None);

        db.db_parts().unwrap().clear().unwrap();
        db.create_part(1, 0, 5, checksum(ChecksumAlgorithm::Sha256))
            .unwrap();
        db.create_part(2, 5, 5, checksum(ChecksumAlgorithm::Crc32c))
            .unwrap();
        assert_eq!(db.parts_checksum_algorithm().unwrap(), None);
    }

    // <https://github.com/s3m/s3m/issues/83>: the plan is written in one batch, with
    // its checksum algorithm, and the ranges of pending and uploaded parts read back
    // by part number.
    #[test]
    fn test_create_parts_and_part_ranges() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "checksum", 1, dir.path()).unwrap();
        assert!(db.pending_part_ranges().unwrap().is_empty());

        let checksum = Checksum::new(ChecksumAlgorithm::Crc32c);
        db.create_parts([(1, 0, 5), (2, 5, 5), (3, 10, 1)], Some(&checksum))
            .unwrap();

        assert_eq!(db.db_parts().unwrap().len(), 3);
        assert_eq!(
            db.parts_checksum_algorithm().unwrap(),
            Some(ChecksumAlgorithm::Crc32c)
        );
        assert_eq!(
            db.pending_part_ranges().unwrap(),
            BTreeMap::from([(1, (0, 5)), (2, (5, 5)), (3, (10, 1))])
        );
        assert!(db.uploaded_part_ranges().unwrap().is_empty());

        // Part 2 uploaded: it moves to the other tree.
        let part = db
            .get_part(2)
            .unwrap()
            .unwrap()
            .set_etag("\"e\"".to_string());
        db.db_uploaded()
            .unwrap()
            .insert(
                2_u16.to_be_bytes(),
                to_bytes::<RkyvError>(&part).unwrap().as_slice(),
            )
            .unwrap();
        db.db_parts().unwrap().remove(2_u16.to_be_bytes()).unwrap();
        assert_eq!(
            db.pending_part_ranges().unwrap(),
            BTreeMap::from([(1, (0, 5)), (3, (10, 1))])
        );
        assert_eq!(
            db.uploaded_part_ranges().unwrap(),
            BTreeMap::from([(2, (5, 5))])
        );
        assert_eq!(
            db.uploaded_parts().unwrap().keys().collect::<Vec<_>>(),
            [&2]
        );
    }

    // #83: an uploaded part that can't be decoded is an error for both readers, never a
    // part left out of the completion.
    #[test]
    fn test_uploaded_parts_reports_a_part_it_cannot_read() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "checksum", 1, dir.path()).unwrap();
        db.db_uploaded()
            .unwrap()
            .insert(1_u16.to_be_bytes(), &b"not a part"[..])
            .unwrap();

        assert!(db.uploaded_parts().is_err());
        assert!(db.uploaded_part_ranges().is_err());
    }

    // <https://github.com/s3m/s3m/issues/77>: the algorithm of the upload in progress
    // is recorded with it, and forgotten with it.
    #[test]
    fn test_upload_checksum_is_saved_and_forgotten() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "checksum", 1, dir.path()).unwrap();
        assert_eq!(db.upload_checksum().unwrap(), None);

        db.save_upload_checksum(None).unwrap();
        assert_eq!(db.upload_checksum().unwrap(), Some(None));

        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ] {
            db.save_upload_checksum(Some(&algorithm)).unwrap();
            assert_eq!(db.upload_checksum().unwrap(), Some(Some(algorithm)));
        }

        db.forget_upload().unwrap();
        assert_eq!(db.upload_checksum().unwrap(), None);
    }

    fn test_s3() -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        )
    }

    #[test]
    fn test_forget_upload_clears_etag_and_upload_id_of_this_key_only() -> Result<()> {
        let dir = tempdir()?;
        let s3 = test_s3();
        let db = Db::new(&s3, "key", "checksum", 0, dir.path())?;
        // Another key in the same state directory (same content): sled locks the directory,
        // so the second handle shares the storage, as two keys of one file do.
        let other = Db {
            storage: db.storage.clone(),
            key: format!("{} other 0", &s3.hash()[0..8]),
            state_dir: db.state_dir.clone(),
        };

        db.save_upload_id("uid")?;
        db.save_etag("\"abc\"")?;
        other.save_upload_id("other-uid")?;
        other.save_etag("\"def\"")?;
        assert_eq!(db.cached_etag()?, Some("\"abc\"".to_string()));

        db.forget_upload()?;

        assert_eq!(db.cached_etag()?, None);
        assert_eq!(db.check()?, None);
        assert_eq!(db.upload_id()?, None);
        assert_eq!(other.cached_etag()?, Some("\"def\"".to_string()));
        assert_eq!(other.upload_id()?, Some("other-uid".to_string()));
        Ok(())
    }

    #[test]
    fn independent_endpoints_and_buckets_can_hold_the_same_key_open() {
        let dir = tempdir().unwrap();
        let host = |endpoint, bucket: &str| {
            S3::new(
                &Credentials::new("mock-access", &SecretString::new("mock-secret".into())),
                &Region::custom("test", endpoint),
                Some(bucket.to_owned()),
                false,
            )
        };
        let original = Db::new(
            &host("http://127.0.0.1:1", "bucket"),
            "key",
            "file",
            1,
            dir.path(),
        )
        .unwrap();
        original.save_upload_id("pending-abort").unwrap();
        original.mark_abort_pending().unwrap();
        original.save_etag("single-put").unwrap();
        for destination in [
            host("http://127.0.0.1:2", "bucket"),
            host("http://127.0.0.1:1", "other"),
        ] {
            let other = Db::new(&destination, "key", "file", 1, dir.path()).unwrap();
            assert_ne!(other.path(), original.path());
            assert_eq!(other.upload_id().unwrap(), None);
            assert!(!other.abort_pending().unwrap());
            assert_eq!(other.cached_etag().unwrap(), None);
            other.save_upload_id("second").unwrap();
            other.forget_upload().unwrap();
        }
        assert!(original.abort_pending().unwrap());
        assert_eq!(
            original.upload_id().unwrap().as_deref(),
            Some("pending-abort")
        );
        assert_eq!(
            original.cached_etag().unwrap().as_deref(),
            Some("single-put")
        );
        assert!(!original.state_key().contains("mock-access"));
        assert!(!original.state_key().contains("mock-secret"));
    }

    #[test]
    fn test_db() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;
        assert_eq!(db.db().len(), 0);
        assert_eq!(db.db_parts()?.len(), 0);
        assert_eq!(db.db_uploaded()?.len(), 0);
        assert_eq!(db.check()?, None);
        assert_eq!(db.upload_id()?, None);
        assert_eq!(db.save_upload_id("uid")?, None);
        assert_eq!(db.upload_id()?, Some("uid".to_string()));
        assert_eq!(db.save_etag("etag")?, None);
        assert!(db.flush().is_ok());
        assert_eq!(db.flush()?, 0);
        assert_eq!(db.check()?, Some("ETag: etag".to_string()));
        assert_eq!(db.upload_id()?, Some("uid".to_string()));
        assert_eq!(db.db().len(), 2);
        assert_eq!(db.db_parts()?.len(), 0);
        assert_eq!(db.db_uploaded()?.len(), 0);
        assert_eq!(db.create_part(1, 0, 0, None)?, None);
        assert_eq!(db.create_part(2, 0, 0, None)?, None);
        assert_eq!(db.db().len(), 2);
        assert_eq!(db.db_parts()?.len(), 2);
        assert_eq!(db.db_uploaded()?.len(), 0);
        assert_eq!(db.get_part(1)?.unwrap().get_number(), 1);
        assert_eq!(db.get_part(2)?.unwrap().get_number(), 2);
        assert_eq!(db.uploaded_parts()?.len(), 0);
        assert!(db.flush().is_ok());
        assert_eq!(db.db_parts()?.len(), 2);
        assert_eq!(db.db_uploaded()?.len(), 0);
        assert_eq!(db.uploaded_parts()?.len(), 0);
        assert_eq!(db.get_part(1)?.unwrap().get_number(), 1);
        assert_eq!(db.get_part(2)?.unwrap().get_number(), 2);
        Ok(())
    }

    #[test]
    fn test_db_parts() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;
        assert_eq!(db.db_parts()?.len(), 0);
        assert_eq!(db.create_part(1, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 1);
        assert_eq!(db.create_part(2, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 2);
        assert_eq!(db.create_part(3, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 3);
        assert_eq!(db.create_part(4, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 4);
        assert_eq!(db.create_part(5, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 5);
        assert_eq!(db.create_part(6, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 6);
        assert_eq!(db.create_part(7, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 7);
        assert_eq!(db.create_part(8, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 8);
        assert_eq!(db.create_part(9, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 9);
        assert_eq!(db.create_part(10, 0, 0, None)?, None);
        assert_eq!(db.db_parts()?.len(), 10);
        assert_eq!(db.db_parts()?.iter().count(), 10);
        Ok(())
    }

    #[test]
    fn test_db_uploaded() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;
        assert_eq!(db.db_uploaded()?.len(), 0);
        assert_eq!(
            db.db_uploaded()?
                .insert(1_i32.to_be_bytes(), "etag".as_bytes())?,
            None
        );
        assert_eq!(db.db_uploaded()?.len(), 1);
        assert_eq!(
            db.db_uploaded()?
                .insert(2_i32.to_be_bytes(), "etag".as_bytes())?,
            None
        );
        assert_eq!(db.db_uploaded()?.len(), 2);
        Ok(())
    }

    #[test]
    fn test_db_part_rkyv_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;

        // Create a part with all fields populated
        let checksum =
            crate::s3::checksum::Checksum::new(crate::s3::checksum::ChecksumAlgorithm::Sha256);
        db.create_part(1, 1024, 5_242_880, Some(checksum))?;

        // Retrieve and verify
        let part = db.get_part(1)?.unwrap();
        assert_eq!(part.get_number(), 1);
        assert_eq!(part.get_seek(), 1024);
        assert_eq!(part.get_chunk(), 5_242_880);
        assert!(part.get_checksum().is_some());
        assert_eq!(
            part.get_checksum().unwrap().algorithm,
            crate::s3::checksum::ChecksumAlgorithm::Sha256
        );
        Ok(())
    }

    #[test]
    fn test_db_part_rkyv_multiple_parts() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;

        // Create multiple parts with different checksums
        for i in 1..=10 {
            let checksum =
                crate::s3::checksum::Checksum::new(crate::s3::checksum::ChecksumAlgorithm::Crc32c);
            db.create_part(i, u64::from(i) * 5_242_880, 5_242_880, Some(checksum))?;
        }

        // Verify all parts
        for i in 1..=10 {
            let part = db.get_part(i)?.unwrap();
            assert_eq!(part.get_number(), i);
            assert_eq!(part.get_seek(), u64::from(i) * 5_242_880);
            assert_eq!(part.get_chunk(), 5_242_880);
        }
        Ok(())
    }

    #[test]
    fn test_db_part_rkyv_without_checksum() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;

        // Create part without checksum
        db.create_part(1, 0, 1024, None)?;

        let part = db.get_part(1)?.unwrap();
        assert_eq!(part.get_number(), 1);
        assert!(part.get_checksum().is_none());
        Ok(())
    }

    #[test]
    fn test_db_uploaded_parts_rkyv() -> Result<()> {
        use rkyv::{rancor::Error as RkyvError, to_bytes};

        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some("awsexamplebucket1".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "checksum", 0, &path)?;

        // Manually insert serialized parts into uploaded tree
        let mut checksum =
            crate::s3::checksum::Checksum::new(crate::s3::checksum::ChecksumAlgorithm::Crc32c);
        checksum.checksum = "yZRlqg==".to_string();

        let part = Part::new(1, 0, 5_242_880, Some(checksum))
            .set_etag("\"d41d8cd98f00b204e9800998ecf8427e\"".to_string());

        let bytes = to_bytes::<RkyvError>(&part)?;
        db.db_uploaded()?
            .insert(1_u16.to_be_bytes(), bytes.as_slice())?;

        // Verify uploaded_parts() can deserialize
        let uploaded = db.uploaded_parts()?;
        assert_eq!(uploaded.len(), 1);
        let action_part = uploaded.get(&1).unwrap();
        assert_eq!(action_part.number, 1);
        assert_eq!(action_part.etag, "\"d41d8cd98f00b204e9800998ecf8427e\"");
        assert!(action_part.checksum.is_some());
        Ok(())
    }

    // #86: an upload replaced by a single PUT whose abort failed is marked; the mark
    // goes with the multipart records when the abort succeeds later.
    #[test]
    fn test_abort_pending_is_marked_and_forgotten_with_the_upload() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3(), "key", "checksum", 1, dir.path()).unwrap();
        db.save_upload_id("UP1").unwrap();
        db.save_etag("\"single\"").unwrap();
        assert!(!db.abort_pending().unwrap());

        db.mark_abort_pending().unwrap();
        assert!(db.abort_pending().unwrap());
        assert_eq!(db.cached_etag().unwrap().as_deref(), Some("\"single\""));

        db.forget_multipart_upload().unwrap();
        assert!(!db.abort_pending().unwrap());
        assert_eq!(db.upload_id().unwrap(), None);
        assert_eq!(db.cached_etag().unwrap().as_deref(), Some("\"single\""));
    }
}
