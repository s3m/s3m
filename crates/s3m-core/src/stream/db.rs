use crate::{
    s3::{
        S3, actions,
        checksum::{Checksum, ChecksumAlgorithm},
    },
    stream::{part::Part, state},
};
use anyhow::Result;
use rkyv::{from_bytes, rancor::Error as RkyvError, to_bytes};
use std::{
    collections::BTreeMap,
    convert::Into,
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
        let result = sled::Config::new()
            .path(path)
            .use_compression(false)
            .mode(sled::Mode::LowSpace)
            .open();
        match result {
            Err(error) if attempt < attempts && is_lock_error(&error.to_string()) => {
                attempt += 1;
                sleep(delay);
            }
            result => return result,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Db {
    storage: sled::Db,
    key: String,
    state_dir: PathBuf,
}

impl Db {
    #[must_use]
    pub fn state_dir(path: &Path, checksum: &str) -> PathBuf {
        path.join("streams").join(checksum)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not create the db
    pub fn new(s3: &S3, key: &str, checksum: &str, mtime: u128, path: &Path) -> Result<Self> {
        let key = format!("{} {} {}", &s3.hash()[0..8], key, mtime);
        let state_dir = Self::state_dir(path, checksum);

        log::debug!("db key: [{}], path: {}", key, path.display());

        let storage = open_state(&state_dir)?;
        Ok(Self {
            storage,
            key,
            state_dir,
        })
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open an existing db
    pub fn open_existing(path: &Path, key: String) -> Result<Self> {
        let storage = open_state(path)?;
        Ok(Self {
            storage,
            key,
            state_dir: path.to_path_buf(),
        })
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
        self.storage
            .remove(format!("checksum {}", self.key).as_bytes())?;
        self.storage.remove(&self.key)?;
        self.storage.flush()?;
        state::touch_metadata(&self.state_dir)?;
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
        self.db_uploaded()?
            .into_iter()
            .values()
            .flat_map(|part| {
                part.map(|part| {
                    from_bytes::<Part, RkyvError>(&part)
                        .map(|p| {
                            (
                                p.get_number(),
                                actions::Part {
                                    etag: p.get_etag().to_string(),
                                    number: p.get_number(),
                                    checksum: p.get_checksum(),
                                },
                            )
                        })
                        .map_err(Into::into)
                })
            })
            .collect::<Result<BTreeMap<u16, actions::Part>>>()
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
}
