use crate::{
    s3::{actions, checksum::Checksum, S3},
    stream::part::Part,
};
use anyhow::Result;
use bincode::{deserialize, serialize};
use std::{collections::BTreeMap, convert::Into, path::Path};

pub const DB_PARTS: &str = "parts";
pub const DB_UPLOADED: &str = "uploaded parts";

#[derive(Debug, Clone)]
pub struct Db {
    db: sled::Db,
    key: String,
}

impl Db {
    /// # Errors
    ///
    /// Will return `Err` if can not create the db
    pub fn new(s3: &S3, key: &str, checksum: &str, mtime: u128, path: &Path) -> Result<Self> {
        let key = format!("{} {} {}", &s3.hash()[0..8], key, mtime);

        log::debug!("db key: [{}], path: {}", &key, path.display());

        let db = sled::Config::new()
            .path(format!("{}/streams/{}", path.display(), checksum))
            .use_compression(false)
            .mode(sled::Mode::LowSpace)
            .open()?;
        Ok(Self { db, key })
    }

    #[must_use]
    pub const fn db(&self) -> &sled::Db {
        &self.db
    }

    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn check(&self) -> Result<Option<String>> {
        let etag = &self
            .db
            .get(format!("etag {}", &self.key).as_bytes())?
            .map(|s| String::from_utf8(s.to_vec()).map(|s| format!("ETag: {s}")))
            .transpose()?;
        Ok(etag.clone())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not query the db
    pub fn upload_id(&self) -> Result<Option<String>> {
        let uid = &self
            .db
            .get(&self.key)?
            .map(|s| String::from_utf8(s.to_vec()))
            .transpose()?;
        Ok(uid.clone())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open the tree
    pub fn db_parts(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree(DB_PARTS)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not open the tree
    pub fn db_uploaded(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree(DB_UPLOADED)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert the `upload_id`
    pub fn save_upload_id(&self, uid: &str) -> Result<Option<sled::IVec>> {
        Ok(self.db.insert(&self.key, uid)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert the itag
    pub fn save_etag(&self, etag: &str) -> Result<Option<sled::IVec>> {
        Ok(self
            .db
            .insert(format!("etag {}", &self.key).as_bytes(), etag)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not `flush`
    pub fn flush(&self) -> Result<usize> {
        Ok(self.db.flush()?)
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
        let cbor_part = serialize(&part)?;
        Ok(self.db_parts()?.insert(number.to_be_bytes(), cbor_part)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert a `Part`
    pub fn get_part(&self, number: u16) -> Result<Option<Part>> {
        let part = &self
            .db_parts()?
            .get(number.to_be_bytes())?
            .map(|part| deserialize(&part[..]))
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
                    deserialize(&part[..])
                        .map(|p: Part| {
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
mod tests {
    use super::*;
    use crate::s3::{credentials::Credentials, Region, S3};
    use anyhow::Result;
    use secrecy::Secret;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_db() -> Result<()> {
        let dir = tempdir()?;
        let path = PathBuf::from(dir.path());
        let s3 = S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
                &Secret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
}
