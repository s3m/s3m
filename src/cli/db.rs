use crate::{
    cli::Part,
    s3::{actions, S3},
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
        let db = sled::Config::new()
            .path(format!("{}/streams/{}", path.display(), checksum))
            .use_compression(true)
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
    pub fn create_part(&self, number: u16, seek: u64, chunk: u64) -> Result<Option<sled::IVec>> {
        let part = Part::new(number, seek, chunk);
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
                                    etag: p.get_etag(),
                                    number: p.get_number(),
                                },
                            )
                        })
                        .map_err(Into::into)
                })
            })
            .collect::<Result<BTreeMap<u16, actions::Part>>>()
    }
}
