use crate::s3::{actions, S3};
use crate::s3m::Part;
use anyhow::Result;
use serde_cbor::{de::from_reader, to_vec};
use std::collections::BTreeMap;

pub const DB_PARTS: &str = "parts";
pub const DB_UPLOADED: &str = "uploaded parts";

#[derive(Debug, Clone)]
pub struct Stream {
    db: sled::Db,
    key: String,
}

impl Stream {
    /// # Errors
    ///
    /// Will return `Err` if can not create the db
    pub fn new(s3: &S3, key: &str, checksum: &str, mtime: u128, path: &str) -> Result<Self> {
        let key = format!("{} {} {}", &s3.hash()[0..8], key, mtime);
        let db = sled::Config::new()
            .path(format!("{}/.s3m/streams/{}", path, checksum))
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()?;
        Ok(Self { key, db })
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
            .map(|s| String::from_utf8(s.to_vec()).map(|s| format!("ETag: {}", s)))
            .transpose()?;
        Ok(etag.to_owned())
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
        Ok(uid.to_owned())
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
        let cbor_part = to_vec(&part)?;
        Ok(self.db_parts()?.insert(number.to_be_bytes(), cbor_part)?)
    }

    /// # Errors
    ///
    /// Will return `Err` if can not insert a `Part`
    pub fn get_part(&self, number: u16) -> Result<Option<Part>> {
        let part = &self
            .db_parts()?
            .get(number.to_be_bytes())?
            .map(|part| from_reader(&part[..]).map(|p: Part| p))
            .transpose()?;
        Ok(part.to_owned())
    }

    /// # Errors
    ///
    /// Will return `Err` if can not create the `BTreeMap<u16, actions::Part>`
    pub fn uploaded_parts(&self) -> Result<BTreeMap<u16, actions::Part>> {
        Ok(self
            .db_uploaded()?
            .into_iter()
            .values()
            .flat_map(|part| {
                part.map(|part| {
                    from_reader(&part[..])
                        .map(|p: Part| {
                            (
                                p.get_number(),
                                actions::Part {
                                    etag: p.get_etag(),
                                    number: p.get_number(),
                                },
                            )
                        })
                        .map_err(|e| e.into())
                })
            })
            .collect::<Result<BTreeMap<u16, actions::Part>>>()?)
    }
}
