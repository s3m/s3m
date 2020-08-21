use crate::s3::S3;
use anyhow::Result;

pub const DB_PARTS: &str = "parts";
pub const DB_UPLOADED: &str = "uploaded parts";

#[derive(Debug, Clone)]
pub struct Stream {
    db: sled::Db,
    key: String,
}

impl Stream {
    pub fn new(s3: &S3, key: &str, checksum: &str, path: &str) -> Result<Self> {
        let key = format!("{} {}", &s3.hash()[0..8], key);
        let db = sled::Config::new()
            .path(format!("{}/.s3m/streams/{}", path, checksum))
            .open()?;
        Ok(Self { key, db })
    }

    #[must_use]
    pub const fn db(&self) -> &sled::Db {
        &self.db
    }

    pub fn check(&self) -> Result<Option<String>> {
        let etag = &self
            .db
            .get(format!("etag {}", &self.key).as_bytes())?
            .map(|s| String::from_utf8(s.to_vec()).map(|s| format!("ETag: {}", s)))
            .transpose()?;
        Ok(etag.to_owned())
    }

    pub fn upload_id(&self) -> Result<Option<String>> {
        let uid = &self
            .db
            .get(&self.key)?
            .map(|s| String::from_utf8(s.to_vec()))
            .transpose()?;
        Ok(uid.to_owned())
    }

    pub fn db_parts(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree(DB_PARTS)?)
    }

    pub fn db_uploaded(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree(DB_UPLOADED)?)
    }

    pub fn save_upload_id(&self, uid: &str) -> Result<Option<sled::IVec>> {
        Ok(self.db.insert(&self.key, uid)?)
    }

    pub fn save_etag(&self, etag: &str) -> Result<Option<sled::IVec>> {
        Ok(self
            .db
            .insert(format!("etag {}", &self.key).as_bytes(), etag)?)
    }

    pub async fn flush_async(&self) -> Result<usize> {
        Ok(self.db.flush_async().await?)
    }
}
