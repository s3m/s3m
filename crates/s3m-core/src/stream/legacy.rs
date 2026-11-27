//! Ownership of the single database older s3m versions kept for a file hash.
use super::state::{StreamEntry, StreamMetadata, StreamStatus, read_record};
use anyhow::Result;
use std::{collections::BTreeSet, path::Path};

pub(super) fn owners(db: &sled::Db, metadata: Option<&StreamMetadata>) -> Result<BTreeSet<String>> {
    let mut owners = BTreeSet::new();
    if let Some(metadata) = metadata {
        owners.insert(metadata.db_key.clone());
    }
    for key in db.iter().keys() {
        let key = key?;
        let key = std::str::from_utf8(&key)?;
        let owner = record_owner(key).unwrap_or(key);
        owners.insert(owner.to_owned());
    }
    Ok(owners)
}

pub(super) fn record_owner(key: &str) -> Option<&str> {
    ["etag ", "checksum ", "abort "]
        .iter()
        .find_map(|prefix| key.strip_prefix(prefix))
}

pub(super) const SHARED: &str = "destinations share this legacy state and its parts cannot be \
    attributed. Check each saved upload ID on its original server and abort any open uploads \
    before removing this directory and uploading again";

pub(super) fn shared_entries(
    id: &str,
    path: &Path,
    db: &sled::Db,
    metadata: Option<&StreamMetadata>,
    owners: &BTreeSet<String>,
) -> Result<Vec<StreamEntry>> {
    let original = metadata.map(|m| &m.db_key).or_else(|| owners.first());
    owners
        .iter()
        .map(|key| {
            let metadata = metadata.filter(|m| m.db_key == *key).cloned();
            Ok(StreamEntry {
                id: if Some(key) == original {
                    id.to_owned()
                } else {
                    format!("{id}.{}", blake3::hash(key.as_bytes()).to_hex())
                },
                state_dir: path.to_owned(),
                updated_at: metadata.as_ref().and_then(|m| m.updated_at),
                metadata,
                status: StreamStatus::Broken,
                upload_id: read_record(db, key)?,
                etag: read_record(db, &format!("etag {key}"))?,
                parts_pending: 0,
                parts_uploaded: 0,
                parts_total: 0,
                bytes_uploaded: 0,
                error: Some(format!("saved destination {key:?}: {SHARED}")),
            })
        })
        .collect()
}
