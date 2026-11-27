//! Ownership of the single database older s3m versions kept for a file hash.
use super::{
    db::{is_lock_error, open_state, open_state_once},
    state::{StreamEntry, StreamMetadata, StreamStatus, load_metadata, read_record},
};
use anyhow::Result;
use std::{collections::BTreeSet, path::Path};

/// An unlocked legacy database can list several destinations even when the YAML
/// names just one. If it is busy, only wait for a matching or unknown owner.
pub(super) fn open_for_key(path: &Path, key: &str) -> sled::Result<Option<sled::Db>> {
    match open_state_once(path) {
        Err(error) if is_lock_error(&error.to_string()) => {
            if load_metadata(path).is_ok_and(|metadata| metadata.db_key != key) {
                Ok(None)
            } else {
                open_state(path).map(Some)
            }
        }
        result => result.map(Some),
    }
}

pub(super) fn unfinished_owners(
    db: &sled::Db,
    metadata: Option<&StreamMetadata>,
) -> Result<BTreeSet<String>> {
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
    let mut unfinished = BTreeSet::new();
    for key in owners {
        if !is_complete(db, &key)? {
            unfinished.insert(key);
        }
    }
    Ok(unfinished)
}

pub(super) fn is_complete(db: &sled::Db, key: &str) -> sled::Result<bool> {
    Ok(db.contains_key(format!("etag {key}"))? && !db.contains_key(format!("abort {key}"))?)
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
    let original = metadata
        .map(|m| &m.db_key)
        .filter(|key| owners.contains(*key))
        .or_else(|| owners.first());
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
