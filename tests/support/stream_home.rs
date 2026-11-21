use anyhow::{Context, Result};
use s3m::stream::state::{StreamEntry, scan_streams, state_dir};
#[cfg(not(unix))]
use std::sync::{Mutex, MutexGuard};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tempfile::{TempDir, tempdir};

// Windows uses one Known Folder profile for every fixture. A stream scan opens
// every database there, so concurrent fixtures would contend for sled locks.
#[cfg(not(unix))]
static PROFILE: Mutex<()> = Mutex::new(());

/// Own only this payload's stream namespace, including on Windows where HOME
/// does not override the Known Folder API used by the CLI.
pub struct StreamHome {
    home: TempDir,
    base: PathBuf,
    id: String,
    owned: PathBuf,
    #[cfg(not(unix))]
    _profile: MutexGuard<'static, ()>,
}

impl StreamHome {
    pub fn new(payload: &[u8]) -> Result<Self> {
        #[cfg(not(unix))]
        let profile_guard = PROFILE
            .lock()
            .map_err(|error| anyhow::anyhow!("stream fixture profile lock: {error}"))?;
        let home = tempdir()?;
        #[cfg(unix)]
        let profile = home.path().to_path_buf();
        #[cfg(not(unix))]
        let profile = dirs::home_dir().context("platform home directory")?;
        let base = profile.join(".config/s3m");
        let id = blake3::hash(payload).to_hex().to_string();
        let owned = state_dir(&base, &id);
        fs::create_dir_all(base.join("streams"))?;
        // Exclusive creation proves ownership. Never adopt or remove prior state.
        fs::create_dir(&owned).context("reserve fresh upload state namespace")?;
        Ok(Self {
            home,
            base,
            id,
            owned,
            #[cfg(not(unix))]
            _profile: profile_guard,
        })
    }

    pub fn path(&self) -> &Path {
        self.home.path()
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn entries(&self) -> Result<Vec<StreamEntry>> {
        Ok(scan_streams(&self.base)?
            .into_iter()
            .filter(|entry| entry.id == self.id)
            .collect())
    }
}

impl Drop for StreamHome {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.owned) {
            eprintln!("owned stream cleanup {}: {error}", self.owned.display());
            assert!(std::thread::panicking(), "owned stream cleanup failed");
        }
    }
}
