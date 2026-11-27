use anyhow::Result;
use s3m::stream::state::{StreamEntry, scan_streams};
use std::path::{Path, PathBuf};
use tempfile::{TempDir, tempdir};

/// Own the whole stream namespace by setting `S3M_HOME` on the child, including
/// platforms where HOME alone does not override the Known Folder profile.
pub struct StreamHome {
    home: TempDir,
    base: PathBuf,
}

impl StreamHome {
    pub fn new() -> Result<Self> {
        let home = tempdir()?;
        let base = home.path().join(".config/s3m");
        std::fs::create_dir_all(&base)?;
        Ok(Self { home, base })
    }

    pub fn path(&self) -> &Path {
        self.home.path()
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn entries(&self) -> Result<Vec<StreamEntry>> {
        scan_streams(&self.base)
    }
}

impl Drop for StreamHome {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.base) {
            eprintln!("owned stream cleanup {}: {error}", self.base.display());
            assert!(std::thread::panicking(), "owned stream cleanup failed");
        }
    }
}
