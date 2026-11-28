use std::{io, path::Path, time::SystemTime};

pub(super) struct SourceFile {
    size: u64,
    modified: SystemTime,
}

impl SourceFile {
    pub(super) async fn read(file: &Path) -> io::Result<Self> {
        let metadata = tokio::fs::metadata(file).await?;
        Ok(Self {
            size: metadata.len(),
            modified: metadata.modified()?,
        })
    }

    pub(super) fn change_since_hash(&self, size: u64, mtime: Option<u128>) -> Option<String> {
        if self.size != size {
            return Some(size_change(size, self.size));
        }
        let modified_millis = self
            .modified
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| since.as_millis());
        mtime
            .filter(|expected| modified_millis != Some(*expected))
            .map(|_| "modification time changed since it was read".to_string())
    }

    pub(super) async fn change(&self, file: &Path) -> Option<String> {
        match Self::read(file).await {
            Ok(now) if now.size != self.size => Some(size_change(self.size, now.size)),
            Ok(now) if now.modified != self.modified => {
                Some("modification time changed".to_string())
            }
            Ok(_) => None,
            Err(error) => Some(unverified_file(&error)),
        }
    }
}

fn size_change(before: u64, after: u64) -> String {
    format!("size changed from {before} to {after} bytes")
}

pub(super) fn unverified_file(error: &io::Error) -> String {
    format!("could not verify the file's size and modification time: {error}")
}

pub(super) fn changed_file_reason(change: &str) -> String {
    format!("the file changed during the upload: {change}")
}
