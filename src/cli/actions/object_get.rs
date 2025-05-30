use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
    s3::{actions, tools::throttle_download, S3},
};
use anyhow::{anyhow, Context, Result};
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use colored::Colorize;
use std::{
    cmp::min,
    ffi::OsStr,
    path::{Path, PathBuf},
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

/// # Errors
/// Will return an error if the action fails
pub async fn handle(s3: &S3, action: Action, globals: GlobalArgs) -> Result<()> {
    if let Action::GetObject {
        key,
        metadata,
        dest,
        quiet,
        force,
        versions,
        version,
    } = action
    {
        if metadata {
            let action = actions::HeadObject::new(&key, version);
            let headers = action.request(s3).await?;

            let mut i = 0;

            for k in headers.keys() {
                i = k.len();
            }

            i += 1;
            for (k, v) in headers {
                println!("{:<width$} {}", format!("{k}:").green(), v, width = i);
            }
        } else if versions {
            let action = actions::ListObjectVersions::new(&key);
            let result = action.request(s3).await?;

            if result.versions.is_empty() {
                println!("No versions found for key: {}", key);
                return Ok(());
            }

            for version in result.versions {
                let dt = DateTime::parse_from_rfc3339(&version.last_modified)?;
                let last_modified: DateTime<Utc> = DateTime::from(dt);
                println!(
                    "{} {:>10} {:<} ID: {}",
                    format!("[{}]", last_modified.format("%F %T %Z")).green(),
                    ByteSize(version.size).to_string().yellow(),
                    if version.is_latest {
                        format!("{} (latest)", version.key)
                    } else {
                        version.key.to_string()
                    },
                    version.version_id
                );
            }
        } else {
            let file_name = Path::new(&key)
                .file_name()
                .with_context(|| format!("Failed to get file name from: {key}"))?;

            let path = get_dest(dest, file_name)?;

            // check if file exists
            if path.is_file() && !force {
                return Err(anyhow!("file {:?} already exists", path));
            }

            // open
            let mut options = OpenOptions::new();
            options.write(true).create(true);

            // Set truncate flag to overwrite the file if it exists
            if force {
                options.truncate(true);
            }

            // do the request
            let action = actions::GetObject::new(&key, version);
            let mut res = action.request(s3, &globals).await?;

            // Open the file with the specified options
            let mut file = options
                .open(&path)
                .await
                .context(format!("could not open {}", &path.display()))?;

            // get the file_size in bytes by using the content_length
            let file_size = res
                .content_length()
                .context("could not get content_length")?;

            // if quiet is true, then use a default progress bar
            let pb = if quiet {
                Bar::default()
            } else {
                Bar::new(file_size)
            };

            let mut downloaded = 0;
            while let Some(bytes) = res.chunk().await? {
                let new = min(downloaded + bytes.len() as u64, file_size);

                downloaded = new;

                if let Some(pb) = pb.progress.as_ref() {
                    pb.set_position(new);
                }

                file.write_all(&bytes).await?;

                // throttle bandwidth
                if let Some(bandwidth_kb) = globals.throttle {
                    throttle_download(bandwidth_kb, bytes.len()).await?;
                }
            }

            if let Some(pb) = pb.progress.as_ref() {
                pb.finish();
            }

            while let Some(bytes) = res.chunk().await? {
                file.write_all(&bytes).await?;
            }
        }
    }

    Ok(())
}

fn get_dest(dest: Option<String>, file_name: &OsStr) -> Result<PathBuf> {
    if let Some(d) = dest {
        let mut path_buf = PathBuf::from(&d);

        // Check if the provided path is a directory
        if path_buf.is_dir() {
            path_buf.push(file_name);
            return Ok(path_buf);
        }

        // If it's a file, check if the parent directory exists
        if let Some(parent) = path_buf.parent() {
            if parent.exists() {
                return Ok(path_buf);
            } else if path_buf.components().count() > 1 {
                return Err(anyhow!(
                    "parent directory {} does not exist",
                    parent.display()
                ));
            }
            return Ok(Path::new(".").join(path_buf));
        }
    }

    // Use default path if dest is None
    Ok(Path::new(".").join(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    struct Test {
        dest: Option<String>,
        file_name: &'static OsStr,
        expected: Option<PathBuf>,
        error_expected: bool,
    }

    #[tokio::test]
    async fn test_get_dest() -> Result<()> {
        let tests = vec![
            Test {
                dest: None,
                file_name: OsStr::new("key.json"),
                expected: Some(Path::new(".").join("key.json")),
                error_expected: false,
            },
            Test {
                dest: Some("./file.txt".to_string()),
                file_name: OsStr::new("key.json"),
                expected: Some(Path::new(".").join("file.txt")),
                error_expected: false,
            },
            Test {
                dest: Some(".".to_string()),
                file_name: OsStr::new("key.json"),
                expected: Some(Path::new(".").join("key.json")),
                error_expected: false,
            },
            Test {
                dest: Some("file.txt".to_string()),
                file_name: OsStr::new("key.json"),
                expected: Some(Path::new(".").join("file.txt")),
                error_expected: false,
            },
            Test {
                dest: Some("/file.txt".to_string()),
                file_name: OsStr::new("key.json"),
                expected: Some(Path::new("/").join("file.txt")),
                error_expected: false,
            },
            Test {
                dest: Some("tmp/file.txt".to_string()),
                file_name: OsStr::new("key.json"),
                expected: None,
                error_expected: true,
            },
            Test {
                dest: Some("a/b/cfile.txt".to_string()),
                file_name: OsStr::new("key.json"),
                expected: None,
                error_expected: true,
            },
        ];

        for test in tests {
            match get_dest(test.dest, test.file_name) {
                Ok(res) => {
                    if test.error_expected {
                        // If an error was not expected but the test passed, fail the test
                        panic!("Expected an error, but got: {:?}", res);
                    } else {
                        assert_eq!(res, test.expected.unwrap());
                    }
                }
                Err(_) => {
                    if !test.error_expected {
                        // If an error was not expected but the test failed, fail the test
                        panic!("Unexpected error");
                    }
                }
            }
        }

        Ok(())
    }
}
