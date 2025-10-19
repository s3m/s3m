use crate::{
    cli::{actions::Action, globals::GlobalArgs, progressbar::Bar},
    s3::{S3, actions, tools::throttle_download},
};
use anyhow::{Context, Result, anyhow};
use bytes::{Buf, BytesMut};
use bytesize::ByteSize;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::stream::DecryptorBE32};
use chrono::{DateTime, Utc};
use colored::Colorize;
use http::{HeaderMap, header::CONTENT_TYPE};
use secrecy::ExposeSecret;
use std::{
    cmp::min,
    ffi::{OsStr, OsString},
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
            return handle_metadata(s3, &key, version).await;
        }

        if versions {
            return handle_versions(s3, &key).await;
        }

        let file_name = Path::new(&key)
            .file_name()
            .with_context(|| format!("Failed to get file name from: {key}"))?;

        // do the request first to get the headers
        let action = actions::GetObject::new(&key, version);
        let mut res = action.request(s3, &globals).await?;

        let is_encrypted = is_s3m_encrypted(res.headers());
        let can_decrypt = is_encrypted && globals.enc_key.is_some();

        log::info!(
            "file_name: {}, is_encrypted: {}, can_decrypt: {}",
            file_name.to_string_lossy(),
            is_encrypted,
            can_decrypt
        );

        let final_file_name = determine_final_filename(file_name, can_decrypt);

        let path = get_dest(dest, &final_file_name)?;

        // check if file exists
        if path.is_file() && !force {
            return Err(anyhow!("file {:?} already exists", path));
        }

        let mut file = create_output_file(&path, force).await?;

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

        let mut downloaded = 0u64;
        let mut buffer = BytesMut::new();
        let mut decryptor: Option<DecryptorBE32<ChaCha20Poly1305>> = None;

        let cipher = create_cipher_if_needed(&globals, can_decrypt)?;

        while let Some(chunk) = res.chunk().await? {
            let new = min(downloaded + chunk.len() as u64, file_size);
            downloaded = new;

            buffer.extend_from_slice(&chunk);

            if can_decrypt {
                loop {
                    // Handle initial nonce
                    if decryptor.is_none() {
                        if buffer.len() < 8 {
                            break;
                        }

                        let nonce_len = buffer[0] as usize;

                        if nonce_len != 7 {
                            return Err(anyhow::anyhow!(
                                "Expected nonce length 7, got {}",
                                nonce_len
                            ));
                        }
                        let nonce = &buffer[1..8];

                        decryptor = Some(DecryptorBE32::from_aead(
                            cipher.clone().unwrap(),
                            nonce.into(),
                        ));

                        buffer.advance(8);

                        continue;
                    }

                    // Need 4 bytes for encrypted chunk length
                    if buffer.len() < 4 {
                        break;
                    }

                    let len = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
                    if buffer.len() < 4 + len {
                        break;
                    }

                    let mut encrypted_chunk = buffer[4..4 + len].to_vec();

                    // decrypt_next_in_place modifies the slice in place and returns ()
                    decryptor
                        .as_mut()
                        .unwrap()
                        .decrypt_next_in_place(&[], &mut encrypted_chunk)
                        .map_err(|_| {
                            anyhow::anyhow!("Decryption failed, check your encryption key")
                        })?;

                    file.write_all(&encrypted_chunk).await?;

                    if let Some(pb) = pb.progress.as_ref() {
                        pb.set_position(downloaded);
                    }

                    buffer.advance(4 + len);
                }
            }

            // If encryption is present but we cannot decrypt (no key), store raw
            if is_encrypted && !can_decrypt {
                file.write_all(&buffer).await?;
                buffer.clear();
                if let Some(pb) = pb.progress.as_ref() {
                    pb.set_position(downloaded);
                }
                continue;
            }

            // If not encrypted, write raw
            if !is_encrypted {
                file.write_all(&buffer).await?;
                buffer.clear();
                if let Some(pb) = pb.progress.as_ref() {
                    pb.set_position(downloaded);
                }
            }

            if let Some(bandwidth_kb) = globals.throttle {
                throttle_download(bandwidth_kb, chunk.len()).await?;
            }
        }

        if let Some(pb) = pb.progress.as_ref() {
            pb.finish();
        }
    }

    Ok(())
}

async fn handle_metadata(s3: &S3, key: &str, version: Option<String>) -> Result<()> {
    let action = actions::HeadObject::new(key, version);
    let headers = action.request(s3).await?;

    let max_key_len = headers.keys().map(|k| k.len()).max().unwrap_or(0) + 1;

    for (k, v) in headers {
        println!(
            "{:<width$} {}",
            format!("{k}:").green(),
            v,
            width = max_key_len
        );
    }

    Ok(())
}

async fn handle_versions(s3: &S3, key: &str) -> Result<()> {
    let action = actions::ListObjectVersions::new(key);
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

    Ok(())
}

fn determine_final_filename(file_name: &OsStr, can_decrypt: bool) -> OsString {
    if can_decrypt {
        let file_name_str = file_name.to_string_lossy();
        if let Some(stripped) = file_name_str.strip_suffix(".enc") {
            OsString::from(stripped)
        } else {
            file_name.to_os_string()
        }
    } else {
        file_name.to_os_string()
    }
}

async fn create_output_file(path: &Path, force: bool) -> Result<tokio::fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true);

    if force {
        options.truncate(true);
    }

    options
        .open(path)
        .await
        .with_context(|| format!("could not open {}", path.display()))
}

fn create_cipher_if_needed(
    globals: &GlobalArgs,
    can_decrypt: bool,
) -> Result<Option<ChaCha20Poly1305>> {
    if can_decrypt {
        let key_bytes = globals
            .enc_key
            .as_ref()
            .context("Encryption key is required but not provided")?
            .expose_secret()
            .as_bytes()
            .into();
        Ok(Some(ChaCha20Poly1305::new(key_bytes)))
    } else {
        Ok(None)
    }
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

/// Returns `true` if the Content-Type is `application/vnd.s3m.encrypted`
/// or starts with that (e.g., `application/vnd.s3m.encrypted`)
pub fn is_s3m_encrypted(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with("application/vnd.s3m.encrypted"))
        .unwrap_or(false)
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
