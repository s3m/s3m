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
use serde::Serialize;
use std::{
    cmp::min,
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

#[derive(Debug, Serialize, PartialEq, Eq)]
struct MetadataJsonOutput {
    bucket: Option<String>,
    key: String,
    version_id: Option<String>,
    content_length: Option<u64>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    storage_class: Option<String>,
    checksum_crc32: Option<String>,
    checksum_crc32c: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
    metadata: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct VersionsJsonOutput {
    bucket: Option<String>,
    key_prefix: String,
    versions: Vec<VersionJsonEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct VersionJsonEntry {
    key: String,
    version_id: String,
    is_latest: bool,
    last_modified: String,
    size_bytes: u64,
    etag: String,
    storage_class: String,
}

/// # Errors
/// Will return an error if the action fails
#[allow(clippy::too_many_lines, clippy::missing_panics_doc)]
pub async fn handle(s3: &S3, action: Action, globals: GlobalArgs) -> Result<()> {
    if let Action::GetObject {
        key,
        metadata,
        dest,
        quiet,
        force,
        json,
        versions,
        version,
    } = action
    {
        if metadata {
            return handle_metadata(s3, &key, version, json).await;
        }

        if versions {
            return handle_versions(s3, &key, json).await;
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
            return Err(anyhow!("file {} already exists", path.display()));
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

                        let nonce_len =
                            *buffer.first().context("Failed to read nonce length")? as usize;

                        if nonce_len != 7 {
                            return Err(anyhow::anyhow!(
                                "Expected nonce length 7, got {nonce_len}"
                            ));
                        }
                        let nonce = buffer.get(1..8).context("Failed to get nonce bytes")?;

                        let cipher_instance = cipher
                            .clone()
                            .context("Cipher not initialized for decryption")?;
                        decryptor = Some(DecryptorBE32::from_aead(cipher_instance, nonce.into()));

                        buffer.advance(8);

                        continue;
                    }

                    // Need 4 bytes for encrypted chunk length
                    if buffer.len() < 4 {
                        break;
                    }

                    let len_bytes = buffer.get(..4).context("Failed to read chunk length")?;
                    let len = u32::from_be_bytes(
                        len_bytes
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("Invalid chunk length bytes"))?,
                    ) as usize;
                    if buffer.len() < 4 + len {
                        break;
                    }

                    let mut encrypted_chunk = buffer
                        .get(4..4 + len)
                        .context("Failed to read encrypted chunk")?
                        .to_vec();

                    // decrypt_next_in_place modifies the slice in place and returns ()
                    decryptor
                        .as_mut()
                        .context("Decryptor not initialized")?
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

async fn handle_metadata(s3: &S3, key: &str, version: Option<String>, json: bool) -> Result<()> {
    let action = actions::HeadObject::new(key, version.clone());
    let headers = action.request(s3).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json_metadata_output(s3, key, version, headers)?)?
        );
        return Ok(());
    }

    let max_key_len = headers
        .keys()
        .map(std::string::String::len)
        .max()
        .unwrap_or(0)
        + 1;

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

async fn handle_versions(s3: &S3, key: &str, json: bool) -> Result<()> {
    let action = actions::ListObjectVersions::new(key);
    let result = action.request(s3).await?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&VersionsJsonOutput {
                bucket: s3.bucket().map(str::to_string),
                key_prefix: key.to_string(),
                versions: result
                    .versions
                    .into_iter()
                    .map(|version| VersionJsonEntry {
                        key: version.key,
                        version_id: version.version_id,
                        is_latest: version.is_latest,
                        last_modified: version.last_modified,
                        size_bytes: version.size,
                        etag: version.e_tag,
                        storage_class: version.storage_class,
                    })
                    .collect(),
            })?
        );
        return Ok(());
    }

    if result.versions.is_empty() {
        println!("No versions found for key: {key}");
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
                version.key.clone()
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
        .is_some_and(|ct| ct.starts_with("application/vnd.s3m.encrypted"))
}

fn json_metadata_output(
    s3: &S3,
    key: &str,
    version: Option<String>,
    headers: BTreeMap<String, String>,
) -> Result<MetadataJsonOutput> {
    let metadata = headers
        .iter()
        .filter_map(|(header, value)| {
            header
                .strip_prefix("x-amz-meta-")
                .map(|metadata_key| (metadata_key.to_string(), value.clone()))
        })
        .collect();

    Ok(MetadataJsonOutput {
        bucket: s3.bucket().map(str::to_string),
        key: key.to_string(),
        version_id: version,
        content_length: headers
            .get("content-length")
            .and_then(|value| value.parse::<u64>().ok()),
        content_type: headers.get("content-type").cloned(),
        etag: headers.get("etag").cloned(),
        last_modified: headers
            .get("last-modified")
            .map(|value| normalize_json_timestamp(value))
            .transpose()?,
        storage_class: headers.get("x-amz-storage-class").cloned(),
        checksum_crc32: headers.get("x-amz-checksum-crc32").cloned(),
        checksum_crc32c: headers.get("x-amz-checksum-crc32c").cloned(),
        checksum_sha1: headers.get("x-amz-checksum-sha1").cloned(),
        checksum_sha256: headers.get("x-amz-checksum-sha256").cloned(),
        metadata,
        headers,
    })
}

fn normalize_json_timestamp(value: &str) -> Result<String> {
    DateTime::parse_from_rfc2822(value)
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .map(|timestamp| timestamp.with_timezone(&Utc).to_rfc3339())
        .map_err(|error| anyhow!("Failed to parse Last-Modified header '{value}': {error}"))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::s3::{Credentials, Region, S3};
    use anyhow::Result;
    use mockito::{Matcher, Server};
    use secrecy::SecretString;

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
                        panic!("Expected an error, but got: {res:?}");
                    } else {
                        assert_eq!(res, test.expected.unwrap());
                    }
                }
                Err(_) => {
                    // If an error was not expected but the test failed, fail the test
                    assert!(test.error_expected, "Unexpected error");
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_json_metadata_output_extracts_fields() {
        let mut headers = BTreeMap::new();
        headers.insert("content-length".to_string(), "42".to_string());
        headers.insert("content-type".to_string(), "text/plain".to_string());
        headers.insert("etag".to_string(), "\"etag\"".to_string());
        headers.insert(
            "last-modified".to_string(),
            "Sat, 14 Mar 2026 08:00:00 GMT".to_string(),
        );
        headers.insert("x-amz-meta-owner".to_string(), "alice".to_string());
        let s3 = S3::new(
            &crate::s3::Credentials::new("AKIA", &secrecy::SecretString::new("secret".into())),
            &"us-east-1".parse::<crate::s3::Region>().unwrap(),
            Some("bucket".to_string()),
            false,
        );

        let output =
            json_metadata_output(&s3, "path/file.txt", Some("v1".to_string()), headers).unwrap();
        let rendered = serde_json::to_value(output).unwrap();

        assert_eq!(rendered["bucket"], "bucket");
        assert_eq!(rendered["key"], "path/file.txt");
        assert_eq!(rendered["version_id"], "v1");
        assert_eq!(rendered["content_length"], 42);
        assert_eq!(rendered["last_modified"], "2026-03-14T08:00:00+00:00");
        assert_eq!(rendered["metadata"]["owner"], "alice");
    }

    #[test]
    fn test_json_metadata_output_rejects_invalid_last_modified() {
        let mut headers = BTreeMap::new();
        headers.insert("last-modified".to_string(), "not-a-date".to_string());
        let s3 = S3::new(
            &crate::s3::Credentials::new("AKIA", &secrecy::SecretString::new("secret".into())),
            &"us-east-1".parse::<crate::s3::Region>().unwrap(),
            Some("bucket".to_string()),
            false,
        );

        let err = json_metadata_output(&s3, "path/file.txt", None, headers)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to parse Last-Modified header"));
    }

    fn test_s3(endpoint: String) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &Region::custom("us-west-1", endpoint),
            Some("bucket".to_string()),
            false,
        )
    }

    #[tokio::test]
    async fn test_handle_metadata_json_branch() {
        let mut server = Server::new_async().await;
        let _head = server
            .mock("HEAD", "/bucket/file.txt")
            .with_status(200)
            .with_header("content-length", "42")
            .with_header("content-type", "text/plain")
            .with_header("etag", "\"head-etag\"")
            .with_header("x-amz-meta-owner", "alice")
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::GetObject {
                dest: None,
                metadata: true,
                key: "file.txt".to_string(),
                quiet: false,
                force: false,
                json: true,
                versions: false,
                version: None,
            },
            GlobalArgs::new(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_handle_versions_json_branch() {
        let mut server = Server::new_async().await;
        let _versions = server
            .mock("GET", "/bucket")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("prefix".into(), "prefix".into()),
                Matcher::UrlEncoded("versions".into(), String::new()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult><Name>bucket</Name><Prefix>prefix</Prefix><KeyMarker></KeyMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Version><Key>prefix</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><LastModified>2026-03-14T00:00:00.000Z</LastModified><ETag>"etag"</ETag><Size>5</Size><Owner><ID>owner</ID></Owner><StorageClass>STANDARD</StorageClass></Version></ListVersionsResult>"#,
            )
            .create_async()
            .await;

        handle(
            &test_s3(server.url()),
            Action::GetObject {
                dest: None,
                metadata: false,
                key: "prefix".to_string(),
                quiet: false,
                force: false,
                json: true,
                versions: true,
                version: None,
            },
            GlobalArgs::new(),
        )
        .await
        .unwrap();
    }
}
