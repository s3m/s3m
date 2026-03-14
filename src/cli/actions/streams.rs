use crate::{
    cli::{
        Config,
        actions::{Action, StreamCommand},
        globals::GlobalArgs,
        start::get_host,
    },
    s3::{Credentials, Region, S3},
    stream::{
        db::Db,
        state::{
            CleanSummary, StreamEntry, StreamMetadata, StreamStatus, clean_streams, scan_streams,
        },
        upload_multipart::upload_multipart,
    },
};
use anyhow::{Context, Result, anyhow};
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use colored::Colorize;
use secrecy::SecretString;
use serde::Serialize;
use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StreamEntryJson {
    id: String,
    status: String,
    state_dir: String,
    source: Option<String>,
    destination: Option<String>,
    host: Option<String>,
    upload_id: Option<String>,
    checksum: Option<String>,
    file_size_bytes: Option<u64>,
    part_size_bytes: Option<u64>,
    parts_total: usize,
    parts_uploaded: usize,
    parts_pending: usize,
    bytes_uploaded: u64,
    created_at: Option<String>,
    updated_at: Option<String>,
    etag: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct StreamsListJsonOutput {
    streams: Vec<StreamEntryJson>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct CleanSummaryJsonOutput {
    removed: Vec<String>,
    kept: Vec<String>,
}

/// # Errors
/// Will return an error if the action fails
pub async fn handle(action: Action, globals: GlobalArgs) -> Result<()> {
    if let Action::Streams {
        command,
        config_file,
        json,
        s3m_dir,
        number,
    } = action
    {
        match command {
            StreamCommand::List => {
                let entries = scan_streams(&s3m_dir)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&StreamsListJsonOutput {
                            streams: entries.iter().map(stream_entry_json).collect(),
                        })?
                    );
                } else {
                    print!("{}", render_stream_list(&entries));
                }
            }
            StreamCommand::Show { id } => {
                let entry = find_entry(&s3m_dir, &id)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&stream_entry_json(&entry))?
                    );
                } else {
                    print!("{}", render_stream_show(&entry));
                }
            }
            StreamCommand::Resume { id } => {
                resume_stream(&config_file, &s3m_dir, &id, number, globals).await?;
            }
            StreamCommand::Clean => {
                let summary = clean_streams(&s3m_dir)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&CleanSummaryJsonOutput {
                            removed: summary.removed,
                            kept: summary.kept,
                        })?
                    );
                } else {
                    print!("{}", render_clean_summary(&summary));
                }
            }
        }
    }

    Ok(())
}

fn render_stream_list(entries: &[StreamEntry]) -> String {
    if entries.is_empty() {
        return "No stream state found\n".to_string();
    }

    let mut lines = Vec::with_capacity(entries.len() + 2);
    lines.push(format_list_header());
    lines.extend(entries.iter().map(format_list_line));

    let mut output = lines.join("\n");
    output.push('\n');
    output
}

fn render_stream_show(entry: &StreamEntry) -> String {
    let mut lines = vec![
        format!("id: {}", entry.id),
        format!("status: {}", entry.status.as_str()),
        format!("state_dir: {}", entry.state_dir.display()),
    ];

    if let Some(metadata) = &entry.metadata {
        lines.extend([
            format!("source: {}", format_source(metadata)),
            format!("destination: {}/{}", metadata.bucket, metadata.key),
            format!("host: {}", metadata.host),
            format!("upload_id: {}", entry.upload_id.as_deref().unwrap_or("-")),
            format!("checksum: {}", metadata.checksum),
            format!("file_size: {}", ByteSize(metadata.file_size)),
            format!("part_size: {}", ByteSize(metadata.part_size)),
            format!("parts_total: {}", entry.parts_total),
            format!("parts_uploaded: {}", entry.parts_uploaded),
            format!("parts_pending: {}", entry.parts_pending),
            format!("bytes_uploaded: {}", ByteSize(entry.bytes_uploaded)),
            format!(
                "created_at: {}",
                format_timestamp(Some(metadata.created_at))
            ),
        ]);
    } else {
        lines.extend([
            "source: -".to_string(),
            "destination: -".to_string(),
            "host: -".to_string(),
            format!("upload_id: {}", entry.upload_id.as_deref().unwrap_or("-")),
            "checksum: -".to_string(),
            "file_size: -".to_string(),
            "part_size: -".to_string(),
            format!("parts_total: {}", entry.parts_total),
            format!("parts_uploaded: {}", entry.parts_uploaded),
            format!("parts_pending: {}", entry.parts_pending),
            format!("bytes_uploaded: {}", ByteSize(entry.bytes_uploaded)),
            "created_at: -".to_string(),
        ]);
    }

    lines.extend([
        format!("updated_at: {}", format_timestamp(entry.updated_at)),
        format!("etag: {}", entry.etag.as_deref().unwrap_or("-")),
    ]);

    if let Some(error) = &entry.error {
        lines.push(format!("error: {error}"));
    }

    lines.join("\n") + "\n"
}

fn format_list_line(entry: &StreamEntry) -> String {
    let destination = entry.metadata.as_ref().map_or_else(
        || "-".to_string(),
        |metadata| format!("{}/{}", metadata.bucket, metadata.key),
    );
    let source = entry
        .metadata
        .as_ref()
        .map_or_else(|| "-".to_string(), format_source);
    let updated = format!("[{}]", format_timestamp(entry.updated_at)).green();
    let bytes_uploaded = ByteSize(entry.bytes_uploaded).to_string().yellow();
    let upload_id = format_upload_id_for_list(entry.upload_id.as_deref()).yellow();
    let status = format_status(entry.status);

    format!(
        "{:<64} {:<12} {} {:>10} {:>7} {:<24} {:<28} {}",
        entry.id,
        status,
        updated,
        bytes_uploaded,
        format!("{}/{}", entry.parts_uploaded, entry.parts_total),
        upload_id,
        destination,
        source,
    )
}

fn format_list_header() -> String {
    format!(
        "{:<64} {:<12} {:<22} {:>10} {:>7} {:<24} {:<28} {}",
        "ID", "STATUS", "UPDATED_AT", "UPLOADED", "PARTS", "UPLOAD_ID", "DESTINATION", "SOURCE"
    )
    .bold()
    .to_string()
}

fn format_upload_id_for_list(upload_id: Option<&str>) -> String {
    match upload_id {
        Some(upload_id) if upload_id.len() > 24 => {
            let prefix = &upload_id[..21];
            format!("{prefix}...")
        }
        Some(upload_id) => upload_id.to_string(),
        None => "-".to_string(),
    }
}

fn format_status(status: StreamStatus) -> colored::ColoredString {
    match status {
        StreamStatus::Active | StreamStatus::Complete => status.as_str().green(),
        StreamStatus::Resumable => status.as_str().cyan(),
        StreamStatus::Stale => status.as_str().yellow(),
        StreamStatus::Broken => status.as_str().red(),
    }
}

fn format_source(metadata: &StreamMetadata) -> String {
    if metadata.pipe {
        "stdin/pipe".to_string()
    } else {
        metadata.source_path.display().to_string()
    }
}

fn format_timestamp(timestamp: Option<u64>) -> String {
    timestamp.map_or_else(
        || "-".to_string(),
        |ts| {
            let dt = DateTime::<Utc>::from(UNIX_EPOCH + std::time::Duration::from_secs(ts));
            dt.format("%F %T UTC").to_string()
        },
    )
}

fn format_timestamp_rfc3339(timestamp: Option<u64>) -> String {
    timestamp.map_or_else(
        || "-".to_string(),
        |ts| {
            let dt = DateTime::<Utc>::from(UNIX_EPOCH + std::time::Duration::from_secs(ts));
            dt.to_rfc3339()
        },
    )
}

fn render_clean_summary(summary: &CleanSummary) -> String {
    let mut lines = vec![format!(
        "Removed {} stream state entr{}",
        summary.removed.len(),
        if summary.removed.len() == 1 {
            "y"
        } else {
            "ies"
        }
    )];

    lines.extend(summary.removed.iter().map(|id| format!(" - {id}")));

    if !summary.kept.is_empty() {
        lines.push(format!(
            "Kept {} active/resumable entr{}",
            summary.kept.len(),
            if summary.kept.len() == 1 { "y" } else { "ies" }
        ));
    }

    lines.join("\n") + "\n"
}

fn stream_entry_json(entry: &StreamEntry) -> StreamEntryJson {
    StreamEntryJson {
        id: entry.id.clone(),
        status: entry.status.as_str().to_string(),
        state_dir: entry.state_dir.display().to_string(),
        source: entry.metadata.as_ref().map(format_source),
        destination: entry
            .metadata
            .as_ref()
            .map(|metadata| format!("{}/{}", metadata.bucket, metadata.key)),
        host: entry
            .metadata
            .as_ref()
            .map(|metadata| metadata.host.clone()),
        upload_id: entry.upload_id.clone(),
        checksum: entry
            .metadata
            .as_ref()
            .map(|metadata| metadata.checksum.clone()),
        file_size_bytes: entry.metadata.as_ref().map(|metadata| metadata.file_size),
        part_size_bytes: entry.metadata.as_ref().map(|metadata| metadata.part_size),
        parts_total: entry.parts_total,
        parts_uploaded: entry.parts_uploaded,
        parts_pending: entry.parts_pending,
        bytes_uploaded: entry.bytes_uploaded,
        created_at: entry
            .metadata
            .as_ref()
            .map(|metadata| format_timestamp_rfc3339(Some(metadata.created_at))),
        updated_at: entry
            .updated_at
            .map(|_| format_timestamp_rfc3339(entry.updated_at)),
        etag: entry.etag.clone(),
        error: entry.error.clone(),
    }
}

fn find_entry(streams_dir: &Path, id: &str) -> Result<StreamEntry> {
    scan_streams(streams_dir)?
        .into_iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| anyhow!("Unknown stream state id: {id}"))
}

fn config_dir(config_file: &Path) -> &Path {
    config_file.parent().unwrap_or(config_file)
}

fn calculate_max_requests(number: u8) -> u8 {
    number.max(1)
}

fn validate_resume_entry(entry: &StreamEntry, id: &str) -> Result<StreamMetadata> {
    if matches!(entry.status, StreamStatus::Broken | StreamStatus::Complete) {
        return Err(anyhow!(
            "Stream state {id} is not resumable (status: {})",
            entry.status.as_str()
        ));
    }

    let stream_metadata = entry
        .metadata
        .clone()
        .ok_or_else(|| anyhow!("Stream state {id} is missing metadata"))?;

    if stream_metadata.pipe || stream_metadata.compress || stream_metadata.encrypt {
        return Err(anyhow!(
            "Stream state {id} is not resumable through `streams resume`"
        ));
    }

    Ok(stream_metadata)
}

fn validate_resume_file(stream_metadata: &StreamMetadata) -> Result<fs::Metadata> {
    let file_metadata = fs::metadata(&stream_metadata.source_path).with_context(|| {
        format!(
            "Source file missing or unreadable: {}",
            stream_metadata.source_path.display()
        )
    })?;

    let current_mtime = file_metadata
        .modified()
        .unwrap_or_else(|_| SystemTime::now())
        .duration_since(UNIX_EPOCH)?
        .as_millis();

    if current_mtime != stream_metadata.file_mtime {
        return Err(anyhow!(
            "Source file changed since the multipart state was created: {}",
            stream_metadata.source_path.display()
        ));
    }

    let current_checksum =
        crate::cli::actions::object_put::blake3_checksum(&stream_metadata.source_path, true)?;
    if current_checksum != stream_metadata.checksum {
        return Err(anyhow!(
            "Source file checksum no longer matches saved stream state: {}",
            stream_metadata.source_path.display()
        ));
    }

    Ok(file_metadata)
}

fn validate_resume_db(
    entry: &StreamEntry,
    stream_metadata: &StreamMetadata,
    id: &str,
) -> Result<Db> {
    let db = Db::open_existing(&entry.state_dir, stream_metadata.db_key.clone())?;
    if db.upload_id()?.is_none() {
        return Err(anyhow!("Stream state {id} has no saved upload id"));
    }

    if db.check()?.is_some() {
        return Err(anyhow!("Stream state {id} is already complete"));
    }

    if entry.parts_total == 0 {
        return Err(anyhow!(
            "Stream state {id} has no multipart parts to resume"
        ));
    }

    Ok(db)
}

async fn resume_stream(
    config_file: &Path,
    streams_dir: &Path,
    id: &str,
    number: u8,
    globals: GlobalArgs,
) -> Result<()> {
    let entry = find_entry(streams_dir, id)?;
    let stream_metadata = validate_resume_entry(&entry, id)?;
    let file_metadata = validate_resume_file(&stream_metadata)?;

    let config = Config::new(config_file.to_path_buf())?;
    let location = crate::cli::s3_location::parse_location(
        &format!(
            "{}/{}/{}",
            stream_metadata.host, stream_metadata.bucket, stream_metadata.key
        ),
        false,
        false,
    )?;
    let host = get_host(&config, config_dir(config_file), &location)?;
    let region = host.get_region()?;
    let credentials = Credentials::new(&host.access_key, &host.secret_key);
    let s3 = S3::new(
        &credentials,
        &region,
        Some(stream_metadata.bucket.clone()),
        false,
    );

    let db = validate_resume_db(&entry, &stream_metadata, id)?;

    let file_size = file_metadata.len();
    let max_requests = calculate_max_requests(number);

    let etag = upload_multipart(
        &s3,
        &stream_metadata.key,
        &stream_metadata.source_path,
        file_size,
        stream_metadata.part_size,
        &db,
        None,
        None,
        false,
        None,
        max_requests,
        globals,
    )
    .await?;

    println!("{etag}");

    Ok(())
}

/// # Errors
/// Will return `Err` if stream state cleanup fails
pub fn clean_streams_state(streams_dir: &Path) -> Result<CleanSummary> {
    clean_streams(streams_dir)
}

fn placeholder_s3() -> S3 {
    S3::new(
        &Credentials::new("", &SecretString::new("".into())),
        &Region::aws("us-east-1"),
        None,
        true,
    )
}

#[must_use]
pub fn placeholder() -> S3 {
    placeholder_s3()
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
    use crate::{
        cli::globals::GlobalArgs,
        s3::{Credentials, Region},
        stream::{
            db::Db,
            state::{StreamMode, write_metadata},
        },
    };
    use secrecy::SecretString;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tempfile::tempdir;

    fn test_s3(bucket: &str) -> S3 {
        S3::new(
            &Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                &SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
            ),
            &"us-west-1".parse::<Region>().unwrap(),
            Some(bucket.to_string()),
            false,
        )
    }

    #[test]
    fn test_find_entry_unknown() {
        let dir = tempdir().unwrap();
        let err = find_entry(dir.path(), "missing").unwrap_err().to_string();
        assert!(err.contains("Unknown stream state id: missing"));
    }

    #[test]
    fn test_render_stream_show_includes_details() {
        let dir = tempdir().unwrap();
        let output = render_stream_show(&StreamEntry {
            id: "stream-1".to_string(),
            state_dir: dir.path().join("stream-1"),
            metadata: Some(StreamMetadata {
                version: 1,
                id: "stream-1".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "checksum".to_string(),
                file_size: 2048,
                file_mtime: 1,
                part_size: 1024,
                db_key: "db-key".to_string(),
                created_at: 1,
                updated_at: Some(1),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            }),
            status: StreamStatus::Resumable,
            upload_id: Some("upload-1".to_string()),
            etag: None,
            parts_total: 2,
            parts_pending: 1,
            parts_uploaded: 1,
            bytes_uploaded: 1024,
            updated_at: Some(2),
            error: Some("broken metadata".to_string()),
        });

        assert!(output.contains("id: stream-1"));
        assert!(output.contains("destination: bucket/key"));
        assert!(output.contains("upload_id: upload-1"));
        assert!(output.contains("parts_pending: 1"));
        assert!(output.contains("error: broken metadata"));
    }

    #[test]
    fn test_render_stream_list_handles_empty() {
        assert_eq!(render_stream_list(&[]), "No stream state found\n");
    }

    #[test]
    fn test_render_stream_list_includes_header() {
        let output = render_stream_list(&[StreamEntry {
            id: "stream-1".to_string(),
            state_dir: PathBuf::from("/tmp/stream-1"),
            metadata: None,
            status: StreamStatus::Broken,
            upload_id: None,
            etag: None,
            parts_total: 0,
            parts_pending: 0,
            parts_uploaded: 0,
            bytes_uploaded: 0,
            updated_at: Some(2),
            error: None,
        }]);

        assert!(output.contains("ID"));
        assert!(output.contains("STATUS"));
        assert!(output.contains("UPLOAD_ID"));
    }

    #[test]
    fn test_render_stream_list_does_not_wrap_id_in_brackets() {
        let output = render_stream_list(&[StreamEntry {
            id: "stream-1".to_string(),
            state_dir: PathBuf::from("/tmp/stream-1"),
            metadata: Some(StreamMetadata {
                version: 1,
                id: "stream-1".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "checksum".to_string(),
                file_size: 1024,
                file_mtime: 1,
                part_size: 512,
                db_key: "db-key".to_string(),
                created_at: 1,
                updated_at: Some(1),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            }),
            status: StreamStatus::Resumable,
            upload_id: Some("upload-1".to_string()),
            etag: None,
            parts_total: 2,
            parts_pending: 1,
            parts_uploaded: 1,
            bytes_uploaded: 512,
            updated_at: Some(2),
            error: None,
        }]);

        assert!(output.contains("stream-1"));
        assert!(!output.contains("[stream-1]"));
    }

    #[test]
    fn test_render_stream_list_truncates_upload_id() {
        let output = render_stream_list(&[StreamEntry {
            id: "stream-1".to_string(),
            state_dir: PathBuf::from("/tmp/stream-1"),
            metadata: None,
            status: StreamStatus::Complete,
            upload_id: Some("abcdefghijklmnopqrstuvwxyz0123456789".to_string()),
            etag: None,
            parts_total: 0,
            parts_pending: 0,
            parts_uploaded: 0,
            bytes_uploaded: 0,
            updated_at: Some(2),
            error: None,
        }]);

        assert!(output.contains("abcdefghijklmnopqrstu..."));
        assert!(!output.contains("abcdefghijklmnopqrstuvwxyz0123456789"));
    }

    #[test]
    fn test_stream_entry_json_shape() {
        let entry = StreamEntry {
            id: "stream-1".to_string(),
            state_dir: PathBuf::from("/tmp/stream-1"),
            metadata: None,
            status: StreamStatus::Broken,
            upload_id: None,
            etag: None,
            parts_total: 0,
            parts_pending: 0,
            parts_uploaded: 0,
            bytes_uploaded: 0,
            updated_at: None,
            error: Some("bad state".to_string()),
        };

        let rendered = serde_json::to_value(stream_entry_json(&entry)).unwrap();
        assert_eq!(rendered["id"], "stream-1");
        assert_eq!(rendered["status"], "broken");
        assert_eq!(rendered["error"], "bad state");
    }

    #[test]
    fn test_clean_streams_state_keeps_resumable_entries() {
        let dir = tempdir().unwrap();
        let db = Db::new(&test_s3("bucket"), "key", "good", 1, dir.path()).unwrap();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 1, None).unwrap();
        db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            dir.path(),
            &StreamMetadata {
                version: 1,
                id: "good".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/file"),
                checksum: "good".to_string(),
                file_size: 1,
                file_mtime: 1,
                part_size: 1,
                db_key: db.state_key().to_string(),
                created_at: 1,
                updated_at: Some(1),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();

        let summary = clean_streams_state(dir.path()).unwrap();
        assert!(summary.removed.is_empty());
        assert_eq!(summary.kept, vec!["good".to_string()]);
    }

    #[test]
    fn test_validate_resume_entry_rejects_pipe_state() {
        let entry = StreamEntry {
            id: "pipe".to_string(),
            state_dir: PathBuf::from("/tmp/pipe"),
            metadata: Some(StreamMetadata {
                version: 1,
                id: "pipe".to_string(),
                host: "s3".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: PathBuf::from("/tmp/source"),
                checksum: "checksum".to_string(),
                file_size: 1,
                file_mtime: 1,
                part_size: 1,
                db_key: "db-key".to_string(),
                created_at: 1,
                updated_at: Some(1),
                pipe: true,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            }),
            status: StreamStatus::Resumable,
            upload_id: Some("upload-1".to_string()),
            etag: None,
            parts_total: 1,
            parts_pending: 1,
            parts_uploaded: 0,
            bytes_uploaded: 0,
            updated_at: Some(1),
            error: None,
        };

        let err = validate_resume_entry(&entry, "pipe")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not resumable through `streams resume`"));
    }

    #[test]
    fn test_validate_resume_db_rejects_missing_upload_id() {
        let dir = tempdir().unwrap();
        let source = dir.path().join("source.bin");
        fs::write(&source, b"hello").unwrap();

        let checksum = crate::cli::actions::object_put::blake3_checksum(&source, true).unwrap();
        let file_mtime = fs::metadata(&source)
            .unwrap()
            .modified()
            .unwrap_or_else(|_| SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let (db_key, state_dir) = {
            let db = Db::new(&test_s3("bucket"), "key", &checksum, file_mtime, dir.path()).unwrap();
            db.create_part(1, 0, 5, None).unwrap();
            db.db_parts().unwrap().flush().unwrap();
            db.flush().unwrap();
            (
                db.state_key().to_string(),
                Db::state_dir(dir.path(), &checksum),
            )
        };

        let metadata = StreamMetadata {
            version: 1,
            id: checksum.clone(),
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            source_path: source,
            checksum,
            file_size: 5,
            file_mtime,
            part_size: 5,
            db_key: db_key.clone(),
            created_at: 1,
            updated_at: Some(1),
            pipe: false,
            compress: false,
            encrypt: false,
            mode: StreamMode::FileMultipart,
        };

        let entry = StreamEntry {
            id: metadata.id.clone(),
            state_dir,
            metadata: Some(metadata.clone()),
            status: StreamStatus::Resumable,
            upload_id: None,
            etag: None,
            parts_total: 1,
            parts_pending: 1,
            parts_uploaded: 0,
            bytes_uploaded: 0,
            updated_at: Some(1),
            error: None,
        };

        let err = validate_resume_db(&entry, &metadata, &entry.id)
            .unwrap_err()
            .to_string();
        assert!(err.contains("has no saved upload id"));
    }

    #[test]
    fn test_resume_stream_rejects_unknown_id() {
        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let err = runtime
            .block_on(resume_stream(
                dir.path(),
                dir.path(),
                "missing",
                4,
                GlobalArgs::new(),
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Unknown stream state id: missing"));
    }

    #[test]
    fn test_handle_json_list_show_and_clean() {
        let dir = tempdir().unwrap();
        let streams_dir = dir.path().join("s3m");
        let broken_dir = crate::stream::state::state_dir(&streams_dir, "broken");
        fs::create_dir_all(&broken_dir).unwrap();
        fs::write(
            broken_dir.join(crate::stream::state::STATE_FILE),
            "not: [valid",
        )
        .unwrap();

        let config_path = dir.path().join("config.yml");
        fs::write(&config_path, "---\nhosts: {}\n").unwrap();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(handle(
                Action::Streams {
                    command: StreamCommand::List,
                    config_file: config_path.clone(),
                    json: true,
                    s3m_dir: streams_dir.clone(),
                    number: 1,
                },
                GlobalArgs::new(),
            ))
            .unwrap();
        runtime
            .block_on(handle(
                Action::Streams {
                    command: StreamCommand::Show {
                        id: "broken".to_string(),
                    },
                    config_file: config_path.clone(),
                    json: true,
                    s3m_dir: streams_dir.clone(),
                    number: 1,
                },
                GlobalArgs::new(),
            ))
            .unwrap();
        runtime
            .block_on(handle(
                Action::Streams {
                    command: StreamCommand::Clean,
                    config_file: config_path,
                    json: true,
                    s3m_dir: streams_dir.clone(),
                    number: 1,
                },
                GlobalArgs::new(),
            ))
            .unwrap();

        assert!(!broken_dir.exists());
    }
}
