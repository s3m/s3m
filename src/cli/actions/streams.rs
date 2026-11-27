use crate::{
    cli::{
        Config,
        actions::{Action, StreamCommand},
        globals::GlobalArgs,
        start::get_host,
        time,
    },
    s3::S3,
    stream::{
        db::Db,
        state::{
            CleanSummary, StreamEntry, StreamMetadata, StreamStatus, clean_streams, scan_streams,
        },
        upload_multipart::{MultipartUploadRequest, upload_multipart},
    },
};
use anyhow::{Context, Result, anyhow};
use bytesize::ByteSize;
use colored::Colorize;
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
            StreamCommand::Resume {
                id,
                no_sign_request,
            } => {
                resume_stream(
                    &config_file,
                    &s3m_dir,
                    &id,
                    number,
                    no_sign_request,
                    globals,
                )
                .await?;
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

    let mut line = format!(
        "{:<64} {:<12} {} {:>10} {:>7} {:<24} {:<28} {}",
        entry.id,
        status,
        updated,
        bytes_uploaded,
        format!("{}/{}", entry.parts_uploaded, entry.parts_total),
        upload_id,
        destination,
        source,
    );
    if entry.status == StreamStatus::Broken
        && let Some(reason) = &entry.error
    {
        line.push_str("  ");
        line.push_str(reason);
    }
    line
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
        |ts| time::from_unix_seconds(ts).map_or_else(|| ts.to_string(), time::display),
    )
}

fn format_timestamp_rfc3339(timestamp: Option<u64>) -> String {
    timestamp.map_or_else(
        || "-".to_string(),
        |ts| time::from_unix_seconds(ts).map_or_else(|| ts.to_string(), time::rfc3339),
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

#[cfg(test)]
#[path = "streams_concurrency_tests.rs"]
mod concurrency_tests;

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

    if db.db_parts()?.is_empty() && db.db_uploaded()?.is_empty() {
        return Err(anyhow!(
            "Stream state {id} has no multipart parts to resume; run the upload again \
             (s3m <file> <host>/<bucket>/<key>), which plans them"
        ));
    }

    Ok(db)
}

async fn resume_stream(
    config_file: &Path,
    streams_dir: &Path,
    id: &str,
    number: u8,
    no_sign_request: bool,
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
    let credentials = host.credentials(&location.host, no_sign_request)?;
    let s3 = S3::new(
        &credentials,
        &region,
        Some(stream_metadata.bucket.clone()),
        no_sign_request,
    );

    let db = validate_resume_db(&entry, &stream_metadata, id)?;

    let file_size = file_metadata.len();
    let max_requests = calculate_max_requests(number);

    let etag = upload_multipart(MultipartUploadRequest {
        s3: &s3,
        key: &stream_metadata.key,
        file: &stream_metadata.source_path,
        file_size,
        file_mtime: Some(stream_metadata.file_mtime),
        chunk_size: stream_metadata.part_size,
        sdb: &db,
        acl: None,
        meta: None,
        quiet: false,
        additional_checksum: None,
        max_requests,
        globals,
    })
    .await?;

    println!("{etag}");

    Ok(())
}

/// # Errors
/// Will return `Err` if stream state cleanup fails
pub fn clean_streams_state(streams_dir: &Path) -> Result<CleanSummary> {
    clean_streams(streams_dir)
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

    fn resumable_state(base: &Path, legacy: bool) -> (Db, String) {
        let db = if legacy {
            let path = Db::state_dir(base, "good");
            fs::create_dir_all(&path).unwrap();
            Db::open_existing(&path, "legacy key 1".to_owned())
        } else {
            Db::new(&test_s3("bucket"), "key", "good", 1, base)
        }
        .unwrap();
        let id = db.path().file_name().unwrap().to_str().unwrap().to_owned();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 1, None).unwrap();
        db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            base,
            &StreamMetadata {
                version: 1,
                id: id.clone(),
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

        (db, id)
    }

    #[cfg(unix)]
    fn same_size_resume_fixture(base: &Path, endpoint: &str) -> (PathBuf, String) {
        let source = base.join("source.bin");
        let bytes = b"original";
        fs::write(&source, bytes).unwrap();
        let old_time = UNIX_EPOCH + std::time::Duration::from_secs(10);
        fs::OpenOptions::new()
            .write(true)
            .open(&source)
            .unwrap()
            .set_modified(old_time)
            .unwrap();
        let file_mtime = old_time.duration_since(UNIX_EPOCH).unwrap().as_millis();
        let file_size = u64::try_from(bytes.len()).unwrap();
        let checksum = blake3::hash(bytes).to_hex().to_string();
        let s3 = S3::new(
            &Credentials::new("fixture", &SecretString::new("local-only".into())),
            &Region::custom("us-east-1", endpoint),
            Some("bucket".into()),
            false,
        );
        let db = Db::new(&s3, "key", &checksum, file_mtime, base).unwrap();
        db.save_upload_id("upload-1").unwrap();
        db.create_parts([(1, 0, file_size)], None).unwrap();
        let id = db.path().file_name().unwrap().to_str().unwrap().to_owned();
        write_metadata(
            base,
            &StreamMetadata {
                version: 1,
                id: id.clone(),
                host: "s3".into(),
                bucket: "bucket".into(),
                key: "key".into(),
                source_path: source.clone(),
                checksum,
                file_size,
                file_mtime,
                part_size: file_size,
                db_key: db.state_key().to_owned(),
                created_at: 1,
                updated_at: Some(1),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();
        (source, id)
    }

    #[cfg(unix)]
    async fn rewrite_source_during_config_read(config: &Path, source: &Path, endpoint: &str) {
        use std::{process::Stdio, time::Duration};
        use tokio::{
            io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
            process::Command,
            time::timeout,
        };

        // Opening the FIFO for writing unblocks only once resume has validated
        // the original source and reached the read of the host configuration.
        let mut writer = Command::new("sh")
            .args([
                "-c",
                r#"exec 3>"$1"; printf '%s\n' ready; exec cat >&3"#,
                "sh",
            ])
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(writer.stdout.take().unwrap()).lines();
        let ready = timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ready.as_deref(),
            Some("ready"),
            "resume did not open the config FIFO"
        );
        let mut replacement = tempfile::NamedTempFile::new_in(source.parent().unwrap()).unwrap();
        std::io::Write::write_all(&mut replacement, b"modified").unwrap();
        replacement
            .as_file()
            .set_modified(UNIX_EPOCH + Duration::from_secs(20))
            .unwrap();
        replacement.persist(source).unwrap();
        let mut input = writer.stdin.take().unwrap();
        input.write_all(
            format!("hosts:\n  s3:\n    endpoint: {endpoint}\n    access_key: fixture\n    secret_key: local-only\n").as_bytes(),
        ).await.unwrap();
        drop(input);
        assert!(writer.wait().await.unwrap().success());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streams_resume_rejects_a_same_size_rewrite_after_validation() {
        use mockito::{Matcher, Server};
        use std::process::Command;

        let mut server = Server::new_async().await;
        let endpoint = server.url();
        let base = tempdir().unwrap();
        let (source, id) = same_size_resume_fixture(base.path(), &endpoint);
        let config = base.path().join("config.yml");
        assert!(
            Command::new("mkfifo")
                .arg(&config)
                .status()
                .unwrap()
                .success()
        );
        let abort = server
            .mock("DELETE", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "upload-1".into()))
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_status(500)
            .expect(0)
            .create_async()
            .await;

        let resume_config = config.clone();
        let resume_base = base.path().to_owned();
        let resume = tokio::spawn(async move {
            resume_stream(
                &resume_config,
                &resume_base,
                &id,
                1,
                false,
                GlobalArgs::new(),
            )
            .await
        });
        rewrite_source_during_config_read(&config, &source, &endpoint).await;
        let error = resume.await.unwrap().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("modification time changed since it was read"),
            "streams resume must reject a same-size rewrite after validation: {error:#}"
        );
        abort.assert_async().await;
        part.assert_async().await;
    }

    #[test]
    fn resume_reads_parts_after_a_locked_listing() {
        for uploaded in [false, true] {
            let dir = tempdir().unwrap();
            let (db, id) = resumable_state(dir.path(), false);
            if uploaded {
                let key = 1_u16.to_be_bytes();
                let part = db.db_parts().unwrap().remove(key).unwrap().unwrap();
                db.db_uploaded().unwrap().insert(key, part).unwrap();
            }
            let entry = find_entry(dir.path(), &id).unwrap();
            assert_eq!(entry.status, StreamStatus::Active);
            assert_eq!(entry.parts_total, 0);
            drop(db);
            let metadata = validate_resume_entry(&entry, &id).unwrap();
            validate_resume_db(&entry, &metadata, &id).unwrap();
        }
    }

    #[test]
    fn test_clean_streams_state_keeps_resumable_entries() {
        for legacy in [true, false] {
            let dir = tempdir().unwrap();
            let (_db, id) = resumable_state(dir.path(), legacy);
            let summary = clean_streams_state(dir.path()).unwrap();
            assert_eq!(summary.removed, Vec::<String>::new());
            assert_eq!(summary.kept, [id]);
        }
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
            (db.state_key().to_string(), db.path().to_owned())
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
        assert!(err.contains("has no saved upload id"), "{err}");
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
                false,
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
