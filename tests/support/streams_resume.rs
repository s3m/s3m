use super::*;
use anyhow::{Context, Result, ensure};
use std::process::Stdio;
use tokio::{net::TcpListener, process::Command, time::timeout};

#[path = "resume_peer.rs"]
mod resume_peer;

/// Saved state for `home/source-parts.bin` ("hello world") split into three
/// pending parts of 5, 5 and 1 bytes for upload `upload-1`.
fn create_three_part_state(home: &Path, id: &str) {
    let s3m_dir = s3m_dir(home);
    let source = home.join("source-parts.bin");
    fs::write(&source, b"hello world").unwrap();
    let file_mtime = fs::metadata(&source)
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let db = Db::new(&test_s3(), "key", id, file_mtime, &s3m_dir).unwrap();
    db.save_upload_id("upload-1").unwrap();
    for (number, seek, chunk) in PartIterator::new(11, 5) {
        db.create_part(number, seek, chunk, None).unwrap();
    }
    db.flush().unwrap();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    write_metadata(
        &s3m_dir,
        &StreamMetadata {
            version: 1,
            id: id.to_string(),
            host: "s3".to_string(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            checksum: calculate_file_hash(&source),
            source_path: source,
            file_size: 11,
            file_mtime,
            part_size: 5,
            db_key: db.state_key().to_string(),
            created_at,
            updated_at: Some(created_at),
            pipe: false,
            compress: false,
            encrypt: false,
            mode: StreamMode::FileMultipart,
        },
    )
    .unwrap();
    drop(db);
    wait_for_state_lock(home, id);
}

#[tokio::test]
async fn test_streams_resume_preserves_parts_and_bounds_inflight_requests() -> Result<()> {
    for slots in [1, 2] {
        let home = tempdir()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let config_dir = s3m_dir(home.path());
        fs::create_dir_all(&config_dir)?;
        fs::write(
            config_dir.join("config.yml"),
            format!(
                "---\nhosts:\n  s3:\n    endpoint: {url}\n    access_key: XXX\n    secret_key: YYY\n"
            ),
        )?;
        create_three_part_state(home.path(), "parts-id");
        let transcript = home.path().join("resume.log");
        let mut child = Command::new(get_s3m_binary())
            .env("HOME", home.path())
            .env("RUST_LOG_STYLE", "never")
            .args([
                "-vv",
                "-n",
                &slots.to_string(),
                "streams",
                "resume",
                "parts-id",
            ])
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&transcript)?)
            .spawn()?;
        let stdout = child.stdout.take().context("piped resume stdout")?;
        // Deadlines fail on a hung peer or child. Only positive admission and
        // HTTP events establish concurrency, never an absence interval.
        let outcome = timeout(Duration::from_secs(60), async {
            tokio::try_join!(
                resume_peer::observe(listener, transcript.clone()),
                async { child.wait().await.map_err(anyhow::Error::from) },
                async {
                    use tokio::io::AsyncReadExt;
                    let mut stdout = stdout;
                    let mut bytes = Vec::new();
                    stdout.read_to_end(&mut bytes).await?;
                    Ok::<_, anyhow::Error>(bytes)
                }
            )
        })
        .await
        .context("resume liveness watchdog expired")
        .and_then(|result| result);
        if outcome.is_err() && child.try_wait()?.is_none() {
            child.kill().await?;
        }
        let trace = fs::read_to_string(&transcript)?;
        let (probe, status, stdout) = outcome.with_context(|| trace.clone())?;
        ensure!(status.success(), "-n {slots}: {trace}");
        assert_eq!(stdout, b"ETag: \"final-etag\"\n");
        assert_eq!(probe.completions, 1);
        assert_eq!(
            probe.parts,
            std::collections::BTreeMap::from([
                (1, b"hello".to_vec()),
                (2, b" worl".to_vec()),
                (3, b"d".to_vec())
            ])
        );
        let shown = show_json(home.path(), "parts-id");
        assert_eq!(shown["status"], "complete");
        assert_eq!(shown["etag"], "\"final-etag\"");
        println!(
            "resume number={slots} peak={} parts=[5,5,1] bytes=verified completions=1 state=complete exit={status}",
            probe.peak
        );
        assert_eq!(probe.peak, slots, "resume --number {slots}");
    }
    Ok(())
}
