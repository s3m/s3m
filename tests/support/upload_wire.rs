use super::*;
use std::{
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command,
    sync::{Barrier, OnceCell},
    task::JoinSet,
    time::{Duration, timeout},
};

// The real uploader queues its initial batch before polling any upload future.
// Its first "Uploading part" record therefore closes that admission batch before
// the corresponding HTTP request can arrive. Use this positive watermark to
// gate the actual batch, including excess admission; never gate on desired slots.
async fn multipart_peer(
    listener: TcpListener,
    payload: Arc<Vec<u8>>,
    transcript: PathBuf,
) -> Result<usize> {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(OnceCell::new());
    let part_size = (payload.len() - 137) / 3;
    let mut handlers = JoinSet::new();
    for _ in 0..6 {
        let (stream, _) = timeout(Duration::from_secs(20), listener.accept()).await??;
        let transcript = transcript.clone();
        let (payload, active, peak, barrier) = (
            Arc::clone(&payload),
            Arc::clone(&active),
            Arc::clone(&peak),
            Arc::clone(&barrier),
        );
        handlers.spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut first = String::new();
            reader.read_line(&mut first).await?;
            let target = first.split_whitespace().nth(1).context("HTTP target")?;
            let url = url::Url::parse(&format!("http://localhost{target}"))?;
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                if line == "\r\n" { break; }
                if let Some((name, value)) = line.split_once(':') && name.eq_ignore_ascii_case("content-length") { length = value.trim().parse()?; }
            }
            let part = url.query_pairs().find(|(key, _)| key == "partNumber").map(|(_, value)| value.parse::<usize>()).transpose()?;
            if part.is_some() { peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst); }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await?;
            let response = if let Some(number) = part {
                let offset = (number - 1) * part_size;
                assert_eq!(body.len(), part_size.min(payload.len() - offset), "part {number} body length");
                ensure!(payload.get(offset..offset + body.len()) == Some(body.as_slice()), "part {number} byte mismatch");
                let barrier = barrier.get_or_try_init(|| async {
                    let trace = fs::read_to_string(&transcript)?;
                    let mut admitted = 0;
                    for line in trace.lines() {
                        let message = line.split_once("] ").map_or("", |(_, message)| message);
                        if message.starts_with("Task push part: ") {
                            admitted += 1;
                        }
                        if message.starts_with("Uploading part: ") {
                            ensure!((1..=4).contains(&admitted), "invalid admission batch: {trace}");
                            println!("initial admission batch={admitted}; holding real HTTP responses");
                            return Ok::<_, anyhow::Error>(Barrier::new(admitted));
                        }
                    }
                    anyhow::bail!("request arrived without its positive upload watermark: {trace}");
                }).await?;
                timeout(Duration::from_secs(10), barrier.wait()).await?;
                println!("observed part={number} size={} bytes=verified", body.len());
                ""
            } else if url.query_pairs().any(|(key, _)| key == "uploads") { INIT } else { DONE };
            let mut stream = reader.into_inner();
            // Count unanswered requests until response commitment. Decrement
            // before writing, so a fast client cannot race a stale server count.
            if part.is_some() { active.fetch_sub(1, Ordering::SeqCst); }
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: wire-part\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await?;
            Ok::<(), anyhow::Error>(())
        });
    }
    while let Some(result) = handlers.join_next().await {
        result??;
    }
    Ok(peak.load(Ordering::SeqCst))
}

#[tokio::test]
async fn buffers_split_exact_bytes_and_number_bounds_inflight_requests() -> Result<()> {
    for mib in [5, 10, 20] {
        for slots in [1, 2] {
            let size = mib * MIB;
            let payload = Arc::new(
                (0..3 * size + 137)
                    .map(|i| u8::try_from(i % 251))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let fixture = Fixture::new(&format!("http://{}", listener.local_addr()?), &payload)?;
            let transcript = fixture.home.path().join("upload.log");
            let mut cmd = fixture.command();
            cmd.args([
                "-q",
                "-vv",
                "-r",
                "1",
                if slots == 1 { "--number" } else { "-n" },
                &slots.to_string(),
                if mib == 5 { "-b" } else { "--buffer" },
                &size.to_string(),
            ])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .env("RUST_LOG_STYLE", "never");
            let mut child = Command::from(cmd)
                .kill_on_drop(true)
                .stdout(Stdio::null())
                .stderr(fs::File::create(&transcript)?)
                .spawn()?;
            // This deadline only detects a hung child/peer. Admission is proved
            // by positive log/HTTP events and the gated peak, never elapsed time.
            let outcome = timeout(Duration::from_secs(60), async {
                tokio::try_join!(
                    multipart_peer(listener, payload, transcript.clone()),
                    async { child.wait().await.map_err(anyhow::Error::from) }
                )
            })
            .await
            .context("upload liveness watchdog expired")
            .and_then(|result| result);
            if outcome.is_err() && child.try_wait()?.is_none() {
                child.kill().await?;
            }
            let trace = fs::read_to_string(&transcript)?;
            let (peak, status) = outcome.with_context(|| trace.clone())?;
            ensure!(status.success(), "{trace}");
            assert_eq!(peak, slots, "--buffer {size} --number {slots}");
            println!(
                "buffer={size} observed parts=[{size},{size},{size},137] peak={peak} exit={status}"
            );
        }
    }
    Ok(())
}
