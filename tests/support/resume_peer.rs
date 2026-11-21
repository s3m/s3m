use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Barrier, OnceCell},
    task::JoinSet,
    time::{Duration, timeout},
};

#[derive(Default)]
pub(super) struct UploadProbe {
    pub(super) peak: usize,
    pub(super) parts: BTreeMap<u16, Vec<u8>>,
    pub(super) completions: usize,
}

pub(super) async fn observe(listener: TcpListener, transcript: PathBuf) -> Result<UploadProbe> {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(OnceCell::new());
    let mut handlers = JoinSet::new();
    for _ in 0..4 {
        let (stream, _) = timeout(Duration::from_secs(20), listener.accept()).await??;
        let transcript = transcript.clone();
        let (active, peak, arrivals, gate) = (
            Arc::clone(&active),
            Arc::clone(&peak),
            Arc::clone(&arrivals),
            Arc::clone(&gate),
        );
        handlers.spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut first = String::new();
            reader.read_line(&mut first).await?;
            let mut words = first.split_whitespace();
            let method = words.next().context("HTTP method")?;
            let target = words.next().context("HTTP target")?;
            let url = url::Url::parse(&format!("http://localhost{target}"))?;
            ensure!(url.path() == "/bucket/key", "unexpected resume target: {url}");
            ensure!(url.query_pairs().any(|(key, value)| key == "uploadId" && value == "upload-1"), "missing saved upload ID");
            let mut length = 0;
            loop {
                let mut line = String::new();
                ensure!(reader.read_line(&mut line).await? != 0, "truncated HTTP headers");
                if line == "\r\n" { break; }
                if let Some((name, value)) = line.split_once(':') && name.eq_ignore_ascii_case("content-length") { length = value.trim().parse()?; }
            }
            let part = url.query_pairs().find(|(key, _)| key == "partNumber").map(|(_, value)| value.parse::<u16>()).transpose()?;
            if part.is_some() { peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst); }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await?;
            let (response, etag) = if let Some(number) = part {
                ensure!(method == "PUT", "part method: {method}");
                // The enqueue loop closes its initial batch before polling the
                // first upload future. Its debug record is flushed before HTTP.
                // Gate that observed batch, including excess admission; neither
                // requested slots nor the reported limit sizes this barrier.
                let (admitted, barrier) = gate.get_or_try_init(|| async {
                    let trace = fs::read_to_string(&transcript)?;
                    let mut admitted = 0;
                    for line in trace.lines() {
                        let message = line.split_once("] ").map_or("", |(_, message)| message);
                        if message.starts_with("Task push part: ") { admitted += 1; }
                        if message.starts_with("Uploading part: ") {
                            ensure!((1..=3).contains(&admitted), "invalid resume admission batch: {trace}");
                            println!("resume initial admission batch={admitted}; holding real HTTP responses");
                            return Ok::<_, anyhow::Error>((admitted, Barrier::new(admitted)));
                        }
                    }
                    anyhow::bail!("resume request without positive upload watermark: {trace}");
                }).await?;
                // Three parts leave a tail at two slots. Only the closed first
                // batch waits; a cyclic barrier would strand that final part.
                if arrivals.fetch_add(1, Ordering::SeqCst) < *admitted {
                    timeout(Duration::from_secs(10), barrier.wait()).await?;
                }
                println!("resume observed part={number} bytes={}", body.len());
                (String::new(), format!("\"etag-{number}\""))
            } else {
                ensure!(method == "POST", "completion method: {method}");
                (r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Location>here</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>"final-etag"</ETag></CompleteMultipartUploadResult>"#.to_string(), String::new())
            };
            let mut stream = reader.into_inner();
            // Count unanswered HTTP parts, ending at response commitment so a
            // fast client cannot race a stale server-side active count.
            if part.is_some() { active.fetch_sub(1, Ordering::SeqCst); }
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: {etag}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await?;
            Ok::<_, anyhow::Error>((part, body))
        });
    }
    let mut probe = UploadProbe::default();
    while let Some(result) = handlers.join_next().await {
        let (part, body) = result??;
        if let Some(number) = part {
            ensure!(
                probe.parts.insert(number, body).is_none(),
                "duplicate resume part {number}"
            );
        } else {
            probe.completions += 1;
        }
    }
    probe.peak = peak.load(Ordering::SeqCst);
    Ok(probe)
}
