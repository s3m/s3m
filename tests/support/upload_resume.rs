use super::*;
use std::sync::{Arc, atomic::Ordering};

#[test]
fn failed_part_and_changed_or_missing_resume_source_preserve_durable_state() -> Result<()> {
    use s3m::stream::state::state_file_path;
    let mut server = Server::new();
    let payload = vec![43; 5 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let init = initiate(&mut server).expect(1).create();
    let first = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
        .match_body(payload.get(..5 * MIB).context("first part")?.to_vec())
        .with_header("etag", "first")
        .expect(1)
        .create();
    let failed = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "2".into()))
        .match_body(vec![43; 137])
        .with_status(500)
        .with_body("<Error><Code>InternalError</Code></Error>")
        .expect(1)
        .create();
    let done = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "UP1".into()))
        .expect(0)
        .create();
    let output = fixture
        .command()
        .args(["-q", "-r", "1", "-n", "1", "-b", "5242880"])
        .arg(&fixture.source)
        .arg("s3/bucket/key")
        .output()?;
    ensure!(
        !output.status.success() && output.stdout.is_empty(),
        "failed part claimed success"
    );
    ensure!(
        String::from_utf8_lossy(&output.stderr).contains("500 Internal Server Error"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let base = fixture.home.base();
    let entries = fixture.home.entries()?;
    ensure!(entries.len() == 1, "expected one durable upload");
    let entry = entries.first().context("saved upload")?;
    assert_eq!(
        (
            entry.upload_id.as_deref(),
            entry.parts_uploaded,
            entry.parts_pending,
            entry.bytes_uploaded
        ),
        (Some("UP1"), 1, 1, 5 * u64::try_from(MIB)?)
    );
    let state_path = state_file_path(base, &entry.id);
    let state_before = fs::read(&state_path)?;
    fs::remove_file(&fixture.source)?;
    for (changed, diagnostic) in [(false, "Source file"), (true, "Source file changed")] {
        if changed {
            fs::write(&fixture.source, b"changed")?;
        }
        let output = fixture
            .command()
            .args(["streams", "resume", &entry.id])
            .output()?;
        ensure!(!output.status.success(), "resume accepted invalid source");
        ensure!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(&state_path)?,
            state_before,
            "failed resume rewrote metadata"
        );
        let durable = fixture.home.entries()?;
        let durable = durable.first().context("state survived")?;
        assert_eq!(
            (
                durable.parts_uploaded,
                durable.parts_pending,
                durable.upload_id.as_deref()
            ),
            (1, 1, Some("UP1"))
        );
        println!(
            "resume changed={changed} exit={} metadata unchanged uploaded=1 pending=1 uploadId=UP1",
            output.status
        );
    }
    init.assert();
    first.assert();
    failed.assert();
    done.assert();
    Ok(())
}

#[test]
fn repeated_process_interruptions_resume_same_upload_and_original_bytes() -> Result<()> {
    use std::{
        process::Stdio,
        sync::{Mutex, atomic::AtomicBool, mpsc},
    };
    let mut server = Server::new();
    let payload = vec![67; 5 * MIB + 137];
    let fixture = Fixture::new(&server.url(), &payload)?;
    let init = initiate(&mut server).expect(1).create();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Mutex::new(release_rx);
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_peer = Arc::clone(&timed_out);
    let held = part_mock(&mut server, 1, &vec![67; 5 * MIB])
        .with_body_from_request(move |_| {
            let entered = entered_tx.send(()).is_ok();
            let released = release_rx
                .lock()
                .is_ok_and(|rx| rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok());
            timed_out_peer.store(!entered || !released, Ordering::SeqCst);
            Vec::new()
        })
        .expect(2)
        .create();
    let mut id = None;
    for interruption in 1..=2 {
        let mut child = fixture
            .command()
            .args(["-q", "-r", "1", "-n", "1", "-b", "5242880"])
            .arg(&fixture.source)
            .arg("s3/bucket/key")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let entered = entered_rx.recv_timeout(std::time::Duration::from_secs(10));
        child.kill()?;
        let killed = child.wait_with_output()?;
        release_tx.send(())?;
        entered?;
        assert!(!killed.status.success());
        let entries = fixture.home.entries()?;
        let entry = entries.first().context("durable interrupted upload")?;
        if let Some(previous) = &id {
            assert_eq!(&entry.id, previous);
        }
        id = Some(entry.id.clone());
        assert_eq!(
            (
                entry.upload_id.as_deref(),
                entry.parts_pending,
                entry.parts_uploaded
            ),
            (Some("UP1"), 2, 0)
        );
        println!(
            "interruption={interruption} CLI killed after part arrival; uploadId=UP1 pending=2 uploaded=0 exit={}",
            killed.status
        );
    }
    held.assert();
    held.remove();
    ensure!(!timed_out.load(Ordering::SeqCst), "gate timed out");
    let parts: Vec<_> = payload
        .chunks(5 * MIB)
        .enumerate()
        .map(|(i, bytes)| part_mock(&mut server, i + 1, bytes).expect(1).create())
        .collect();
    let done = complete(&mut server).expect(1).create();
    let output = fixture
        .command()
        .args([
            "-n",
            "1",
            "streams",
            "resume",
            id.as_deref().context("saved id")?,
        ])
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    init.assert();
    for part in parts {
        part.assert();
    }
    done.assert();
    let entries = fixture.home.entries()?;
    let entry = entries.first().context("completed upload")?;
    assert_eq!(
        (entry.etag.as_deref(), entry.parts_pending),
        (Some("done"), 0)
    );
    println!(
        "resume after two kills: original bytes verified in both parts; same uploadId=UP1; durable ETag=done pending=0"
    );
    Ok(())
}
