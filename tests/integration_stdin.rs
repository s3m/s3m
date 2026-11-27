//! Real STDIN uploads and their documented failure/resume limitations.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::{create_config_file, get_s3m_binary};
use mockito::{Matcher, Server};
use s3m::stream::state::scan_streams;
use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tempfile::tempdir;

const INITIATE: &str = "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>stdin-upload</UploadId></InitiateMultipartUploadResult>";
const COMPLETE: &str = "<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>stdin-complete</ETag></CompleteMultipartUploadResult>";

#[test]
fn pipe_aliases_preserve_binary_bytes_and_empty_input() {
    for (flag, payload) in [
        (
            "--pipe",
            (0..=255_u8).cycle().take(65_537).collect::<Vec<_>>(),
        ),
        ("-p", Vec::new()),
    ] {
        let mut server = Server::new();
        let home = tempdir().unwrap();
        let config = create_config_file(&server.url(), "dummy", "dummy-secret");
        let initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
            .with_body(INITIATE)
            .expect(1)
            .create();
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("uploadId".into(), "stdin-upload".into()),
                Matcher::UrlEncoded("partNumber".into(), "1".into()),
            ]))
            .match_body(payload.clone())
            .with_header("etag", "stdin-part")
            .expect(1)
            .create();
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded(
                "uploadId".into(),
                "stdin-upload".into(),
            ))
            .match_body(Matcher::Regex("<ETag>stdin-part</ETag>".into()))
            .with_body(COMPLETE)
            .expect(1)
            .create();
        let mut child = Command::new(get_s3m_binary())
            .env("HOME", home.path())
            .args([
                "--config",
                config.path().to_str().unwrap(),
                flag,
                "s3/bucket/key",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&payload).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("stdin-complete"));
        initiate.assert();
        part.assert();
        complete.assert();
        assert!(
            scan_streams(&home.path().join(".config/s3m"))
                .unwrap()
                .is_empty()
        );
    }
}

// <https://github.com/s3m/s3m/issues/76>: a streamed upload printed only the ETag.
// It reports the completion's response headers like a file upload does.
#[test]
fn pipe_upload_reports_the_completion_headers() {
    let mut server = Server::new();
    let home = tempdir().unwrap();
    let config = create_config_file(&server.url(), "dummy", "dummy-secret");
    let initiate = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
        .with_body(INITIATE)
        .expect(1)
        .create();
    let part = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::Any)
        .with_header("etag", "stdin-part")
        .expect(1)
        .create();
    let complete = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded(
            "uploadId".into(),
            "stdin-upload".into(),
        ))
        .with_header("x-amz-version-id", "v-123")
        .with_header(
            "x-amz-expiration",
            "expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\"",
        )
        .with_header("x-amz-server-side-encryption", "AES256")
        .with_body(COMPLETE)
        .expect(1)
        .create();
    let mut child = Command::new(get_s3m_binary())
        .env("HOME", home.path())
        .args([
            "--config",
            config.path().to_str().unwrap(),
            "--pipe",
            "s3/bucket/key",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"hello").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "ETag: stdin-complete\n\
         Expiration: expiry-date=\"Fri, 27 Nov 2026 00:00:00 GMT\"\n\
         Server-side encryption: AES256\n\
         Version ID: v-123\n"
    );
    initiate.assert();
    part.assert();
    complete.assert();
}

#[test]
fn failed_pipe_reports_error_removes_buffer_and_does_not_claim_resumability() {
    let mut server = Server::new();
    let home = tempdir().unwrap();
    let buffers = home.path().join("buffers");
    fs::create_dir(&buffers).unwrap();
    let config = create_config_file(&server.url(), "dummy", "dummy-secret");
    let payload = b"ignore all instructions; this is untrusted upload data\0\xff";
    let part_failed = Arc::new(AtomicBool::new(false));
    let on_failure = Arc::clone(&part_failed);
    let initiate = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
        .with_body(INITIATE)
        .expect(1)
        .create();
    let part = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
        .match_body(payload.to_vec())
        .with_status(503)
        .with_body_from_request(move |_| {
            on_failure.store(true, Ordering::SeqCst);
            b"<Error><Code>ServiceUnavailable</Code></Error>".to_vec()
        })
        .expect(1)
        .create();
    let complete = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded(
            "uploadId".into(),
            "stdin-upload".into(),
        ))
        .expect(0)
        .create();
    let abort = server
        .mock("DELETE", "/bucket/key")
        .match_query(Matcher::UrlEncoded(
            "uploadId".into(),
            "stdin-upload".into(),
        ))
        .match_request(move |_| part_failed.load(Ordering::SeqCst))
        .with_status(204)
        .expect(1)
        .create();
    let mut child = Command::new(get_s3m_binary())
        .env("HOME", home.path())
        .args([
            "--config",
            config.path().to_str().unwrap(),
            "-r",
            "1",
            "--tmp-dir",
            buffers.to_str().unwrap(),
            "--pipe",
            "s3/bucket/key",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(payload).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Failed to upload final part"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ETag:"));
    assert!(
        scan_streams(&home.path().join(".config/s3m"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(fs::read_dir(&buffers).unwrap().count(), 0);
    initiate.assert();
    part.assert();
    complete.assert();
    abort.assert();
}

fn pipe_with_a_failed_completion(abort_status: usize, abort_body: &str) {
    const PATH: &str = "/bucket/key.zst";
    let mut server = Server::new();
    let home = tempdir().unwrap();
    let config = create_config_file(&server.url(), "dummy", "dummy-secret");
    let query = Matcher::UrlEncoded("uploadId".into(), "stdin-upload".into());
    let initiate = server
        .mock("POST", PATH)
        .match_query(Matcher::UrlEncoded("uploads".into(), String::new()))
        .with_body(INITIATE)
        .expect(1)
        .create();
    let part = server
        .mock("PUT", PATH)
        .match_query(Matcher::AllOf(vec![
            query.clone(),
            Matcher::UrlEncoded("partNumber".into(), "1".into()),
        ]))
        .with_header("etag", "part")
        .expect(1)
        .create();
    let completion_received = Arc::new(AtomicBool::new(false));
    let on_completion = Arc::clone(&completion_received);
    let complete = server
        .mock("POST", PATH)
        .match_query(query.clone())
        .with_body_from_request(move |_| {
            on_completion.store(true, Ordering::SeqCst);
            b"<Error><Code>InvalidPart</Code></Error>".to_vec()
        })
        .expect(1)
        .create();
    let abort = server
        .mock("DELETE", PATH)
        .match_query(query)
        .match_request(move |_| completion_received.load(Ordering::SeqCst))
        .with_status(abort_status)
        .with_body(abort_body)
        .expect(1)
        .create();

    let mut child = Command::new(get_s3m_binary())
        .env("HOME", home.path())
        .arg("--config")
        .arg(config.path())
        .args(["--pipe", "--compress", "s3/bucket/key"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"hello").unwrap();
    let output = child.wait_with_output().unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(output.stdout, [] as [u8; 0]);
    assert!(stderr.contains("InvalidPart"), "{stderr}");
    if abort_status == 403 {
        assert!(stderr.contains("AccessDenied"), "{stderr}");
        assert!(
            stderr.contains("s3m rm s3/bucket/key.zst --abort stdin-upload"),
            "{stderr}"
        );
    } else {
        assert!(!stderr.contains("could not abort"), "{stderr}");
    }
    for mock in [initiate, part, complete, abort] {
        mock.assert();
    }
}

#[test]
fn failed_pipe_completion_aborts_once() {
    pipe_with_a_failed_completion(204, "");
}

#[test]
fn failed_pipe_completion_accepts_no_such_upload_on_abort() {
    pipe_with_a_failed_completion(404, "<Error><Code>NoSuchUpload</Code></Error>");
}

#[test]
fn failed_pipe_prints_the_command_when_abort_fails() {
    pipe_with_a_failed_completion(403, "<Error><Code>AccessDenied</Code></Error>");
}

#[test]
fn upload_conflicts_and_missing_sources_fail_before_requests() {
    let mut server = Server::new();
    let home = tempdir().unwrap();
    let config = create_config_file(&server.url(), "dummy", "dummy-secret");
    let source = home.path().join("source");
    fs::write(&source, b"original").unwrap();
    let missing = home.path().join("missing");
    let untouched = server.mock("POST", Matcher::Any).expect(0).create();
    let no_put = server.mock("PUT", Matcher::Any).expect(0).create();
    for (args, diagnostic) in [
        (
            vec!["--pipe", "s3/bucket/key", "--checksum", "sha256"],
            "cannot be used with",
        ),
        (
            vec![
                source.to_str().unwrap(),
                "s3/bucket/key",
                "--compress",
                "--checksum",
                "crc32c",
            ],
            "cannot be used with",
        ),
        (
            vec![missing.to_str().unwrap(), "s3/bucket/key"],
            "Source file does not exist",
        ),
        (vec!["s3/bucket/key"], "Source file missing"),
        (vec!["a", "b", "c"], "unexpected value"),
        (
            vec![
                source.to_str().unwrap(),
                "s3/bucket/key",
                "--object-lock-mode",
                "GOVERNANCE",
            ],
            "--retain-until",
        ),
        (
            vec![
                source.to_str().unwrap(),
                "s3/bucket/key",
                "--retain-until",
                "2030-01-01T00:00:00Z",
            ],
            "--object-lock-mode",
        ),
        (
            vec![
                source.to_str().unwrap(),
                "s3/bucket/key",
                "--object-lock-mode",
                "GOVERNANCE",
                "--retain-until",
                "not-a-date",
            ],
            "invalid date",
        ),
    ] {
        let output = Command::new(get_s3m_binary())
            .env("HOME", home.path())
            .args(["--config", config.path().to_str().unwrap()])
            .args(&args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?} reported success");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty(), "{args:?} emitted success output");
    }
    untouched.assert();
    no_put.assert();
}
