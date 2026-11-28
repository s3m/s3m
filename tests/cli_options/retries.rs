use super::support::*;
use anyhow::{Context, Result};
use mockito::{Matcher, Server};
use std::{
    fs,
    io::Write as _,
    path::Path,
    process::Output,
    time::{Duration, UNIX_EPOCH},
};
use tempfile::tempdir;

#[test]
fn test_invalid_runtime_values_fail_before_any_request() -> Result<()> {
    let mut server = Server::new();
    let any_post = server.mock("POST", Matcher::Any).expect(0).create();
    let any_put = server.mock("PUT", Matcher::Any).expect(0).create();
    let head = head_mock(&mut server, "unused").expect(0).create();
    let home = tempdir()?;
    let config = write_file(
        &home.path().join("config.yml"),
        config_yaml(&server.url(), "").as_bytes(),
    )?;
    let source = write_file(&home.path().join("source.bin"), b"data")?;
    let missing_dir = home.path().join("missing-dir");
    let missing_dir = missing_dir.to_str().context("UTF-8 missing directory")?;

    let cases: [(Vec<&str>, &str); 6] = [
        (
            vec!["-t", missing_dir, &source, "s3/bucket/key"],
            "Invalid path or directory does not exist",
        ),
        (
            vec!["--retries", "4294967296", &source, "s3/bucket/key"],
            "'--retries <retries>'",
        ),
        // Root-only flags are not global: clap rejects them after a subcommand.
        (
            vec!["get", "s3/bucket/key", "--meta", "--tmp-dir", missing_dir],
            "unexpected argument '--tmp-dir'",
        ),
        (
            vec!["get", "s3/bucket/key", "--meta", "-r", "1"],
            "unexpected argument '-r'",
        ),
        (
            vec!["get", "s3/bucket/key", "--meta", "-b", "5242880"],
            "unexpected argument '-b'",
        ),
        (
            vec!["get", "s3/bucket/key", "--meta", "--config", &config],
            "unexpected argument '--config'",
        ),
    ];

    for (case, expected) in cases {
        let mut args = vec!["--config", config.as_str()];
        args.extend(case.iter().copied());
        let output = s3m(home.path(), &args)?;
        let err = stderr(&output);
        assert!(!output.status.success(), "{case:?} succeeded");
        assert!(err.contains(expected), "{case:?}: {err}");
    }

    any_post.assert();
    any_put.assert();
    head.assert();
    Ok(())
}

/// Assert exactly `attempts` requests for a failing part and no completion.
fn failing_multipart(retries: &str, attempts: usize) -> Result<Output> {
    let mut server = Server::new();
    let home = tempdir()?;
    let config = write_file(
        &home.path().join("config.yml"),
        config_yaml(&server.url(), "").as_bytes(),
    )?;
    let source = write_file(&home.path().join("big.bin"), &vec![7_u8; PART_SIZE + 1])?;
    let _initiate = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::Regex("uploads".to_string()))
        .with_body(INITIATE_XML)
        .create();
    let part_one = server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
        .with_status(500)
        .with_body("<Error><Code>InternalError</Code></Error>")
        .expect(attempts)
        .create();
    let complete = server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "UP1".into()))
        .with_body(COMPLETE_XML)
        .expect(0)
        .create();

    let output = s3m(
        home.path(),
        &[
            "-r",
            retries,
            "-n",
            "1",
            "-b",
            &PART_SIZE.to_string(),
            "--quiet",
            "--config",
            &config,
            &source,
            "s3/bucket/key",
        ],
    )?;

    part_one.assert();
    complete.assert();
    Ok(output)
}

#[test]
fn test_retries_bound_part_upload_attempts() -> Result<()> {
    // --retries counts attempts (default 3 = 3 attempts); 0 still makes one.
    for (retries, attempts) in [("0", 1), ("1", 1), ("2", 2)] {
        let output = failing_multipart(retries, attempts)?;
        let err = stderr(&output);
        assert!(!output.status.success(), "-r {retries} succeeded: {err}");
        assert!(
            err.contains("500 Internal Server Error"),
            "-r {retries}: {err}"
        );
    }
    Ok(())
}

const SINGLE_PART_BODY: &str = "single-part payload";

fn put_single_part(home: &Path, endpoint: &str, retries: &str) -> Result<Output> {
    let config = write_file(
        &home.join("config.yml"),
        config_yaml(endpoint, "").as_bytes(),
    )?;
    let source = home.join("single.bin");
    s3m(
        home,
        &[
            "--retries",
            retries,
            "--config",
            &config,
            source.to_str().context("UTF-8 source path")?,
            "s3/bucket/key",
        ],
    )
}

fn s3_error(code: &str, message: &str) -> String {
    format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>")
}

fn assert_single_part_retry(status: usize, error_body: &str) -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    fs::write(home.path().join("single.bin"), SINGLE_PART_BODY)?;
    let first = server
        .mock("PUT", "/bucket/key")
        .match_body(SINGLE_PART_BODY)
        .with_status(status)
        .with_header("etag", "\"failed\"")
        .with_body(error_body)
        .expect(1)
        .create();
    let second = server
        .mock("PUT", "/bucket/key")
        .match_body(SINGLE_PART_BODY)
        .with_status(200)
        .with_header("etag", "\"stored\"")
        .expect(1)
        .create();
    let other_put = server.mock("PUT", Matcher::Any).expect(0).create();

    let output = put_single_part(home.path(), &server.url(), "2")?;
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.lines().any(|line| line == "ETag: \"stored\""),
        "{stdout}"
    );
    assert!(!stdout.contains("\"failed\""), "{stdout}");
    first.assert();
    second.assert();
    other_put.assert();
    Ok(())
}

fn assert_no_single_part_retry(status: usize, code: &str, retries: &str) -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    fs::write(home.path().join("single.bin"), SINGLE_PART_BODY)?;
    let reject = server
        .mock("PUT", "/bucket/key")
        .with_status(status)
        .with_body(s3_error(code, "rejected"))
        .expect(1)
        .create();

    let output = put_single_part(home.path(), &server.url(), retries)?;
    let error = stderr(&output);
    assert!(!output.status.success(), "{code} unexpectedly succeeded");
    assert!(error.contains(code), "{error}");
    reject.assert();
    Ok(())
}

#[test]
fn test_single_part_503_respects_retry_budget() -> Result<()> {
    assert_no_single_part_retry(503, "ServiceUnavailable", "0")?;
    assert_single_part_retry(503, "temporarily unavailable")
}

#[test]
fn test_single_part_4xx_retries_only_throttling() -> Result<()> {
    assert_no_single_part_retry(403, "AccessDenied", "2")?;
    assert_no_single_part_retry(400, "BadDigest", "2")?;
    assert_no_single_part_retry(400, "InvalidRequest", "2")?;
    assert_single_part_retry(429, &s3_error("SlowDown", "throttled"))?;
    assert_single_part_retry(429, "throttled")?;
    assert_single_part_retry(400, &s3_error("SlowDown", "throttled"))
}

#[test]
fn test_single_part_does_not_retry_501() -> Result<()> {
    assert_no_single_part_retry(501, "NotImplemented", "2")
}

#[test]
fn test_single_part_retries_request_timeout_code() -> Result<()> {
    assert_single_part_retry(400, &s3_error("RequestTimeout", "request timed out"))
}

#[test]
fn test_single_part_retries_408() -> Result<()> {
    assert_single_part_retry(408, "request timed out")
}

#[test]
fn test_single_part_retries_operation_aborted() -> Result<()> {
    assert_single_part_retry(409, &s3_error("OperationAborted", "conflicting operation"))
}

#[test]
fn test_single_part_retries_r2_client_disconnect() -> Result<()> {
    assert_single_part_retry(400, &s3_error("ClientDisconnect", "client disconnected"))
}

#[test]
fn test_single_part_returns_the_last_503_after_three_attempts() -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    fs::write(home.path().join("single.bin"), SINGLE_PART_BODY)?;
    let responses: Vec<_> = ["first failure", "second failure", "last failure"]
        .into_iter()
        .map(|message| {
            server
                .mock("PUT", "/bucket/key")
                .match_body(SINGLE_PART_BODY)
                .with_status(503)
                .with_body(s3_error("ServiceUnavailable", message))
                .expect(1)
                .create()
        })
        .collect();

    let output = put_single_part(home.path(), &server.url(), "3")?;
    let error = stderr(&output);
    assert!(
        !output.status.success(),
        "three 503s unexpectedly succeeded"
    );
    assert!(error.contains("503 Service Unavailable"), "{error}");
    assert!(error.contains("last failure"), "{error}");
    for response in responses {
        response.assert();
    }
    Ok(())
}

#[test]
fn test_single_part_stops_when_source_changes_between_attempts() -> Result<()> {
    for (size_changes, expected_change) in
        [(true, "size changed"), (false, "modification time changed")]
    {
        let mut server = Server::new();
        let home = tempdir()?;
        let source = home.path().join("single.bin");
        fs::write(&source, SINGLE_PART_BODY)?;
        let changed_mtime = fs::metadata(&source)?.modified()? + Duration::from_secs(2);
        let reject = server
            .mock("PUT", "/bucket/key")
            .match_body(SINGLE_PART_BODY)
            .with_status(503)
            .with_body_from_request(move |_| {
                let change = if size_changes {
                    fs::write(&source, "shorter")
                } else {
                    fs::File::create(&source).and_then(|mut file| {
                        file.write_all(SINGLE_PART_BODY.to_ascii_uppercase().as_bytes())?;
                        file.set_modified(changed_mtime)
                    })
                };
                assert!(change.is_ok(), "failed to change the source: {change:?}");
                s3_error("ServiceUnavailable", "try again").into_bytes()
            })
            .expect(1)
            .create();
        let retry = server.mock("PUT", Matcher::Any).expect(0).create();

        let output = put_single_part(home.path(), &server.url(), "2")?;
        let error = stderr(&output);
        assert!(
            !output.status.success(),
            "changed source unexpectedly succeeded"
        );
        assert!(
            error.contains("the file changed during the upload"),
            "{error}"
        );
        assert!(error.contains(expected_change), "{error}");
        assert!(error.contains("Run the upload again."), "{error}");
        reject.assert();
        retry.assert();
    }
    Ok(())
}

#[test]
fn test_single_part_stops_before_sending_a_same_millisecond_rewrite() -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    let source = home.path().join("single.bin");
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut file = fs::File::create(&source)?;
    file.write_all(SINGLE_PART_BODY.as_bytes())?;
    file.set_modified(timestamp + Duration::from_micros(100))?;
    let original = file.metadata()?.modified()?;
    drop(file);
    let reject = server
        .mock("PUT", "/bucket/key")
        .match_body(SINGLE_PART_BODY)
        .with_status(503)
        .with_body_from_request(move |_| {
            let change = fs::File::create(&source).and_then(|mut file| {
                file.write_all(SINGLE_PART_BODY.to_ascii_uppercase().as_bytes())?;
                file.set_modified(timestamp + Duration::from_micros(900))?;
                assert_eq!(file.metadata()?.len(), SINGLE_PART_BODY.len() as u64);
                assert_eq!(
                    file.metadata()?
                        .modified()?
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .map(|d| d.as_millis()),
                    original
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .map(|d| d.as_millis()),
                );
                Ok(())
            });
            assert!(change.is_ok(), "could not rewrite the file: {change:?}");
            s3_error("ServiceUnavailable", "try again").into_bytes()
        })
        .expect(1)
        .create();
    let retry = server
        .mock("PUT", Matcher::Any)
        .with_header("etag", "\"wrong-content\"")
        .expect(0)
        .create();

    let output = put_single_part(home.path(), &server.url(), "2")?;
    reject.assert();
    retry.assert();
    let error = stderr(&output);
    assert!(
        !output.status.success(),
        "rewritten source unexpectedly succeeded"
    );
    assert!(
        error.contains("the file changed during the upload"),
        "{error}"
    );
    assert!(error.contains("BLAKE3 checksum changed"), "{error}");
    assert!(error.contains("Run the upload again."), "{error}");
    Ok(())
}
