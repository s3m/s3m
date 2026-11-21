use super::support::*;
use anyhow::{Context, Result};
use mockito::{Matcher, Server};
use std::process::Output;
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
