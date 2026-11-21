use super::support::*;
use anyhow::{Context, Result};
use mockito::Server;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_kilobytes_upload_reports_selected_rate_and_preserves_bytes() -> Result<()> {
    let payload: Vec<u8> = (0..300 * 1024_u32)
        .map(|i| u8::try_from(i % 251))
        .collect::<std::result::Result<_, _>>()?;
    // This binary test observes rate selection, not pacing. Request-stream
    // scheduling cannot be observed through logs or delivered bytes.
    // (throttle flag, value, expected throttle log line from the request path)
    let cases = [
        ("-k", "0", None),
        (
            "--kilobytes",
            "102400",
            Some("Throttling enabled: 102400 KB/s (duration per chunk: 0.001s)"),
        ),
        (
            "-k",
            "18014398509481984",
            Some("Throttling enabled: 18014398509481984 KB/s (duration per chunk: 0.000s)"),
        ),
    ];

    for (flag, value, expected) in cases {
        let mut server = Server::new();
        let home = tempdir()?;
        let config = write_file(
            &home.path().join("config.yml"),
            config_yaml(&server.url(), "").as_bytes(),
        )?;
        let source = write_file(&home.path().join("source.bin"), &payload)?;
        let put = server
            .mock("PUT", "/bucket/key")
            .match_body(payload.clone())
            .with_header("etag", "\"obj\"")
            .expect(1)
            .create();

        let output = s3m(
            home.path(),
            &[
                "-v",
                flag,
                value,
                "--config",
                &config,
                &source,
                "s3/bucket/key",
            ],
        )?;
        let err = stderr(&output);

        assert!(output.status.success(), "{flag} {value}: {err}");
        put.assert();
        match expected {
            Some(line) => assert!(err.contains(line), "{flag} {value}: {err}"),
            None => assert!(!err.contains("Throttling enabled"), "{err}"),
        }
    }
    Ok(())
}

#[test]
fn test_kilobytes_download_reports_selected_rate_and_preserves_bytes() -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    let config = write_file(
        &home.path().join("config.yml"),
        config_yaml(&server.url(), "").as_bytes(),
    )?;
    let body: Vec<u8> = (0..64 * 1024_u32)
        .map(|i| u8::try_from(i % 241))
        .collect::<std::result::Result<_, _>>()?;
    let _get = server
        .mock("GET", "/bucket/key")
        .with_body(&body)
        .expect(1)
        .create();
    let dest = home.path().join("downloaded.bin");

    let output = s3m(
        home.path(),
        &[
            "-v",
            "--config",
            &config,
            "get",
            "s3/bucket/key",
            dest.to_str().context("UTF-8 destination")?,
            "-k",
            "2048",
        ],
    )?;

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stderr(&output).contains("throttle bandwidth: 2048KB/s"));
    assert_eq!(fs::read(&dest)?, body);
    Ok(())
}
