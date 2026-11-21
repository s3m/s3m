use super::support::*;
use anyhow::{Context, Result, anyhow};
use mockito::{Matcher, Server};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, OnceLock},
};
use tempfile::tempdir;

fn count_buffers(dir: &Path) -> std::io::Result<usize> {
    fs::read_dir(dir)?.try_fold(0, |count, entry| {
        let name = entry?.file_name();
        Ok(count + usize::from(name.to_string_lossy().ends_with(".s3m")))
    })
}

#[test]
fn test_tmp_dir_holds_stdin_buffer_during_upload() -> Result<()> {
    let payload = b"piped payload for the tmp-dir buffer\n".repeat(100);

    for flag in ["--tmp-dir", "-t"] {
        let mut server = Server::new();
        let home = tempdir()?;
        let chosen = tempdir()?;
        let default_tmp = tempdir()?;
        let config = write_file(
            &home.path().join("config.yml"),
            config_yaml(&server.url(), "").as_bytes(),
        )?;
        let seen = Arc::new(OnceLock::new());
        let (seen_by_mock, chosen_dir, default_dir) = (
            Arc::clone(&seen),
            chosen.path().to_path_buf(),
            default_tmp.path().to_path_buf(),
        );

        let _initiate = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::Regex("uploads".to_string()))
            .with_body(INITIATE_XML)
            .create();
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::UrlEncoded("partNumber".into(), "1".into()))
            .match_body(payload.clone())
            .with_header("etag", "\"p1\"")
            .with_body_from_request(move |_| {
                seen_by_mock.get_or_init(|| {
                    count_buffers(&chosen_dir).and_then(|chosen| {
                        count_buffers(&default_dir).map(|default| (chosen, default))
                    })
                });
                Vec::new()
            })
            .expect(1)
            .create();
        let complete = server
            .mock("POST", "/bucket/key")
            .match_query(Matcher::UrlEncoded("uploadId".into(), "UP1".into()))
            .with_body(COMPLETE_XML)
            .expect(1)
            .create();

        let mut child = Command::new(env!("CARGO_BIN_EXE_s3m"))
            .env("HOME", home.path())
            .env("TMPDIR", default_tmp.path())
            .args([
                flag,
                chosen.path().to_str().context("UTF-8 buffer path")?,
                "--config",
                &config,
            ])
            .args(["--pipe", "--quiet", "s3/bucket/key"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .context("piped child stdin")?
            .write_all(&payload)?;
        let output = child.wait_with_output()?;

        assert!(output.status.success(), "{flag}: {}", stderr(&output));
        part.assert();
        complete.assert();
        // One buffer file in the chosen dir and none in the default TMPDIR
        // while the part request was in flight.
        let observed = seen
            .get()
            .context("part request did not observe buffers")?
            .as_ref()
            .map_err(|error| anyhow!("{error}"))?;
        assert_eq!(*observed, (1, 0), "{flag}");
    }
    Ok(())
}
