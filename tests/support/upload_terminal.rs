use super::*;
use std::process::Command;

#[test]
fn terminal_progress_is_visible_and_both_quiet_forms_suppress_it() -> Result<()> {
    for flag in [None, Some("--quiet"), Some("-q")] {
        let mut server = Server::new();
        let payload = vec![19; 5 * MIB + 137];
        let fixture = Fixture::new(&server.url(), &payload)?;
        let init = initiate(&mut server).expect(1).create();
        let part = server
            .mock("PUT", "/bucket/key")
            .match_query(Matcher::Any)
            .with_header("etag", "part")
            .expect(2)
            .create();
        let done = complete(&mut server).expect(1).create();
        let command = format!(
            "{} -c {} -r 1 -n 1 -b 5242880 {} {} s3/bucket/key",
            env!("CARGO_BIN_EXE_s3m"),
            fixture.config.display(),
            flag.unwrap_or(""),
            fixture.source.display()
        );
        let output = Command::new("script")
            .env("HOME", fixture.home.path())
            .env("TERM", "xterm")
            .args(["-q", "-e", "-c", &command, "/dev/null"])
            .output()?;
        ensure!(
            output.status.success(),
            "PTY {:?}: {} {}",
            flag,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let terminal = String::from_utf8_lossy(&output.stdout);
        match flag {
            None => {
                ensure!(
                    terminal.contains("MiB") && terminal.contains("ETag:"),
                    "missing terminal progress: {terminal}"
                );
            }
            Some(_) => ensure!(
                output.stdout.is_empty() && output.stderr.is_empty(),
                "quiet terminal emitted: {terminal}"
            ),
        }
        init.assert();
        part.assert();
        done.assert();
        println!(
            "PTY script -q -e -c {command:?} /dev/null: quiet={flag:?} bytes={} exit={}",
            output.stdout.len(),
            output.status
        );
    }
    Ok(())
}
