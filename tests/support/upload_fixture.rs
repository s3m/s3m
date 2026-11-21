use super::*;
use mockito::{Mock, ServerGuard};
use std::{path::PathBuf, process::Command};
#[path = "stream_home.rs"]
mod stream_home;
use stream_home::StreamHome;

pub(super) struct Fixture {
    pub(super) home: StreamHome,
    pub(super) source: PathBuf,
    pub(super) config: PathBuf,
}
impl Fixture {
    pub(super) fn new(endpoint: &str, bytes: &[u8]) -> Result<Self> {
        let home = StreamHome::new(bytes)?;
        let source = home.path().join("source.bin");
        let config = home.path().join("config.yml");
        fs::write(&source, bytes)?;
        fs::write(
            &config,
            format!(
                "hosts:\n  s3:\n    endpoint: {endpoint}\n    access_key: protocol\n    secret_key: local-only\n"
            ),
        )?;
        Ok(Self {
            home,
            source,
            config,
        })
    }
    pub(super) fn command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3m"));
        cmd.env("HOME", self.home.path())
            .arg("-c")
            .arg(&self.config);
        cmd
    }
}
pub(super) fn initiate(server: &mut ServerGuard) -> Mock {
    server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::Regex("uploads".into()))
        .with_body(INIT)
}

pub(super) fn complete(server: &mut ServerGuard) -> Mock {
    server
        .mock("POST", "/bucket/key")
        .match_query(Matcher::UrlEncoded("uploadId".into(), "UP1".into()))
        .with_body(DONE)
}

pub(super) fn part_mock(server: &mut ServerGuard, number: usize, body: &[u8]) -> Mock {
    server
        .mock("PUT", "/bucket/key")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("partNumber".into(), number.to_string()),
            Matcher::UrlEncoded("uploadId".into(), "UP1".into()),
        ]))
        .match_body(body.to_vec())
        .with_header("etag", "part")
}
