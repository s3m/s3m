use anyhow::{Result, ensure};
use mockito::Matcher;
use std::{fs, path::PathBuf, process::Command};
use tempfile::{TempDir, tempdir};

pub struct Cli {
    home: TempDir,
    config: PathBuf,
    work: PathBuf,
}

impl Cli {
    pub fn new(endpoint: &str) -> Result<Self> {
        let home = tempdir()?;
        let config = home.path().join("config.yml");
        let work = home.path().join("work");
        fs::create_dir(&work)?;
        fs::write(
            &config,
            format!(
                "hosts:\n  s3:\n    endpoint: {endpoint}\n    region: eu-central-1\n    access_key: AUDITKEY\n    secret_key: audit-secret\n"
            ),
        )?;
        Ok(Self { home, config, work })
    }

    pub fn run(&self, args: &[&str], code: i32) -> Result<String> {
        let output = Command::new(env!("CARGO_BIN_EXE_s3m"))
            .env("HOME", self.home.path())
            .env("USERPROFILE", self.home.path())
            .current_dir(&self.work)
            .arg("--config")
            .arg(&self.config)
            .args(args)
            .output()?;
        ensure!(
            output.status.code() == Some(code),
            "{args:?}: {:?}\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if code == 0 {
            Ok(String::from_utf8(output.stdout)?)
        } else {
            ensure!(output.stdout.is_empty(), "{args:?}: unexpected stdout");
            Ok(String::from_utf8(output.stderr)?)
        }
    }

    pub fn files(&self) -> Result<Vec<PathBuf>> {
        Ok(fs::read_dir(&self.work)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?)
    }
}

pub fn signed() -> Matcher {
    Matcher::Regex(
        r"^AWS4-HMAC-SHA256 Credential=AUDITKEY/[0-9]{8}/eu-central-1/s3/aws4_request, SignedHeaders=.+, Signature=[0-9a-f]{64}$"
            .into(),
    )
}

pub fn query(
    expected: &[(&str, &str)],
) -> impl Fn(&mockito::Request) -> bool + Send + Sync + 'static {
    let mut expected: Vec<(String, String)> = expected
        .iter()
        .map(|(key, value)| ((*key).into(), (*value).into()))
        .collect();
    expected.sort();
    move |request| {
        let query = request
            .path_and_query()
            .split_once('?')
            .map_or("", |(_, value)| value);
        let mut actual: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        actual.sort();
        actual == expected
    }
}
