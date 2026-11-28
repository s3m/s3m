use anyhow::{Context, Result};
use mockito::{Mock, ServerGuard};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

pub(super) const SECRET_KEY: &str = "s3cr3t-Value-Never-Logged";
pub(super) const ENC_KEY: &str = "9f8e7d6c5b4a39281706f5e4d3c2b1a0";
pub(super) const PART_SIZE: usize = 5 * 1024 * 1024;
pub(super) const INITIATE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>UP1</UploadId></InitiateMultipartUploadResult>"#;
pub(super) const COMPLETE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Location>loc</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>"done"</ETag></CompleteMultipartUploadResult>"#;

pub(super) fn config_yaml(endpoint: &str, extra: &str) -> String {
    format!(
        "---\nhosts:\n  s3:\n    endpoint: {endpoint}\n    access_key: AKIDCLIOPTIONS\n    secret_key: {SECRET_KEY}\n{extra}"
    )
}

pub(super) fn write_file(path: &Path, contents: &[u8]) -> Result<String> {
    fs::create_dir_all(path.parent().context("fixture file parent")?)?;
    fs::write(path, contents)?;
    Ok(path.to_str().context("UTF-8 fixture path")?.to_string())
}

#[cfg(unix)]
pub(super) fn home_config(home: &Path) -> std::path::PathBuf {
    home.join(".config").join("s3m").join("config.yml")
}

pub(super) fn s3m(home: &Path, args: &[&str]) -> Result<Output> {
    Ok(s3m_command(home).args(args).output()?)
}

pub(super) fn s3m_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3m"));
    command
        .env("HOME", home)
        .env("S3M_HOME", home.join(".config").join("s3m"));
    command
}

pub(super) fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

pub(super) fn head_mock(server: &mut ServerGuard, origin: &str) -> Mock {
    server
        .mock("HEAD", "/bucket/key")
        .with_status(200)
        .with_header("etag", "\"abc\"")
        .with_header("x-amz-meta-origin", origin)
}

pub(super) fn meta_origin(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("x-amz-meta-origin:"))
        .map_or_else(String::new, |value| value.trim().to_string())
}
