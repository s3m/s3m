//! Miscellaneous e2e tests for s3m binary
//!
//! Tests for:
//! - Binary version
//! - Binary help, for the root and every (nested) subcommand
//! - Show command (display config hosts)

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::missing_panics_doc
)]

mod common;

use common::get_s3m_binary;
use std::{fs, path::Path, process::Command};
use tempfile::tempdir;

const SHOW_SECRET: &str = "Sh0w-Secret-Never-Printed";
const SHOW_ENC_KEY: &str = "0123456789abcdef0123456789abcdef";

/// Run s3m with an empty HOME (no config file) and return its stdout.
/// Help and version must succeed there without writing anything to HOME.
fn run_without_config(args: &[&str]) -> String {
    let home = tempdir().unwrap();
    let output = Command::new(get_s3m_binary())
        .env("HOME", home.path())
        .args(args)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !home.path().join(".config").exists(),
        "{args:?} wrote to HOME"
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Subcommand names listed under "Commands:", skipping clap's `help`.
fn listed_subcommands(help: &str) -> Vec<String> {
    help.lines()
        .skip_while(|line| !line.starts_with("Commands:"))
        .skip(1)
        .take_while(|line| line.starts_with("  "))
        .filter(|line| !line.starts_with("   "))
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(String::from)
        .collect()
}

#[test]
fn test_binary_version() {
    let short = format!("s3m {}", env!("CARGO_PKG_VERSION"));
    assert_eq!(run_without_config(&["-V"]), format!("{short}\n"));

    // rustc's format, `s3m X.Y.Z (hash date)`; e2e_binary checks the exact
    // commit against git.
    let long = run_without_config(&["--version"]);
    assert!(
        long == format!("{short}\n") || long.starts_with(&format!("{short} (")),
        "{long:?}"
    );
}

#[test]
fn test_binary_help() {
    for args in [&["--help"][..], &["-h"], &["help"]] {
        let stdout = run_without_config(args);
        assert!(stdout.starts_with("Store files"), "{args:?}: {stdout}");
        assert!(stdout.contains("Usage: s3m"), "{args:?}: {stdout}");
    }
}

#[test]
fn test_every_subcommand_help() {
    let mut pending: Vec<Vec<String>> = listed_subcommands(&run_without_config(&["--help"]))
        .into_iter()
        .map(|name| vec![name])
        .collect();
    assert!(!pending.is_empty(), "root help lists no subcommands");

    while let Some(path) = pending.pop() {
        let usage = format!("Usage: s3m {}", path.join(" "));
        let path_args: Vec<&str> = path.iter().map(String::as_str).collect();
        let flag_args = [path_args.as_slice(), &["--help"]].concat();
        let help_args = [&["help"][..], path_args.as_slice()].concat();

        let flag_help = run_without_config(&flag_args);
        assert!(flag_help.contains(&usage), "{flag_args:?}: {flag_help}");
        let command_help = run_without_config(&help_args);
        assert_eq!(command_help, flag_help, "{help_args:?}");

        for child in listed_subcommands(&flag_help) {
            pending.push([path.clone(), vec![child]].concat());
        }
    }
}

/// Run s3m with HOME set to `home` and return the process output.
fn run_with_home(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(get_s3m_binary())
        .env("HOME", home)
        .args(args)
        .output()
        .unwrap()
}

fn show_config() -> String {
    format!(
        "---\nhosts:\n  s3:\n    endpoint: http://127.0.0.1:9\n    access_key: AKIDSHOW\n    secret_key: {SHOW_SECRET}\n  backup:\n    region: eu-west-1\n    access_key: AKIDBACKUP\n    secret_key: {SHOW_SECRET}-backup\n    enc_key: {SHOW_ENC_KEY}\n  aws:\n    region: us-east-1\n    access_key: AKIDAWS\n    secret_key: {SHOW_SECRET}-aws\n"
    )
}

#[test]
fn test_show_lists_configured_host_names_without_secrets() {
    let home = tempdir().unwrap();
    let default_config = home.path().join(".config/s3m/config.yml");
    fs::create_dir_all(default_config.parent().unwrap()).unwrap();
    fs::write(&default_config, show_config()).unwrap();
    let explicit = home.path().join("explicit.yml");
    fs::write(&explicit, show_config()).unwrap();
    let explicit = explicit.to_str().unwrap();

    for args in [
        vec!["show"],
        vec!["--config", explicit, "show"],
        vec!["-c", explicit, "show"],
    ] {
        let output = run_with_home(home.path(), &args);
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(0), "{args:?}: {stderr}");
        assert_eq!(
            stdout, "Hosts:\n   - aws\n   - backup\n   - s3\n",
            "{args:?}"
        );
        assert!(stderr.is_empty(), "{args:?}: {stderr}");
        // Name what leaked, never the value: a failure message must not print a secret.
        for (what, value) in [
            ("a secret_key", SHOW_SECRET),
            ("the enc_key", SHOW_ENC_KEY),
            ("an access_key", "AKID"),
            ("the endpoint", "127.0.0.1"),
        ] {
            assert!(!stdout.contains(value), "{args:?} printed {what}");
        }
    }
}

#[test]
fn test_show_fails_on_missing_or_malformed_config_without_leaking() {
    let home = tempdir().unwrap();
    let malformed = home.path().join("malformed.yml");
    fs::write(
        &malformed,
        format!("---\nhosts:\n  s3:\n    secret_key: {SHOW_SECRET}\n    endpoint: [unclosed\n"),
    )
    .unwrap();
    let missing = home.path().join("missing.yml");

    for (args, code, message) in [
        (vec!["show"], 1, "unable to open config file"),
        (
            vec!["--config", missing.to_str().unwrap(), "show"],
            2,
            "Invalid file path or file does not exist",
        ),
        (
            vec!["--config", malformed.to_str().unwrap(), "show"],
            1,
            "unable to parse config file",
        ),
        (vec!["show", "extra"], 2, "unexpected argument 'extra'"),
    ] {
        let output = run_with_home(home.path(), &args);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(output.status.code(), Some(code), "{args:?}: {stderr}");
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(!stderr.contains(SHOW_SECRET), "{args:?} leaked: {stderr}");
        assert!(output.stdout.is_empty(), "{args:?}");
    }
}
