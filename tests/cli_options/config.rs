use super::support::*;
use anyhow::{Context, Result};
use mockito::{Matcher, Server};
use std::{path::Path, process::Command};
use tempfile::tempdir;

fn default_config_command(home: &Path) -> Command {
    let mut command = s3m_command(home);
    command.env_remove("S3M_HOME");
    command
}

#[test]
fn test_config_flag_selects_endpoint_and_home_config_is_default() -> Result<()> {
    let mut explicit_server = Server::new();
    let home = tempdir()?;
    // `dirs::home_dir` honors HOME only on unix, so the default-path half is unix-only.
    #[cfg(unix)]
    let mut home_server = Server::new();
    #[cfg(unix)]
    write_file(
        &home_config(home.path()),
        config_yaml(&home_server.url(), "").as_bytes(),
    )?;
    let explicit = write_file(
        &home.path().join("explicit.yml"),
        config_yaml(&explicit_server.url(), "").as_bytes(),
    )?;
    #[cfg(unix)]
    let home_head = head_mock(&mut home_server, "home").expect(1).create();
    let explicit_head = head_mock(&mut explicit_server, "explicit")
        .expect(2)
        .create();

    for flag in ["--config", "-c"] {
        let output = s3m(
            home.path(),
            &[flag, &explicit, "get", "s3/bucket/key", "--meta"],
        )?;
        assert!(output.status.success(), "{flag}: {}", stderr(&output));
        assert_eq!(meta_origin(&output), "explicit", "{flag}");
    }

    #[cfg(unix)]
    {
        let mut command = default_config_command(home.path());
        assert!(
            command
                .get_envs()
                .any(|(key, value)| key == "S3M_HOME" && value.is_none()),
            "the default-config test must exercise HOME with S3M_HOME unset"
        );
        let output = command.args(["get", "s3/bucket/key", "--meta"]).output()?;
        assert!(output.status.success(), "{}", stderr(&output));
        assert_eq!(meta_origin(&output), "home");
        home_head.assert();
    }
    explicit_head.assert();
    Ok(())
}

#[test]
fn test_config_errors_fail_before_any_request() -> Result<()> {
    let mut server = Server::new();
    let head = head_mock(&mut server, "unused").expect(0).create();
    let home = tempdir()?;
    let missing = home.path().join("missing.yml");
    let malformed = write_file(
        &home.path().join("malformed.yml"),
        format!(
            "---\nhosts:\n  s3:\n    endpoint: {}\n    secret_key: \"{SECRET_KEY}\n",
            server.url()
        )
        .as_bytes(),
    )?;
    let duplicate = write_file(
        &home.path().join("duplicate.yml"),
        format!(
            "{}  s3:\n    endpoint: {}\n",
            config_yaml(&server.url(), ""),
            server.url()
        )
        .as_bytes(),
    )?;

    let cases: [(Vec<&str>, String); 3] = [
        (
            vec!["--config", missing.to_str().context("UTF-8 missing path")?],
            "Invalid file path or file does not exist".to_string(),
        ),
        (
            vec!["--config", &malformed],
            "unable to parse config file".to_string(),
        ),
        (
            vec!["--config", &duplicate],
            "duplicate key `s3`".to_string(),
        ),
    ];
    // A missing default config is only reachable where HOME selects the config dir.
    #[cfg(unix)]
    let cases = std::iter::once((vec![], home_config(home.path()).display().to_string()))
        .chain(cases)
        .collect::<Vec<_>>();

    for (config_args, expected) in cases {
        let mut args = config_args.clone();
        args.extend(["get", "s3/bucket/key", "--meta"]);
        let output = if config_args.is_empty() {
            default_config_command(home.path()).args(args).output()?
        } else {
            s3m(home.path(), &args)?
        };
        let err = stderr(&output);
        assert!(!output.status.success(), "{config_args:?} succeeded");
        assert!(err.contains(&expected), "{config_args:?}: {err}");
        assert!(!err.contains(SECRET_KEY), "{config_args:?} leaked: {err}");
    }

    head.assert();
    Ok(())
}

#[test]
fn test_verbose_count_selects_log_level_without_leaking_secrets() -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    let extra = format!("    enc_key: {ENC_KEY}\n");
    let config = write_file(
        &home.path().join("config.yml"),
        config_yaml(&server.url(), &extra).as_bytes(),
    )?;
    let head = head_mock(&mut server, "v").expect(5).create();

    // (flags before the subcommand, flags after it, INFO, DEBUG, TRACE)
    let cases: [(&[&str], &[&str], [bool; 3]); 5] = [
        (&[], &[], [false, false, false]),
        (&["-v"], &[], [true, false, false]),
        (&["-vv"], &[], [true, true, false]),
        (
            &["--verbose", "--verbose", "--verbose"],
            &[],
            [true, true, true],
        ),
        (&[], &["-vv"], [true, true, false]),
    ];

    for (before, after, [info, debug, trace]) in cases {
        let mut args = before.to_vec();
        args.extend(["--config", &config, "get", "s3/bucket/key", "--meta"]);
        args.extend(after);
        let output = s3m(home.path(), &args)?;
        let err = stderr(&output);
        assert!(output.status.success(), "{args:?}: {err}");
        assert_eq!(err.contains(" INFO "), info, "{args:?}: {err}");
        assert_eq!(err.contains(" DEBUG "), debug, "{args:?}: {err}");
        assert_eq!(err.contains(" TRACE "), trace, "{args:?}");
        let all = format!("{err}{}", String::from_utf8_lossy(&output.stdout));
        assert!(!all.contains(SECRET_KEY), "{args:?} logged secret_key");
        assert!(!all.contains(ENC_KEY), "{args:?} logged enc_key");
    }

    head.assert();
    Ok(())
}

#[test]
fn test_no_sign_request_omits_authorization_on_both_sides_of_subcommand() -> Result<()> {
    let mut server = Server::new();
    let home = tempdir()?;
    let config = write_file(
        &home.path().join("config.yml"),
        config_yaml(&server.url(), "").as_bytes(),
    )?;
    let unsigned = head_mock(&mut server, "unsigned")
        .match_header("authorization", Matcher::Missing)
        .expect(2)
        .create();
    let signed = head_mock(&mut server, "signed")
        .match_header(
            "authorization",
            Matcher::Regex("^AWS4-HMAC-SHA256 Credential=AKIDCLIOPTIONS/".to_string()),
        )
        .expect(1)
        .create();

    let runs: [(&[&str], &[&str], &str); 3] = [
        (&["--no-sign-request"], &[], "unsigned"),
        (&[], &["--no-sign-request"], "unsigned"),
        (&[], &[], "signed"),
    ];
    for (before, after, origin) in runs {
        let mut args = before.to_vec();
        args.extend(["--config", &config, "get", "s3/bucket/key", "--meta"]);
        args.extend(after);
        let output = s3m(home.path(), &args)?;
        assert!(output.status.success(), "{args:?}: {}", stderr(&output));
        assert_eq!(meta_origin(&output), origin, "{args:?}");
    }

    unsigned.assert();
    signed.assert();
    Ok(())
}
