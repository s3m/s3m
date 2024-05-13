pub mod cmd_acl;
pub mod cmd_cb;
pub mod cmd_get;
pub mod cmd_ls;
pub mod cmd_rm;
pub mod cmd_share;
pub mod cmd_show;

use clap::{
    builder::styling::{AnsiColor, Effects, Styles},
    builder::ValueParser,
    Arg, ColorChoice, Command,
};
use std::{
    cmp, env, fs,
    path::{Path, PathBuf},
};

pub fn validator_key_value() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<String, String> {
        for pair in s.split(';') {
            match pair.split_once('=') {
                Some(_) => (),
                None => return Err(String::from("metadata format is key1=value1;key2=value2")),
            }
        }
        Ok(s.to_string())
    })
}

pub fn validator_is_num() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<usize, String> {
        s.parse::<usize>()
            .map_err(|_| String::from("Not a valid number"))
    })
}

pub fn validator_is_file() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        if let Ok(metadata) = fs::metadata(s) {
            if metadata.is_file() {
                return Ok(PathBuf::from(s));
            }
        }

        Err(format!("Invalid file path or file does not exist: '{s}'"))
    })
}

pub fn validator_is_dir() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        if let Ok(metadata) = fs::metadata(s) {
            if metadata.is_dir() {
                return Ok(PathBuf::from(s));
            }
        }

        Err(format!("Invalid path or directory does not exist: '{s}'"))
    })
}

pub fn new(config_path: &Path) -> Command {
    // get config file path (default: ~/.config/s3m/config.yml)
    let config_file_path = config_path.join("config.yml");

    // get the streams directory path (default: ~/.config/s3m/streams)
    let config_streams_path = config_path.join("streams");

    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    // num_cpus::get_physical() - 2 returns at least 1 type usize
    let num_threads = cmp::min((num_cpus::get_physical() - 2).max(1), u8::MAX as usize).to_string();

    Command::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_negates_reqs(true)
        .color(ColorChoice::Auto)
        .styles(styles)
        .arg(
            Arg::new("clean")
            .long("clean")
            .help(format!("remove {} directory", config_streams_path.display()))
            .num_args(0)
        )
        .arg(
            Arg::new("checksum")
            .help("Additional checksums algorithms")
            .long("checksum")
            .value_parser([
                "crc32",
                "crc32c",
                "sha1",
                "sha256",
            ])
            .value_name("algorithm")
            .num_args(1)
        )
        .arg(
            Arg::new("quiet")
            .long("quiet")
            .short('q')
            .help("Don't show progress bar")
            .num_args(0)
        )
        .arg(
            Arg::new("acl")
            .help("The canned ACL to apply to the object example")
            .long("acl")
            .value_parser([
                "private",
                "public-read",
                "public-read-write",
                "authenticated-read",
                "aws-exec-read",
                "bucket-owner-read",
                "bucket-owner-full-control",
            ])
            .short('a')
            .num_args(1)
        )
        .arg(
            Arg::new("meta")
            .long("meta")
            .short('m')
            .help("User-defined object metadata \"x-amz-meta-*\", example: \"key1=value1;key2=value2\"")
            .value_parser(validator_key_value())
            .num_args(1)
        )
        .arg(
            Arg::new("pipe")
            .long("pipe")
            .short('p')
            .help("Read from STDIN")
            .num_args(0)
        )
        .arg(
            Arg::new("buffer")
            .default_value("10485760")
            .help("Buffer \"part size\" in bytes, doesn't apply when reading from STDIN (--pipe option)")
            .long("buffer")
            .short('b')
            .num_args(1)
            .value_parser(validator_is_num())
        )
        .arg(
            Arg::new("config")
            .default_value(config_file_path.into_os_string())
            .long("config")
            .num_args(1)
            .short('c')
            .value_parser(validator_is_file())
            .value_name("config.yml")
        )
        .arg(
            Arg::new("arguments")
            .help("/path/to/file <s3 provider>/<bucket>/<file>")
            .required_unless_present("clean")
            .num_args(1..=2)
        )
        .arg(
            Arg::new("tmp-dir")
            .help("Specify a directory for temporarily storing the STDIN buffer")
            .short('t')
            .long("tmp-dir")
            .default_value(std::env::temp_dir().into_os_string())
            .value_parser(validator_is_dir())
            .num_args(1)
        )
        .arg(
            Arg::new("verbose")
            .help("Verbosity level")
            .short('v')
            .long("verbose")
            .global(true)
            .action(clap::ArgAction::Count)
        )
        .arg(
            Arg::new("number")
            .help("Number of max concurrent requests")
            .short('n')
            .long("number")
            .default_value(num_threads)
            .value_parser(clap::value_parser!(u8).range(1..=255))
            .num_args(1)
        )
        .arg(
            Arg::new("no-sign-request")
            .help("Make requests as anonymous user (no credentials used)")
            .long("no-sign-request")
            .global(true)
            .num_args(0)
        )
        .arg(
            Arg::new("throttle")
            .help("Bandwidth throttle in kilobytes per second, 0 to disable")
            .long("kilobytes")
            .short('k')
            .default_value("0")
            .value_name("kilobytes")
            .value_parser(validator_is_num())
            .global(true)
            .num_args(1)
        )
        .arg(
            Arg::new("retries")
            .help("Number of retries")
            .long("retries")
            .short('r')
            .default_value("3")
            .value_parser(validator_is_num())
            .num_args(1)
        )
        .subcommand(cmd_acl::command())
        .subcommand(cmd_get::command())
        .subcommand(cmd_ls::command())
        .subcommand(cmd_cb::command())
        .subcommand(cmd_rm::command())
        .subcommand(cmd_share::command())
        .subcommand(cmd_show::command())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs::File;
    use std::io::Write;
    use tempfile::Builder;

    const CONF: &str = r#"---
hosts:
  s3:
    region: xx-region-y
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    fn get_config() -> Result<PathBuf> {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir()?;
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path)?;
        config.write_all(CONF.as_bytes())?;
        Ok(tmp_dir.into_path())
    }

    #[test]
    fn test_check_defaults() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("test")
        );
        assert_eq!(m.get_one::<usize>("buffer").map(|s| *s), Some(10485760));
        assert_eq!(m.get_one::<bool>("clean").copied(), Some(false));
        assert_eq!(
            m.get_one::<PathBuf>("config")
                .map(|s| s.display().to_string()),
            Some(config.join("config.yml").display().to_string())
        );
        assert_eq!(m.get_one::<usize>("throttle").map(|s| *s), Some(0));
        assert_eq!(m.get_one::<usize>("retries").map(|s| *s), Some(3));

        Ok(())
    }

    #[test]
    fn test_check_clean() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--clean"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("clean").copied(), Some(true));

        Ok(())
    }

    #[test]
    fn test_check_buffer() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--buffer", "123"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<usize>("buffer").map(|s| *s), Some(123));

        Ok(())
    }

    #[test]
    fn test_check_config() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--config", "test.yml"]);
        assert!(m.is_err());

        Ok(())
    }

    #[test]
    fn test_check_pipe() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--pipe"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("pipe").copied(), Some(true));

        Ok(())
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_check_tmp_dir() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--tmp-dir", "/tmp"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<PathBuf>("tmp-dir")
                .map(|s| s.display().to_string()),
            Some("/tmp".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_check_quiet() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--quiet"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(m.get_one::<bool>("quiet").copied(), Some(true));

        Ok(())
    }

    #[test]
    fn test_check_acl() -> Result<()> {
        let tets = vec![
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ];
        for acl in tets {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--acl", acl]);
            assert!(m.is_ok());

            // get matches
            let m = m.unwrap();
            assert_eq!(m.get_one::<String>("acl").map(String::as_str), Some(acl));
        }
        Ok(())
    }

    #[test]
    fn test_check_meta() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--meta", "key1=value1;key2=value2"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("meta").map(String::as_str),
            Some("key1=value1;key2=value2")
        );

        Ok(())
    }

    #[test]
    fn test_check_checksum() -> Result<()> {
        let tests = vec!["crc32", "crc32c", "sha1", "sha256"];
        for checksum in tests {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--checksum", checksum]);
            assert!(m.is_ok());

            // get matches
            let m = m.unwrap();
            assert_eq!(
                m.get_one::<String>("checksum").map(String::as_str),
                Some(checksum)
            );
        }
        Ok(())
    }

    #[test]
    fn test_check_number() -> Result<()> {
        let tests = vec!["1", "255"];
        for n in tests {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--number", n]);
            assert!(m.is_ok());

            // get matches
            let m = m.unwrap();
            assert_eq!(m.get_one::<u8>("number").map(|s| *s), Some(n.parse()?));
        }
        Ok(())
    }

    #[test]
    fn test_check_number_invalid() -> Result<()> {
        let tests = vec!["0", "256"];
        for n in tests {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--number", n]);
            assert!(m.is_err());
        }
        Ok(())
    }

    #[test]
    fn test_check_no_sign_request() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--no-sign-request"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<bool>("no-sign-request")
                .copied()
                .unwrap_or(false),
            true
        );
        Ok(())
    }

    #[test]
    fn test_check_arguments() -> Result<()> {
        let config = get_config().unwrap();
        let cmd = new(&config);
        let m = cmd.try_get_matches_from(vec!["s3m", "/path/to/file", "s3/my-bucket/path/to/file"]);

        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("/path/to/file")
        );
        let args: Vec<&str> = m
            .get_many::<String>("arguments")
            .unwrap_or_default()
            .map(String::as_str)
            .collect();
        assert_eq!(args, vec!["/path/to/file", "s3/my-bucket/path/to/file"]);

        Ok(())
    }

    #[test]
    fn test_retries() -> Result<()> {
        let tests = vec!["0", "10"];
        for n in tests {
            let config = get_config().unwrap();
            let cmd = new(&config);
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--retries", n]);
            assert!(m.is_ok());

            // get matches
            let m = m.unwrap();
            assert_eq!(m.get_one::<usize>("retries").map(|s| *s), Some(n.parse()?));
        }
        Ok(())
    }
}
