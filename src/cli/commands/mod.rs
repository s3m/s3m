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
    env, fs,
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

    let after_help = format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/streams] use the option (--clean) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", config_streams_path.display());

    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    Command::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_negates_reqs(true)
        .after_help(after_help)
        .color(ColorChoice::Auto)
        .styles(styles)
        .arg(
            Arg::new("clean")
            .long("clean")
            .help(format!("remove {} directory", config_streams_path.display()))
            .num_args(0)
        )
        .arg(
            Arg::new("quiet")
            .long("quiet")
            .short('q')
            .help("Don't show progress bar when uploading")
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
            .help("Specify a directory for temporarily storing the STDIN buffer.")
            .short('t')
            .long("tmp-dir")
            .default_value(std::env::temp_dir().into_os_string())
            .value_parser(validator_is_dir())
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

        Ok(())
    }
}
