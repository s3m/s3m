pub mod cmd_acl;
pub mod cmd_get;
pub mod cmd_ls;
pub mod cmd_mb;
pub mod cmd_rm;
pub mod cmd_share;

use anyhow::{Context, Result};
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
    ValueParser::from(move |s: &str| -> std::result::Result<(), String> {
        for pair in s.split(';') {
            match pair.split_once('=') {
                Some(_) => (),
                None => return Err(String::from("metadata format is key1=value1;key2=value2")),
            }
        }
        Ok(())
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
        if fs::metadata(s).map_err(|e| e.to_string())?.is_file() {
            Ok(PathBuf::from(s))
        } else {
            Err(format!("Cannot read the file: {}", s))
        }
    })
}

pub fn get_config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().map_or_else(|| PathBuf::from("/tmp"), |h| h);

    let config_path = Path::new(&home_dir).join(".config").join("s3m");
    fs::create_dir_all(&config_path)
        .context(format!("unable to create: {}", &config_path.display()))?;

    Ok(config_path)
}

pub fn new() -> Command {
    // get config file path (default: ~/.config/s3m/config.yml)
    let config_file_path = get_config_path()
        .unwrap_or_else(|err| {
            eprintln!("Error getting config path: {}", err);
            PathBuf::from("/tmp") // Fallback directory
        })
        .join("config.yml");

    // get the streams directory path (default: ~/.config/s3m/streams)
    let config_streams_path = get_config_path()
        .unwrap_or_else(|err| {
            eprintln!("Error getting config path: {}", err);
            PathBuf::from("/tmp") // Fallback directory
        })
        .join("streams");

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
            .number_of_values(0)
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
        .subcommand(cmd_acl::subcommand_acl())
        .subcommand(cmd_get::subcommand_get())
        .subcommand(cmd_ls::subcommand_ls())
        .subcommand(cmd_mb::subcommand_mb())
        .subcommand(cmd_rm::subcommand_rm())
        .subcommand(cmd_share::subcommand_share())
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
    secret_key: YYY"#;

    fn get_matches(args: Vec<&str>) -> Result<clap::ArgMatches> {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir()?;
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path)?;
        config.write_all(CONF.as_bytes())?;
        let cmd = new();
        Ok(cmd.get_matches_from(args))
    }

    #[test]
    fn test_put_check_defaults() -> Result<()> {
        let m = get_matches(vec!["s3m", "test"])?;
        assert_eq!(m.get_one::<&str>("arguments"), Some("test").as_ref());
        assert_eq!(m.get_one::<&str>("buffer"), Some("10485760").as_ref());
        Ok(())
    }
}
