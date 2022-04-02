use crate::s3m::args::{subcommands, validators};
use clap::{Arg, Command};
use std::env;
use std::ffi::OsStr;

#[must_use]
pub fn new<'a>(config_path: &'a OsStr, after_help: &'a str, help_clean: &'a str) -> Command<'a> {
    Command::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_negates_reqs(true)
        .after_help(after_help.as_ref())
        .arg(
            Arg::new("clean")
            .long("clean")
            .help(help_clean.as_ref())
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
            .possible_values([
                "private",
                "public-read",
                "public-read-write",
                "authenticated-read",
                "aws-exec-read",
                "bucket-owner-read",
                "bucket-owner-full-control",
            ])
            .short('a')
            .takes_value(true)
        )
        .arg(
            Arg::new("meta")
            .long("meta")
            .short('m')
            .help("User-defined object metadata \"x-amz-meta-*\", example: key1=value1;key2=value2")
            .takes_value(true)
            .validator(validators::key_val)
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
            .takes_value(true)
            .validator(validators::is_num)
        )
        .arg(
            Arg::new("config")
            .default_value_os(config_path.as_ref())
            .help("config.yml")
            .long("config")
            .takes_value(true)
            .short('c')
            .validator(validators::is_file)
            .value_name("config.yml"),
        )
        .arg(
            Arg::new("arguments")
            .help("/path/to/file <s3 provider>/<bucket>/<file>")
            .required_unless_present("clean")
            .min_values(1)
            .max_values(2),
        )
        .subcommand(subcommands::subcommand_acl())
        .subcommand(subcommands::subcommand_get())
        .subcommand(subcommands::subcommand_ls())
        .subcommand(subcommands::subcommand_rm())
        .subcommand(subcommands::subcommand_share())
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
        let cmd = new(config_path.as_os_str(), "a", "b");
        Ok(cmd.get_matches_from(args))
    }

    #[test]
    fn test_put_check_defaults() -> Result<()> {
        let m = get_matches(vec!["s3m", "test"])?;
        assert_eq!(m.value_of("arguments"), Some("test"));
        assert_eq!(m.value_of("buffer"), Some("10485760"));
        Ok(())
    }
}
