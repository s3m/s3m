use crate::s3m::args::{subcommands, validators};
use clap::{Arg, ArgMatches, Command};
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub struct ArgParser {
    s3m_dir: PathBuf,
    default_config: OsString,
}

impl ArgParser {
    #[must_use]
    pub fn new(s3m_dir: &Path) -> Self {
        let default_config_path: PathBuf = s3m_dir.join("config.yml");
        Self {
            s3m_dir: s3m_dir.to_owned(),
            default_config: default_config_path.as_os_str().to_owned(),
        }
    }

    #[must_use]
    pub fn get_matches(&self) -> ArgMatches {
        Command::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_negates_reqs(true)
        .after_help(format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/streams] use the option (--clean) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", self.s3m_dir.display()).as_ref())
        .arg(
            Arg::new("clean")
            .long("clean")
           .help(format!("remove {}/streams directory", self.s3m_dir.display()).as_ref()),
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
                .default_value_os(&self.default_config)
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
        .get_matches()
    }
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

    #[test]
    fn test_foo() -> Result<()> {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir()?;
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(config_path)?;
        config.write_all(CONF.as_bytes())?;

        //let arg_vec = vec!["my_prog", "some", "args", "to", "parse"];
        //let a = ArgParser::new(tmp_dir.path()).get_matches();
        //        let config = matches.value_of("config").context("config file missing")?;

        Ok(())
    }
}
