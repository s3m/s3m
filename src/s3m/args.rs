use clap::{Arg, ArgMatches, Command};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

fn is_num(s: &str) -> Result<(), String> {
    if let Err(..) = s.parse::<u64>() {
        return Err(String::from("Not a valid number!"));
    }
    Ok(())
}

fn is_file(s: &str) -> Result<(), String> {
    if fs::metadata(&s).map_err(|e| e.to_string())?.is_file() {
        Ok(())
    } else {
        Err(format!(
            "cannot read the file: {}, verify file exist and is not a directory.",
            s
        ))
    }
}

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
            Arg::new("attr")
                .takes_value(true)
                .help("Add custom metadata for the object (format: KeyName1=string;KeyName2=string), example: -a ACL=private")
                .long("attributes")
                .short('a'),
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
                .validator(is_num)
        )
        .arg(
            Arg::new("config")
                .default_value_os(&self.default_config)
                .help("config.yml")
                .long("config")
                .takes_value(true)
                .short('c')
                .validator(is_file)
                .value_name("config.yml"),
        )
        .arg(
            Arg::new("arguments")
                .help("/path/to/file <s3 provider>/<bucket>/<file>")
                .required_unless_present("clean")
                .min_values(1)
                .max_values(2),
        )
        .subcommand(
            Command::new("ls")
                .about("List objects and in-progress multipart uploads")
                .arg(
                    Arg::new("arguments")
                        .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::new("ListMultipartUploads")
                        .help("Lists in-progress multipart uploads")
                        .long("multipart")
                        .short('m'),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("Delete objects and aborts a multipart upload")
                .arg(
                    Arg::new("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::new("UploadId")
                        .help("aborts a multipart upload")
                        .long("abort")
                        .short('a')
                        .takes_value(true),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Retrieves objects")
                .arg(
                    Arg::new("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::new("HeadObject")
                        .help("Retrieves metadata from an object without returning the object itself")
                        .long("head")
                        .short('H'),
                ),
        )
        .subcommand(
            Command::new("share")
                .about("Share object using a presigned URL")
                .arg(
                    Arg::new("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::new("expire")
                        .help("Time period in seconds, max value 604800 (seven days)")
                        .long("expire")
                        .short('e')
                        .default_value("43200")
                        .takes_value(true)
                        .validator(is_num),
                ),
        )
        .get_matches()
    }
}
