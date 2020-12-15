use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

fn is_num(s: String) -> Result<(), String> {
    if let Err(..) = s.parse::<u64>() {
        return Err(String::from("Not a valid number!"));
    }
    Ok(())
}

fn is_file(s: String) -> Result<(), String> {
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
    default_threads: String,
}

impl ArgParser {
    #[must_use]
    pub fn new(s3m_dir: &Path) -> Self {
        let default_config_path: PathBuf = s3m_dir.join("config.yml");
        let default_threads = if num_cpus::get() > 8 {
            "8".to_string()
        } else {
            num_cpus::get().to_string()
        };
        Self {
            s3m_dir: s3m_dir.to_owned(),
            default_config: default_config_path.as_os_str().to_owned(),
            default_threads,
        }
    }

    #[must_use]
    pub fn get_matches(&self) -> ArgMatches {
        App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::SubcommandsNegateReqs)
                .after_help(format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/streams] use the option (-r) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", self.s3m_dir.display()).as_ref())
        .arg(
            Arg::with_name("remove").short("r").long("remove")
           .help(format!("remove {}/streams directory", self.s3m_dir.display()).as_ref()),
        )
        .arg(
            Arg::with_name("buffer")
                .help("Buffer size in bytes, max value: 5 GB (5,368,709,120 bytes)")
                .long("buffer")
                .default_value("10485760")
                .short("b")
                .required(true)
                .validator(is_num),
        )
        .arg(
            Arg::with_name("threads")
                .help("Number of threads to use")
                .long("threads")
                .short("t")
                .default_value(&self.default_threads)
                .required(true)
                .validator(is_num),
        )
        .arg(
            Arg::with_name("config")
                .help("config.yml")
                .long("config")
                .short("c")
                .default_value_os(&self.default_config)
                .required(true)
                .value_name("config.yml")
                .validator(is_file),
        )
        .arg(
            Arg::with_name("arguments")
                .help("/path/to/file <s3 provider>/<bucket>/<file>")
                .required_unless_one(&["rm", "ls", "remove", "get", "share"])
                .min_values(1)
                .max_values(2),
        )
        .subcommand(
            SubCommand::with_name("ls")
                .about("List objects and in-progress multipart uploads")
                .arg(
                    Arg::with_name("arguments")
                        .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("ListMultipartUploads")
                        .help("Lists in-progress multipart uploads")
                        .long("multipart")
                        .short("m"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Delete objects and aborts a multipart upload")
                .arg(
                    Arg::with_name("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("UploadId")
                        .help("aborts a multipart upload")
                        .long("abort")
                        .short("a")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Retrieves objects")
                .arg(
                    Arg::with_name("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("HeadObject")
                        .help(
                            "Retrieves metadata from an object without returning the object itself",
                        )
                        .long("head")
                        .short("h"),
                ),
        )
        .subcommand(
            SubCommand::with_name("share")
                .about("Share object using a presigned URL")
                .arg(
                    Arg::with_name("arguments")
                        .help("<s3 provider>/<bucket>/<file>")
                        .required(true)
                        .min_values(1),
                )
                .arg(
                    Arg::with_name("expire")
                        .help("Time period in seconds, max value 604800 (seven days)")
                        .long("expire")
                        .short("e")
                        .default_value("43200")
                        .required(true)
                        .validator(is_num),
                ),
        )
        .get_matches()
    }
}
