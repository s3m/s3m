use crate::s3::{Credentials, Region, S3};
use crate::s3m::Config;
use anyhow::{anyhow, Context, Result};
use clap::{App, AppSettings, Arg, SubCommand};
use colored::Colorize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::exit;

const STDIN_BUFF_SIZE: usize = 536_870_912;

#[derive(Debug)]
pub enum Action {
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
    },
    PutObject {
        buf_size: usize,
        file: String,
        home_dir: PathBuf,
        key: String,
        stdin: bool,
        threads: usize,
    },
    DeleteObject {
        key: String,
        upload_id: String,
    },
    GetObject {
        key: String,
        get_head: bool,
    },
    ShareObject {
        key: String,
        expire: usize,
    },
}

fn me() -> Option<String> {
    std::env::current_exe()
        .ok()?
        .file_name()?
        .to_str()?
        .to_owned()
        .into()
}

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

pub fn start() -> Result<(S3, Action)> {
    let home_dir = match dirs::home_dir() {
        Some(h) => h,
        None => PathBuf::from("/tmp"),
    };

    fs::create_dir_all(format!("{}/.s3m/", home_dir.display()))
        .context("unable to create home dir ~/.s3m")?;

    let default_config = format!("{}/.s3m/config.yml", home_dir.display());
    let default_threads = if num_cpus::get() > 8 {
        "8".to_string()
    } else {
        num_cpus::get().to_string()
    };

    let matches = App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::SubcommandsNegateReqs)
        .after_help(format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/.s3m/streams] use the option (-r) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", home_dir.display()).as_ref())
        .arg(
            Arg::with_name("remove")
                .short("r")
                .long("remove")
                .help(&format!("remove {}/.s3m/streams directory", home_dir.display())),
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
                .default_value(&default_threads)
                .short("t")
                .required(true)
                .validator(is_num),
        )
        .arg(
            Arg::with_name("config")
                .help("config.yml")
                .long("config")
                .default_value(&default_config)
                .short("c")
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
            SubCommand::with_name("ls").about("List objects and in-progress multipart uploads").arg(
                Arg::with_name("arguments")
                    .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                    .required(true)
                    .min_values(1),
            )
            .arg(
                Arg::with_name("ListMultipartUploads")
                .help("Lists in-progress multipart uploads")
                .long("multipart")
                .short("m")
            ),
        )
        .subcommand(
            SubCommand::with_name("rm").about("Delete objects and aborts a multipart upload").arg(
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
            SubCommand::with_name("get").about("Retrieves objects").arg(
                Arg::with_name("arguments")
                    .help("<s3 provider>/<bucket>/<file>")
                    .required(true)
                    .min_values(1),
            )
            .arg(
                Arg::with_name("HeadObject")
                .help("Retrieves metadata from an object without returning the object itself")
                .long("head")
                .short("h")
            ),
        )
        .subcommand(
            SubCommand::with_name("share").about("Share object using a presigned URL").arg(
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
        .get_matches();

    // parse config file
    let config = matches.value_of("config").unwrap();
    let file = fs::File::open(config).context("unable to open file")?;
    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            return Err(anyhow!("could not parse the configuration file: {}", e));
        }
        Ok(yml) => yml,
    };

    if matches.is_present("remove") {
        let streams = format!("{}/.s3m/streams", home_dir.display());
        fs::remove_dir_all(&streams)?;
        exit(0);
    }

    let threads = matches.value_of("threads").unwrap().parse::<usize>()?;

    // Host, Bucket, Path
    let mut hbp: Vec<&str>;
    let input_stdin = !atty::is(atty::Stream::Stdin); // isatty returns false if there's something in stdin.
    let mut input_file = "";

    // If stdin use 512M (probably input is close to 5TB) if buffer is not defined
    let buf_size = if input_stdin && matches.occurrences_of("buffer") == 0 {
        STDIN_BUFF_SIZE
    } else {
        matches.value_of("buffer").unwrap().parse::<usize>()?
    };

    // ListObjects
    if let Some(ls) = matches.subcommand_matches("ls") {
        let args: Vec<&str> = ls.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    // ShareObject
    } else if let Some(share) = matches.subcommand_matches("share") {
        let args: Vec<&str> = share.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    // GetObject
    } else if let Some(get) = matches.subcommand_matches("get") {
        let args: Vec<&str> = get.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    // DeleteObject
    } else if let Some(rm) = matches.subcommand_matches("rm") {
        let args: Vec<&str> = rm.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    } else {
        // PutObject
        let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
        if args.len() == 2 {
            hbp = args[1].split('/').filter(|s| !s.is_empty()).collect();
            input_file = args[0];
        } else if input_stdin {
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
        } else {
            return Err(anyhow!(
                "missing argument or standar input. For more information try: --help"
            ));
        }
    }

    // HOST
    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp.remove(0);
        &config.hosts[key]
    } else {
        return Err(anyhow!("no \"host\" found, check ~/.s3m/config.yml"));
    };

    // REGION
    let region = match &host.region {
        Some(h) => match h.parse::<Region>() {
            Ok(r) => r,
            Err(e) => {
                return Err(anyhow!(e));
            }
        },
        None => match &host.endpoint {
            Some(r) => Region::Custom {
                name: "".to_string(),
                endpoint: r.to_string(),
            },
            None => {
                return Err(anyhow!("could not parse host need an endpoint or region"));
            }
        },
    };

    // BUCKET
    let bucket = if !hbp.is_empty() {
        Some(hbp.remove(0).to_string())
    } else if matches.subcommand_matches("ls").is_some() {
        None
    } else {
        return Err(anyhow!(
            "no \"bucket\" found, try: {} /path/to/file <s3 provider>/<bucket name>/file",
            me().unwrap_or_else(|| "s3m".to_string()),
        ));
    };

    // AUTH
    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    // S3
    let s3 = S3::new(&credentials, &region, bucket.clone());

    if let Some(sub_m) = matches.subcommand_matches("ls") {
        let list_multipart_uploads = sub_m.is_present("ListMultipartUploads");
        Ok((
            s3,
            Action::ListObjects {
                bucket,
                list_multipart_uploads,
            },
        ))
    } else {
        if hbp.is_empty() {
            return Err(anyhow!(
                "file name missing, <s3 provider>/<bucket>/{}, For more information try {}",
                "<file name>".red(),
                "--help".green()
            ));
        }
        let key = hbp.join("/");

        // GetObject
        if let Some(sub_m) = matches.subcommand_matches("get") {
            let get_head = sub_m.is_present("HeadObject");
            Ok((s3, Action::GetObject { key, get_head }))
        // ShareObject
        } else if let Some(sub_m) = matches.subcommand_matches("share") {
            let expire = sub_m.value_of("expire").unwrap().parse::<usize>()?;
            Ok((s3, Action::ShareObject { key, expire }))
        // DeleteObject
        } else if let Some(sub_m) = matches.subcommand_matches("rm") {
            let upload_id = sub_m.value_of("UploadId").unwrap_or_default().to_string();
            Ok((s3, Action::DeleteObject { key, upload_id }))
        // PutObject
        } else {
            Ok((
                s3,
                Action::PutObject {
                    buf_size,
                    file: input_file.to_string(),
                    home_dir,
                    key,
                    stdin: input_stdin,
                    threads,
                },
            ))
        }
    }
}
