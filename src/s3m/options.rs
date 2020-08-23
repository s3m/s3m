use crate::s3::{Credentials, Region, S3};
use crate::s3m::Config;
use anyhow::{Context, Result};
use clap::{App, AppSettings, Arg, SubCommand};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::exit;

#[derive(Debug)]
pub enum Action {
    ListObjects(Option<String>),
    PutObject {
        stdin: bool,
        file: String,
        key: String,
        buffer: u64,
        threads: usize,
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
        .context("Unable to create ~/.s3m dir")?;

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
                .help("Part size in bytes, max value: 5 GB (5,368,709,120 bytes)")
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
                .help("/path/to/file <host>/bucket/<file>")
                .required_unless_one(&["ls", "remove"])
                .min_values(1)
                .max_values(2),
        )
        .subcommand(
            SubCommand::with_name("ls").about("list objects").arg(
                Arg::with_name("arguments")
                    .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                    .required(true)
                    .min_values(1),
            ),
        )
        .get_matches();

    // parse config file
    let config = matches.value_of("config").unwrap();
    let file = fs::File::open(config).context("Unable to open file")?;
    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            eprintln!("Error parsing configuration file: {}", e);
            exit(1);
        }
        Ok(yml) => yml,
    };

    if matches.is_present("remove") {
        let streams = format!("{}/.s3m/streams", home_dir.display());
        fs::remove_dir_all(&streams)?;
        exit(0);
    }

    let buffer = matches.value_of("buffer").unwrap();

    let threads = matches.value_of("threads").unwrap().parse::<usize>()?;

    // Host, Bucket, Path
    let mut hbp: Vec<&str>;
    let mut input_stdin = false;
    let mut input_file = "";

    // ListObjects
    if let Some(ls) = matches.subcommand_matches("ls") {
        let args: Vec<&str> = ls.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    } else {
        // PutObject
        let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
        if args.len() == 2 {
            hbp = args[1].split('/').filter(|s| !s.is_empty()).collect();
            input_file = args[0];
        } else if !atty::is(atty::Stream::Stdin) {
            // isatty returns false if there's something in stdin.
            input_stdin = true;
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
        } else {
            eprintln!("Missing argument or Standar input. For more information try: --help");
            exit(1);
        }
    }

    // HOST
    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp.remove(0);
        &config.hosts[key]
    } else {
        eprintln!("No \"host\" found, check ~/.s3m/config.yml");
        exit(1);
    };

    // REGION
    let region = match &host.region {
        Some(h) => match h.parse::<Region>() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", e);
                exit(1);
            }
        },
        None => match &host.endpoint {
            Some(r) => Region::Custom {
                name: "".to_string(),
                endpoint: r.to_string(),
            },
            None => {
                eprintln!("Error parsing host need an endpoint or region");
                exit(1);
            }
        },
    };

    // BUCKET
    let bucket = if !hbp.is_empty() {
        Some(hbp.remove(0).to_string())
    } else if matches.subcommand_matches("ls").is_some() {
        None
    } else {
        eprintln!(
            "No \"bucket\" found, try: {} /path/to/file <s3 provider>/<bucket name>/file",
            me().unwrap_or_else(|| "s3m".to_string()),
        );
        exit(1);
    };

    // AUTH
    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    // S3
    let s3 = S3::new(&credentials, &region, bucket.clone());

    if matches.subcommand_matches("ls").is_some() {
        Ok((s3, Action::ListObjects(bucket)))
    } else {
        if hbp.is_empty() {
            eprintln!(
                "File name missing, try: {} {} <s3 provider>/<bucket>/{}",
                me().unwrap_or_else(|| "s3m".to_string()),
                input_file,
                input_file.split('/').next_back().unwrap_or("")
            );
            exit(1);
        }
        let chunk_size = buffer.parse::<u64>()?;
        Ok((
            s3,
            Action::PutObject {
                stdin: input_stdin,
                file: input_file.to_string(),
                key: hbp.join("/"),
                buffer: chunk_size,
                threads: threads,
            },
        ))
    }
}
