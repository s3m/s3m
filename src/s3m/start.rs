use crate::s3::{Credentials, Region, S3};
use crate::s3m::{ArgParser, Config};
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::fs;
use std::path::{Path, PathBuf};
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
        key: String,
        quiet: bool,
        s3m_dir: PathBuf,
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
        dest: Option<String>,
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

pub fn start() -> Result<(S3, Action)> {
    let home_dir = match dirs::home_dir() {
        Some(h) => h,
        None => PathBuf::from("/tmp"),
    };

    // create ~/.s3m
    let s3m_dir = Path::new(&home_dir).join(".s3m");
    fs::create_dir_all(&s3m_dir).context("unable to create home dir ~/.s3m")?;

    let arg_parser = ArgParser::new(&s3m_dir);
    let matches = arg_parser.get_matches();

    // parse config file
    let config = matches.value_of("config").context("config file missing")?;
    let file = fs::File::open(config).context("unable to open file")?;
    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            return Err(anyhow!("could not parse the configuration file: {}", e));
        }
        Ok(yml) => yml,
    };

    // clean up ~/.streams options: --clean
    if matches.is_present("clean") {
        let streams = s3m_dir.join("streams");
        fs::remove_dir_all(&streams).unwrap_or_else(|_| ());
        exit(0);
    }

    let threads = matches
        .value_of("threads")
        .context("could not get threads")?
        .parse::<usize>()?;

    // Host, Bucket, Path
    let mut hbp: Vec<&str>;
    let input_stdin = !atty::is(atty::Stream::Stdin); // isatty returns false if there's something in stdin.
    let mut input_file: Option<String> = None;
    let mut dest: Option<String> = None;

    // If stdin use 512M (probably input is close to 5TB) if buffer is not defined
    let buf_size = if input_stdin && matches.occurrences_of("buffer") == 0 {
        STDIN_BUFF_SIZE
    } else {
        matches
            .value_of("buffer")
            .context("could not get buffer size")?
            .parse::<usize>()?
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
        if args.len() == 2 {
            dest = Some(args[1].to_string());
        }
    // DeleteObject
    } else if let Some(rm) = matches.subcommand_matches("rm") {
        let args: Vec<&str> = rm.values_of("arguments").unwrap_or_default().collect();
        hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
    } else {
        // PutObject
        let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
        if args.len() == 2 {
            hbp = args[1].split('/').filter(|s| !s.is_empty()).collect();
            input_file = Some(args[0].to_string());
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
            Ok((
                s3,
                Action::GetObject {
                    key,
                    get_head,
                    dest,
                },
            ))
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
                    file: input_file.unwrap(),
                    s3m_dir,
                    key,
                    quiet: matches.is_present("quiet"),
                    stdin: input_stdin,
                    threads,
                },
            ))
        }
    }
}
