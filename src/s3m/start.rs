use crate::s3::{Credentials, S3};
use crate::s3m::{args::ArgParser, matches, Config};
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

#[derive(Debug)]
pub enum Action {
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
    },
    PutObject {
        attr: String,
        buf_size: usize,
        file: Option<String>,
        key: String,
        pipe: bool,
        s3m_dir: PathBuf,
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

pub fn start() -> Result<(S3, Action, bool)> {
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
        fs::remove_dir_all(&streams).unwrap_or(());
        exit(0);
    }

    // define chunk size
    let mut buf_size = matches
        .value_of("buffer")
        .unwrap()
        .parse::<usize>()
        .unwrap_or(10_485_760);

    if buf_size < 5_242_880 {
        buf_size = 5_242_880;
    }

    let (mut hbp, src, dest) = matches::hbp_src_dest(&matches)?;

    // HOST
    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp.remove(0);
        &config.hosts[key]
    } else {
        return Err(anyhow!("no \"host\" found, check ~/.s3m/config.yml"));
    };

    // REGION
    let region = matches::get_region(host)?;

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
            matches.is_present("quiet"),
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
                matches.is_present("quiet"),
            ))
        // ShareObject
        } else if let Some(sub_m) = matches.subcommand_matches("share") {
            let expire = sub_m.value_of("expire").unwrap().parse::<usize>()?;
            Ok((
                s3,
                Action::ShareObject { key, expire },
                matches.is_present("quiet"),
            ))
        // DeleteObject
        } else if let Some(sub_m) = matches.subcommand_matches("rm") {
            let upload_id = sub_m.value_of("UploadId").unwrap_or_default().to_string();
            Ok((
                s3,
                Action::DeleteObject { key, upload_id },
                matches.is_present("quiet"),
            ))
        // PutObject
        } else {
            Ok((
                s3,
                Action::PutObject {
                    attr: matches.value_of("attr").unwrap_or_default().to_string(),
                    buf_size,
                    file: src,
                    s3m_dir,
                    key,
                    pipe: matches.is_present("pipe"),
                },
                matches.is_present("quiet"),
            ))
        }
    }
}
