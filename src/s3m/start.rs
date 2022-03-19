use crate::s3::{Credentials, S3};
use crate::s3m::{args::ArgParser, dispatch, matches, Config};
use anyhow::{anyhow, Context, Result};
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
    let mut buf_size: usize = matches.value_of_t_or_exit("buffer");

    if buf_size < 5_242_880 {
        buf_size = 10_485_760;
    }

    // returns [host,bucket,path]
    // changes depending on the subcommand so need to check for each of them and then again to
    // create the action
    let mut hbp = matches::host_bucket_path(&matches)?;

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

    // create the action
    let action = dispatch::dispatch(hbp, bucket, buf_size, s3m_dir, &matches)?;

    Ok((s3, action, matches.is_present("quiet")))
}
