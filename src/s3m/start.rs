use crate::s3::{Credentials, S3};
use crate::s3m::{args::command, dispatch, matches, Config};
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

#[derive(Debug)]
pub enum Action {
    ACL {
        key: String,
        acl: Option<String>,
    },
    ListObjects {
        bucket: Option<String>,
        list_multipart_uploads: bool,
        prefix: Option<String>,
        start_after: Option<String>,
    },
    PutObject {
        acl: Option<String>,
        buf_size: usize,
        file: Option<String>,
        key: String,
        pipe: bool,
        s3m_dir: PathBuf,
        quiet: bool,
    },
    DeleteObject {
        key: String,
        upload_id: String,
    },
    GetObject {
        key: String,
        get_head: bool,
        dest: Option<String>,
        quiet: bool,
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

    let s3m_config = s3m_dir.join("config.yml");
    let after_help = format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/streams] use the option (--clean) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", s3m_dir.display());
    let help_clean = format!("remove {}/streams directory", s3m_dir.display());
    let cmd = command::new(s3m_config.as_os_str(), &after_help, &help_clean);
    let matches = cmd.get_matches();

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

    Ok((s3, action))
}
