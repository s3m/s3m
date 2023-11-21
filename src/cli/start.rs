use crate::cli::{actions::Action, commands, dispatch, matches, Config};
use crate::s3::{Credentials, S3};
use anyhow::{anyhow, Context, Result};
use std::{fs, path::PathBuf, process::exit};

fn me() -> Option<String> {
    std::env::current_exe()
        .ok()?
        .file_name()?
        .to_str()?
        .to_owned()
        .into()
}

pub fn start() -> Result<(S3, Action)> {
    let cmd = commands::new();

    let matches = cmd.get_matches();

    // Config file is required
    let config_file: PathBuf = matches
        .get_one::<PathBuf>("config")
        .map(|c| c.into())
        .unwrap_or_else(|| {
            eprintln!("no config file found");
            exit(1);
        });

    let config_path = config_file.parent().unwrap_or_else(|| {
        eprintln!("no config file found");
        exit(1);
    });

    let file = fs::File::open(&config_file).context("unable to open file")?;

    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            return Err(anyhow!("could not parse the configuration file: {}", e));
        }
        Ok(yml) => yml,
    };

    // handle option --clean
    // removes ~/.config/s3m/streams directory
    if matches.get_one::<bool>("clean").copied().unwrap_or(false) {
        let streams = config_path.join("streams");
        fs::remove_dir_all(streams).unwrap_or(());
        exit(0);
    }

    // define chunk size
    let mut buf_size: usize = *matches
        .get_one::<usize>("buffer")
        .expect("no buffer size found");

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
        if matches.subcommand_matches("mb").is_some() {
            Some(hbp[0].to_string())
        } else {
            Some(hbp.remove(0).to_string())
        }
    } else if matches.subcommand_matches("ls").is_some() {
        None
    } else {
        if matches.subcommand_matches("mb").is_some() {
            return Err(anyhow!(
                "no \"bucket\" found, try: <s3 provider>/<bucket name>",
            ));
        }
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
    let action = dispatch::dispatch(hbp, bucket, buf_size, config_path.join("streams"), &matches)?;

    Ok((s3, action))
}
