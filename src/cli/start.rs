use crate::cli::{actions::Action, commands, dispatch, matches, Config, Host};
use crate::s3::{Credentials, S3};
use anyhow::{anyhow, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::exit,
};

fn me() -> Option<String> {
    std::env::current_exe()
        .ok()?
        .file_name()?
        .to_str()?
        .to_owned()
        .into()
}

pub fn get_config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().map_or_else(|| PathBuf::from("/tmp"), |h| h);

    let config_path = Path::new(&home_dir).join(".config").join("s3m");
    fs::create_dir_all(&config_path)
        .context(format!("unable to create: {}", &config_path.display()))?;

    Ok(config_path)
}

pub fn start() -> Result<(S3, Action)> {
    let config_path = get_config_path()?;

    // check if config file exists
    if let Err(e) = fs::File::open(config_path.join("config.yml")) {
        eprintln!(
            "Config file {}/config.yml is required, see --help for more info:\n{}",
            config_path.display(),
            e
        );
        exit(1);
    }

    // start the command line interface
    let cmd = commands::new(&config_path);

    // get the matches
    let matches = cmd.get_matches();

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

    // Config file is required
    let config_file: PathBuf = matches
        .get_one::<PathBuf>("config")
        .map(std::convert::Into::into)
        .unwrap_or_else(|| {
            eprintln!("no config file found");
            exit(1);
        });

    // load the config file
    let config = Config::new(config_file)?;

    // returns [host,bucket,path]
    // changes depending on the subcommand so need to check for each of them and then again to
    // create the action
    let mut hbp = matches::host_bucket_path(&matches)?;

    // HOST: get it from the config file
    let host: &Host = match config.get_host(&hbp[0]) {
        Ok(h) => {
            hbp.remove(0);
            h
        }
        Err(e) => {
            return Err(anyhow!(
                "No \"host\" found, check config file {}/config.yml: {}",
                config_path.display(),
                e
            ));
        }
    };

    // REGION
    let region = host.get_region()?;

    // BUCKET
    let bucket = if !hbp.is_empty() {
        if matches.subcommand_matches("cb").is_some() {
            Some(hbp[0].to_string())
        } else {
            Some(hbp.remove(0).to_string())
        }
    } else if matches.subcommand_matches("ls").is_some() {
        None
    } else {
        if matches.subcommand_matches("cb").is_some() {
            return Err(anyhow!(
                "No \"bucket\" found, try: <s3 provider>/<bucket name>",
            ));
        }
        return Err(anyhow!(
            "No \"bucket\" found, try: {} /path/to/file <s3 provider>/<bucket name>/file",
            me().unwrap_or_else(|| "s3m".to_string()),
        ));
    };

    // AUTH
    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    // S3
    let s3 = S3::new(&credentials, &region, bucket.clone());

    // create the action
    let action = dispatch::dispatch(hbp, bucket, buf_size, config_path, &matches)?;

    Ok((s3, action))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_config_path() {
        let config_path = get_config_path();
        assert!(config_path.is_ok());
    }
}
