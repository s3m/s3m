use crate::cli::{
    actions::Action,
    commands, dispatch,
    globals::GlobalArgs,
    s3_location::{host_bucket_key, S3Location},
    Config, Host,
};
use crate::s3::{Credentials, S3};
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use secrecy::SecretString;
use std::{
    fs,
    path::{Path, PathBuf},
    process::exit,
};

pub fn get_config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().map_or_else(|| PathBuf::from("/tmp"), |h| h);

    let config_path = Path::new(&home_dir).join(".config").join("s3m");
    fs::create_dir_all(&config_path)
        .context(format!("unable to create: {}", &config_path.display()))?;

    Ok(config_path)
}

/// # Errors
/// Will return an error if the config file is not found
pub fn start() -> Result<(S3, Action, GlobalArgs)> {
    let config_path = get_config_path()?;

    // start the command line interface
    let cmd = commands::new(&config_path);

    // get the matches
    let matches = cmd.get_matches();

    let verbosity = matches.get_count("verbose");
    let verbosity_level = match verbosity {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    env_logger::Builder::new()
        .filter_level(verbosity_level)
        .init();

    log::info!("config path: {}", config_path.display());

    // handle option --clean
    // removes ~/.config/s3m/streams directory
    if matches.get_one::<bool>("clean").copied().unwrap_or(false) {
        let streams = config_path.join("streams");
        fs::remove_dir_all(streams).unwrap_or(());
        exit(0);
    }

    // define chunk size
    let mut buf_size: usize = matches
        .get_one::<usize>("buffer")
        .map_or(10_485_760, |size| *size);

    if buf_size < 5_242_880 {
        buf_size = 10_485_760;
    }

    log::info!("buffer size: {}", buf_size);

    // define global args
    let mut global_args = GlobalArgs::new();

    // define throttle
    if let Some(throttle) = matches.get_one::<usize>("throttle").filter(|&n| *n > 0) {
        global_args.throttle = Some(*throttle);

        log::info!("throttle bandwidth: {throttle}KB/s");
    }

    //  define retries
    let retries: usize = matches.get_one::<usize>("retries").map_or(3, |size| *size);
    global_args.set_retries(retries);

    // Config file is required
    let config_file: PathBuf = matches.get_one::<PathBuf>("config").map_or_else(
        || {
            eprintln!("no config file found");
            exit(1);
        },
        Into::into,
    );

    // load the config file
    let config = Config::new(config_file)?;

    log::debug!("config: {:#?}", config);

    // show config
    if matches.subcommand_matches("show").is_some() {
        println!("Hosts:");
        for key in config.hosts.keys() {
            println!("   - {key}");
        }
        exit(0);
    }

    // returns [host, bucket, key]
    // changes depending on the subcommand so need to check for each of them and then again to
    // create the action
    let s3_location = host_bucket_key(&matches)?;

    log::info!("host_buket_key: {s3_location:#?}");

    // HOST: get it from the config file
    let host = get_host(&config, &config_path, &s3_location)?;

    log::debug!("host: {host:#?}");

    // check if compression is enabled
    if host.compress.unwrap_or(false) {
        global_args.compress = true;
    }

    // check if encryption is enabled
    if let Some(enc_key) = &host.enc_key {
        if enc_key.len() != 32 {
            return Err(anyhow!("Encryption key must be 32 characters long."));
        }

        global_args.encrypt = true;
        global_args.enc_key = Some(SecretString::new(enc_key.clone().into()));
    }

    // REGION
    let region = host.get_region()?;

    // AUTH
    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    // sign or not the request ( --no-sign-request )
    let no_sign_request = matches
        .get_one::<bool>("no-sign-request")
        .copied()
        .unwrap_or(false);

    // create the S3 object
    let s3 = S3::new(
        &credentials,
        &region,
        s3_location.bucket.clone(),
        no_sign_request,
    );

    log::debug!("S3:\n{s3}");

    // create the action
    let action = dispatch::dispatch(
        s3_location,
        buf_size,
        config_path,
        &matches,
        &mut global_args,
    )?;

    log::debug!("globals: {:#?}, action: {:#?}", global_args, action);

    Ok((s3, action, global_args))
}

fn get_host<'a>(config: &'a Config, config_path: &Path, hbk: &'a S3Location) -> Result<&'a Host> {
    if hbk.host.is_empty() {
        return Err(anyhow!(
            "No \"host\" found, check config file {}/config.yml, For more information try {}",
            config_path.display(),
            "--help".green()
        ));
    }

    config.get_host(&hbk.host).map_err(|host_name| {
        anyhow!(
            "Could not find host: \"{}\". Check config file {}/config.yml, For more information try {}",
            host_name.to_string().red(),
            config_path.display(),
            "--help".green()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::new;
    use crate::s3::Region;
    use std::fs::File;
    use std::io::Write;
    use std::str::FromStr;
    use tempfile::TempDir;

    const CONF: &str = r#"---
hosts:
  aws:
    region: us-east-1
    access_key: XXX
    secret_key: YYY"#;

    #[test]
    fn test_get_config_path() {
        let config_path = get_config_path();
        assert!(config_path.is_ok());
    }

    #[test]
    fn test_get_host_from_hbk() {
        let tmp_dir = TempDir::new().unwrap();
        println!("tmp_dir: {:?}", tmp_dir.path());
        let config_path = tmp_dir.path().join("config.yml");
        let mut tmp_file = File::create(&config_path).unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec!["s3m", "ls", "aws/bucket/x"])
            .unwrap();
        let hbk = host_bucket_key(&matches).unwrap();
        assert_eq!(
            hbk,
            S3Location {
                host: String::from("aws"),
                bucket: Some(String::from("bucket")),
                key: Some(String::from("x")),
            }
        );
        let config = Config::new(config_path.clone()).unwrap();
        let host = get_host(&config, &config_path, &hbk);
        assert!(host.is_ok());

        let host = host.unwrap();
        assert_eq!(
            host.get_region().unwrap(),
            Region::from_str("us-east-1").unwrap()
        );
    }

    #[test]
    fn test_get_bucket_from_hbp_leading_slash() {
        let tmp_dir = TempDir::new().unwrap();
        println!("tmp_dir: {:?}", tmp_dir.path());
        let config_path = tmp_dir.path().join("config.yml");
        let mut tmp_file = File::create(&config_path).unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec!["s3m", "aws//bucket/x"])
            .unwrap();
        let hbp = host_bucket_key(&matches);
        assert!(hbp.is_err());
        let error = hbp.unwrap_err();
        assert!(error.to_string().contains("No \"bucket\" found"));
    }
}
