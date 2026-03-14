use crate::cli::{
    Config, Host,
    actions::Action,
    commands,
    decrypt::decrypt,
    dispatch,
    globals::GlobalArgs,
    s3_location::{S3Location, host_bucket_key},
};
use crate::s3::{Credentials, S3};
use anyhow::{Context, Result, anyhow};
use colored::Colorize;
use secrecy::SecretString;
use std::{
    fs,
    path::{Path, PathBuf},
    process::exit,
};

fn legacy_clean(config_path: &Path) -> Result<()> {
    crate::cli::actions::streams::clean_streams_state(config_path)?;
    Ok(())
}

pub fn get_config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));

    let config_path = Path::new(&home_dir).join(".config").join("s3m");
    fs::create_dir_all(&config_path)
        .context(format!("unable to create: {}", &config_path.display()))?;

    Ok(config_path)
}

/// # Errors
/// Will return an error if the config file is not found
#[allow(clippy::too_many_lines)]
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
    // cleans ~/.config/s3m/streams using the same logic as `s3m streams clean`
    if matches.get_one::<bool>("clean").copied().unwrap_or(false) {
        legacy_clean(&config_path).unwrap_or_default();
        exit(0);
    }

    if let Some([enc_path, enc_key]) = matches
        .get_many::<String>("decrypt")
        .map(std::iter::Iterator::collect::<Vec<_>>)
        .as_deref()
    {
        let enc_path = PathBuf::from(enc_path);

        decrypt(&enc_path, enc_key)?;

        exit(0);
    }

    // define chunk size
    let mut buf_size: usize = matches
        .get_one::<usize>("buffer")
        .map_or(10_485_760, |size| *size);

    if buf_size < 5_242_880 {
        buf_size = 10_485_760;
    }

    log::info!("buffer size: {buf_size}");

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
    let config = Config::new(config_file.clone())?;

    log::debug!("config: {config:#?}");

    if matches.subcommand_name() == Some("streams") {
        let action =
            dispatch::dispatch_streams(&matches, config_path.clone(), config_file.clone())?;
        return Ok((
            crate::cli::actions::streams::placeholder(),
            action,
            global_args,
        ));
    }

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
        config_path.clone(),
        &matches,
        &mut global_args,
    )?;
    let action = dispatch::finalize_action(action, &matches, &config, &config_path)?;

    log::debug!("globals: {global_args:#?}, action: {action:#?}");

    Ok((s3, action, global_args))
}

pub fn get_host<'a>(
    config: &'a Config,
    config_path: &Path,
    hbk: &'a S3Location,
) -> Result<&'a Host> {
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::unnecessary_wraps
)]
mod tests {
    use super::*;
    use crate::{
        cli::commands::new,
        s3::{Credentials, Region, S3},
        stream::{
            db::Db,
            state::{StreamMetadata, StreamMode, state_dir, write_metadata},
        },
    };
    use secrecy::SecretString;
    use std::{fs, fs::File, io::Write, str::FromStr};
    use tempfile::TempDir;

    const CONF: &str = r"---
hosts:
  aws:
    region: us-east-1
    access_key: XXX
    secret_key: YYY";

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
        assert!(
            error
                .to_string()
                .contains("Bucket name missing, expected format")
        );
    }

    #[test]
    fn test_legacy_clean_uses_stream_cleanup_logic() {
        let tmp_dir = TempDir::new().unwrap();
        let s3 = S3::new(
            &Credentials::new("AKIAIOSFODNN7EXAMPLE", &SecretString::new("secret".into())),
            &"us-east-1".parse::<Region>().unwrap(),
            Some("bucket".to_string()),
            false,
        );
        let db = Db::new(&s3, "key", "good", 1, tmp_dir.path()).unwrap();
        db.save_upload_id("upload-1").unwrap();
        db.create_part(1, 0, 10, None).unwrap();
        db.db_parts().unwrap().flush().unwrap();
        write_metadata(
            tmp_dir.path(),
            &StreamMetadata {
                version: 1,
                id: "good".to_string(),
                host: "aws".to_string(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                source_path: tmp_dir.path().join("source.bin"),
                checksum: "good".to_string(),
                file_size: 10,
                file_mtime: 1,
                part_size: 10,
                db_key: db.state_key().to_string(),
                created_at: 1,
                updated_at: Some(1),
                pipe: false,
                compress: false,
                encrypt: false,
                mode: StreamMode::FileMultipart,
            },
        )
        .unwrap();

        let broken_dir = state_dir(tmp_dir.path(), "broken");
        fs::create_dir_all(&broken_dir).unwrap();
        fs::write(broken_dir.join("state.yml"), "not: [valid").unwrap();

        legacy_clean(tmp_dir.path()).unwrap();

        assert!(state_dir(tmp_dir.path(), "good").exists());
        assert!(!state_dir(tmp_dir.path(), "broken").exists());
    }
}
