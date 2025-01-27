use crate::cli::{actions::Action, commands, dispatch, globals::GlobalArgs, matches, Config, Host};
use crate::s3::{Credentials, S3};
use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use colored::Colorize;
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

/// # Errors
/// Will return an error if the config file is not found
pub fn start() -> Result<(S3, Action, GlobalArgs)> {
    let config_path = get_config_path()?;

    // start the command line interface
    let cmd = commands::new(&config_path);

    // get the matches
    let matches = cmd.get_matches();

    let verbosity_level =
        match matches
            .get_one::<u8>("verbose")
            .map_or(0, |&v| if v > 1 { 4 } else { v })
        {
            0 => log::LevelFilter::Off,
            1 => log::LevelFilter::Info,
            _ => log::LevelFilter::Debug,
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

        log::info!("throttle bandwidth: {}", format!("{throttle}KB/s"));
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

    // returns [host,bucket,path]
    // changes depending on the subcommand so need to check for each of them and then again to
    // create the action
    let mut hbp = matches::host_bucket_path(&matches)?;

    log::info!("hbp: {hbp:#?}");

    // HOST: get it from the config file
    let host = get_host_from_hbp(&config, &config_path, &mut hbp)?;

    // check if compression is enabled
    if host.compress.is_some() {
        global_args.compress = true;
    }

    // REGION
    let region = host.get_region()?;

    // BUCKET
    let bucket = get_bucket_from_hbp(&matches, &mut hbp)?;

    log::debug!("path: {hbp:#?}");

    // AUTH
    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    // sign or not the request ( --no-sign-request )
    let no_sign_request = matches
        .get_one::<bool>("no-sign-request")
        .copied()
        .unwrap_or(false);

    // create the S3 object
    let s3 = S3::new(&credentials, &region, bucket.clone(), no_sign_request);

    log::debug!("S3:\n{s3}");

    // create the action
    let action = dispatch::dispatch(
        hbp,
        bucket,
        buf_size,
        config_path,
        &matches,
        &mut global_args,
    )?;

    log::debug!("globals: {:#?}, action: {:#?}", global_args, action);

    Ok((s3, action, global_args))
}

fn get_host_from_hbp<'a>(
    config: &'a Config,
    config_path: &Path,
    hbp: &mut Vec<&str>,
) -> Result<&'a Host> {
    if hbp.is_empty() {
        Err(anyhow!(
            "No \"host\" found, check config file {}/config.yml, For more information try {}",
            config_path.display(),
            "--help".green()
        ))
    } else {
        config.get_host(hbp[0]).map_or_else(
            |h| {
                Err(anyhow!(
                    "Could not find host: \"{}\". Check config file {}/config.yml, For more information try {}",
                    h.to_string().red(),
                    config_path.display(),
                    "--help".green()
                ))
            },
            |h| {
                hbp.remove(0);
                Ok(h)
            },
        )
    }
}

fn get_bucket_from_hbp(matches: &ArgMatches, hbp: &mut Vec<&str>) -> Result<Option<String>> {
    if !hbp.is_empty() {
        if matches.subcommand_matches("cb").is_some() {
            Ok(Some(hbp[0].to_string()))
        } else {
            let bucket = hbp.remove(0).to_string();
            if bucket.is_empty() {
                Err(anyhow!(
                    "No \"bucket\" found, try: <s3 provider>/<bucket name>/path"
                ))
            } else {
                Ok(Some(bucket))
            }
        }
    } else if matches.subcommand_matches("ls").is_some() {
        Ok(None)
    } else {
        let delete_bucket = matches
            .subcommand_matches("rm")
            .is_some_and(|sub_m| sub_m.get_one("bucket").copied().unwrap_or(false));

        if matches.subcommand_matches("cb").is_some() || delete_bucket {
            return Err(anyhow!(
                "No \"bucket\" found, try: <s3 provider>/<bucket name>",
            ));
        }

        return Err(anyhow!(
            "No \"bucket\" found, try: {} /path/to/file <s3 provider>/<bucket name>/file",
            me().unwrap_or_else(|| "s3m".to_string()),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::new;
    use crate::cli::matches;
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
    fn test_get_host_from_hbp() {
        let tmp_dir = TempDir::new().unwrap();
        println!("tmp_dir: {:?}", tmp_dir.path());
        let config_path = tmp_dir.path().join("config.yml");
        let mut tmp_file = File::create(&config_path).unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let cmd = new(&tmp_dir.path().to_path_buf());
        let matches = cmd
            .try_get_matches_from(vec!["s3m", "ls", "aws/bucket/x"])
            .unwrap();
        let mut hbp = matches::host_bucket_path(&matches).unwrap();
        assert_eq!(hbp, vec!["aws", "bucket", "x"]);
        let config = Config::new(config_path.clone()).unwrap();
        let host = get_host_from_hbp(&config, &config_path, &mut hbp);
        assert!(host.is_ok());
        let host = host.unwrap();
        assert_eq!(hbp, vec!["bucket", "x"]);
        assert_eq!(
            host.get_region().unwrap(),
            Region::from_str("us-east-1").unwrap()
        );
        assert_eq!(
            get_bucket_from_hbp(&matches, &mut hbp).unwrap(),
            Some(String::from("bucket"))
        );
        assert_eq!(hbp, vec!["x"]);
    }

    #[test]
    fn test_get_bucket_from_hbp_leading_slash() {
        let tmp_dir = TempDir::new().unwrap();
        println!("tmp_dir: {:?}", tmp_dir.path());
        let config_path = tmp_dir.path().join("config.yml");
        let mut tmp_file = File::create(&config_path).unwrap();
        tmp_file.write_all(CONF.as_bytes()).unwrap();
        let cmd = new(&tmp_dir.path().to_path_buf());
        let matches = cmd
            .try_get_matches_from(vec!["s3m", "aws//bucket/x"])
            .unwrap();
        let mut hbp = matches::host_bucket_path(&matches).unwrap();
        assert_eq!(hbp, vec!["aws", "", "bucket", "x"]);
        let config = Config::new(config_path.clone()).unwrap();
        let host = get_host_from_hbp(&config, &config_path, &mut hbp);
        assert!(host.is_ok());
        let host = host.unwrap();
        assert_eq!(hbp, vec!["", "bucket", "x"]);
        assert_eq!(
            host.get_region().unwrap(),
            Region::from_str("us-east-1").unwrap()
        );
        assert!(get_bucket_from_hbp(&matches, &mut hbp).is_err());
        assert_eq!(hbp, vec!["bucket", "x"]);
    }
}
