use crate::cli::{
    Config, Host,
    actions::Action,
    commands,
    decrypt::decrypt,
    dispatch,
    globals::GlobalArgs,
    s3_location::{S3Location, host_bucket_key},
};
use crate::s3::{Credentials, Region, S3};
use anyhow::{Result, anyhow};
use clap::ArgMatches;
use colored::Colorize;
use secrecy::{ExposeSecret, SecretString};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::exit,
};

fn legacy_clean(config_path: &Path) -> Result<()> {
    crate::cli::actions::streams::clean_streams_state(config_path)?;
    Ok(())
}

fn placeholder_s3() -> S3 {
    S3::new(
        &Credentials::new("", &SecretString::new("".into())),
        &Region::aws("us-east-1"),
        None,
        true,
    )
}

fn init_logger(matches: &ArgMatches) {
    let verbosity_level = match matches.get_count("verbose") {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    env_logger::Builder::new()
        .filter_level(verbosity_level)
        .init();
}

fn handle_early_exit_commands(matches: &ArgMatches, config_path: &Path) -> Result<()> {
    if matches.get_one::<bool>("clean").copied().unwrap_or(false) {
        legacy_clean(config_path)?;
        exit(0);
    }

    if let Some([enc_path, enc_key]) = matches
        .get_many::<String>("decrypt")
        .map(std::iter::Iterator::collect::<Vec<_>>)
        .as_deref()
    {
        decrypt(&PathBuf::from(enc_path), enc_key)?;
        exit(0);
    }

    Ok(())
}

fn resolve_buffer_size(matches: &ArgMatches) -> usize {
    let mut buf_size = matches
        .get_one::<usize>("buffer")
        .map_or(10_485_760, |size| *size);

    if buf_size < 5_242_880 {
        buf_size = 10_485_760;
    }

    buf_size
}

fn build_global_args(matches: &ArgMatches) -> GlobalArgs {
    let mut global_args = GlobalArgs::new();

    if let Some(throttle) = matches.get_one::<usize>("throttle").filter(|&n| *n > 0) {
        global_args.throttle = Some(*throttle);
        log::info!("throttle bandwidth: {throttle}KB/s");
    }

    // clap bounds --retries to u32; set_retries keeps at least one attempt.
    if let Some(&retries) = matches.get_one::<u32>("retries") {
        global_args.set_retries(usize::try_from(retries).unwrap_or(usize::MAX));
    }

    global_args
}

fn resolve_config_file(matches: &ArgMatches, config_path: &Path) -> PathBuf {
    matches
        .get_one::<PathBuf>("config")
        .cloned()
        .unwrap_or_else(|| config_path.join("config.yml"))
}

fn handle_config_only_subcommands(
    matches: &ArgMatches,
    config_path: &Path,
    config_file: &Path,
    config: &Config,
) -> Result<Option<Action>> {
    if matches.subcommand_name() == Some("streams") {
        let action = dispatch::dispatch_streams(
            matches,
            config_path.to_path_buf(),
            config_file.to_path_buf(),
        )?;
        return Ok(Some(action));
    }

    if matches.subcommand_matches("show").is_some() {
        println!("Hosts:");
        for key in config.hosts.keys() {
            println!("   - {key}");
        }
        exit(0);
    }

    Ok(None)
}

fn apply_host_defaults(host: &Host, global_args: &mut GlobalArgs) -> Result<()> {
    if host.compress.unwrap_or(false) {
        global_args.compress = true;
    }

    if let Some(enc_key) = &host.enc_key {
        if enc_key.expose_secret().len() != 32 {
            return Err(anyhow!("Encryption key must be 32 characters long."));
        }

        global_args.encrypt = true;
        global_args.enc_key = Some(enc_key.clone());
    }

    Ok(())
}

fn build_s3(matches: &ArgMatches, host: &Host, bucket: Option<String>) -> Result<S3> {
    let region = host.get_region()?;
    let credentials = Credentials::new(&host.access_key, &host.secret_key);
    let no_sign_request = matches
        .get_one::<bool>("no-sign-request")
        .copied()
        .unwrap_or(false);

    Ok(S3::new(&credentials, &region, bucket, no_sign_request))
}

fn resolve_s3_and_action(
    matches: &ArgMatches,
    config_path: &Path,
    config: &Config,
    buf_size: usize,
    global_args: &mut GlobalArgs,
) -> Result<(S3, Action)> {
    let s3_location = host_bucket_key(matches)?;
    log::info!("host_buket_key: {s3_location:#?}");

    let host = get_host(config, config_path, &s3_location)?;
    log::debug!("host: {host:#?}");
    if let Some(warning) = host.ignored_bucket_warning(&s3_location.host) {
        eprintln!("warning: {warning}");
    }

    apply_host_defaults(host, global_args)?;
    let s3 = build_s3(matches, host, s3_location.bucket.clone())?;
    log::debug!("S3:\n{s3}");

    let action = dispatch::dispatch(&s3_location, buf_size, config_path, matches, global_args)?;
    let action = dispatch::finalize_action(action, matches, config, config_path)?;

    Ok((s3, action))
}

/// Environment variable that replaces `~/.config/s3m` as the directory holding the default
/// `config.yml` and the local upload state (`streams/`).
pub const S3M_HOME_ENV: &str = "S3M_HOME";

fn config_dir() -> PathBuf {
    config_dir_from(std::env::var_os(S3M_HOME_ENV), dirs::home_dir())
}

/// A non-empty `S3M_HOME` wins; otherwise `<home>/.config/s3m`, with the temp directory
/// standing in for a missing home.
fn config_dir_from(s3m_home: Option<OsString>, home: Option<PathBuf>) -> PathBuf {
    if let Some(dir) = s3m_home.filter(|dir| !dir.is_empty()) {
        return PathBuf::from(dir);
    }

    home.unwrap_or_else(std::env::temp_dir)
        .join(".config")
        .join("s3m")
}

/// # Errors
/// Will return an error if the config file is not found
pub fn start() -> Result<(S3, Action, GlobalArgs)> {
    // --help and --version exit inside get_matches, before anything is written.
    // Nothing is created here: a read-only home must not stop `ls`, `get` or `rm`. The
    // upload state directories are created when an upload first writes to them.
    let config_path = config_dir();
    let matches = commands::new(&config_path).get_matches();

    init_logger(&matches);

    log::info!("config path: {}", config_path.display());
    handle_early_exit_commands(&matches, &config_path)?;

    let buf_size = resolve_buffer_size(&matches);
    log::info!("buffer size: {buf_size}");

    let mut global_args = build_global_args(&matches);
    let config_file = resolve_config_file(&matches, &config_path);
    let config = Config::new(config_file.clone())?;

    log::debug!("config: {config:#?}");

    if let Some(action) =
        handle_config_only_subcommands(&matches, &config_path, &config_file, &config)?
    {
        return Ok((placeholder_s3(), action, global_args));
    }

    let (s3, action) =
        resolve_s3_and_action(&matches, &config_path, &config, buf_size, &mut global_args)?;
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
    fn test_config_dir_defaults_to_home_config() {
        assert_eq!(
            config_dir_from(None, Some(PathBuf::from("/home/u"))),
            PathBuf::from("/home/u/.config/s3m")
        );
    }

    #[test]
    fn test_config_dir_honours_s3m_home() {
        assert_eq!(
            config_dir_from(
                Some(OsString::from("/var/lib/s3m")),
                Some(PathBuf::from("/home/u"))
            ),
            PathBuf::from("/var/lib/s3m")
        );
    }

    #[test]
    fn test_config_dir_ignores_empty_s3m_home() {
        assert_eq!(
            config_dir_from(Some(OsString::new()), Some(PathBuf::from("/home/u"))),
            PathBuf::from("/home/u/.config/s3m")
        );
    }

    #[test]
    fn test_config_dir_without_home_uses_temp_dir() {
        assert_eq!(
            config_dir_from(None, None),
            std::env::temp_dir().join(".config").join("s3m")
        );
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
