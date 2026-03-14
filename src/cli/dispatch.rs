use crate::{
    cli::{
        Config,
        actions::{Action, DeleteGroup, DuGroupBy, StreamCommand},
        age_filter::AgeFilter,
        globals::GlobalArgs,
        s3_location::{S3Location, parse_location},
        start::get_host,
    },
    s3::{Credentials, S3, actions::ObjectIdentifier},
};
use anyhow::{Context, Result, anyhow};
use colored::Colorize;
use std::{
    borrow::ToOwned,
    cmp,
    collections::BTreeMap,
    path::{Path, PathBuf},
    string::String,
};

/// # Errors
/// Will return `Err` if the streams subcommand arguments are invalid
pub fn dispatch_streams(
    matches: &clap::ArgMatches,
    s3m_dir: PathBuf,
    config_file: PathBuf,
) -> Result<Action> {
    let streams = matches
        .subcommand_matches("streams")
        .context("streams arguments missing")?;

    let command = match streams.subcommand() {
        None | Some(("ls", _)) => StreamCommand::List,
        Some(("show", sub_m)) => StreamCommand::Show {
            id: sub_m
                .get_one::<String>("id")
                .cloned()
                .context("id missing")?,
        },
        Some(("resume", sub_m)) => StreamCommand::Resume {
            id: sub_m
                .get_one::<String>("id")
                .cloned()
                .context("id missing")?,
        },
        Some(("clean", _)) => StreamCommand::Clean,
        Some((other, _)) => return Err(anyhow!("unsupported streams subcommand: {other}")),
    };

    let json = streams.get_one::<bool>("json").copied().unwrap_or(false);
    if json && matches!(command, StreamCommand::Resume { .. }) {
        return Err(anyhow!("--json is not supported with `s3m streams resume`"));
    }

    Ok(Action::Streams {
        command,
        config_file,
        json,
        s3m_dir,
        number: matches.get_one::<u8>("number").copied().unwrap_or_else(|| {
            u8::try_from(cmp::min(
                (num_cpus::get_physical() - 2).max(1),
                u8::MAX as usize,
            ))
            .unwrap_or(u8::MAX)
        }),
    })
}

// return Action based on the command or subcommand
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn dispatch(
    hbk: S3Location,
    buf_size: usize,
    s3m_dir: PathBuf,
    matches: &clap::ArgMatches,
    global_args: &mut GlobalArgs,
) -> Result<Action> {
    // Closure to return subcommand_matches
    let sub_m = |subcommand| -> Result<&clap::ArgMatches> {
        matches
            .subcommand_matches(subcommand)
            .context("arguments missing")
    };

    let get_key = || -> Result<String> {
        match &hbk.key {
            Some(key) => Ok(key.clone()),

            None => Err(anyhow!(
                "file name missing, <s3 provider>/<bucket>/{}, For more information try {}",
                "<file name>".red(),
                "--help".green()
            )),
        }
    };

    let resolve_prefix = |sub_m: &clap::ArgMatches| -> Result<Option<String>> {
        let flag_prefix = sub_m.get_one::<String>("prefix").cloned();
        match (hbk.key.clone(), flag_prefix) {
            (Some(_), Some(_)) => Err(anyhow!(
                "Prefix provided twice. Use either host/bucket/prefix or --prefix, not both"
            )),
            (Some(prefix), None) | (None, Some(prefix)) => Ok(Some(prefix)),
            (None, None) => Ok(None),
        }
    };

    match matches.subcommand_name() {
        // ACL
        Some("acl") => {
            let key = get_key()?;

            let sub_m = sub_m("acl")?;

            let acl = sub_m.get_one("acl").cloned();

            Ok(Action::ACL { key, acl })
        }

        // GetObject
        Some("get") => {
            let key = get_key()?;

            let sub_m = sub_m("get")?;

            let metadata = sub_m.get_one("metadata").copied().unwrap_or(false);

            let args: Vec<&str> = sub_m
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(String::as_str)
                .collect();

            let quiet = sub_m.get_one("quiet").copied().unwrap_or(false);

            let force = sub_m.get_one("force").copied().unwrap_or(false);

            let json = sub_m.get_one("json").copied().unwrap_or(false);
            let versions = sub_m.get_one("versions").copied().unwrap_or(false);

            let version = sub_m.get_one("version").cloned();

            if json && !metadata && !versions {
                return Err(anyhow!(
                    "--json is only supported with `s3m get --meta` or `s3m get --versions`"
                ));
            }

            // get destination file/path
            let dest = if args.len() == 2 {
                args.get(1).map(|s| (*s).to_string())
            } else {
                None
            };

            Ok(Action::GetObject {
                dest,
                force,
                json,
                key,
                metadata,
                quiet,
                versions,
                version,
            })
        }

        // DiskUsage
        Some("du") => {
            let bucket = hbk
                .bucket
                .clone()
                .context("bucket name missing for du target")?;
            let prefix = hbk.key.clone();
            let group_by =
                sub_m("du")?
                    .get_one::<String>("group-by")
                    .map(|value| match value.as_str() {
                        "day" => DuGroupBy::Day,
                        _ => unreachable!("clap validated group-by"),
                    });
            let json = sub_m("du")?.get_one("json").copied().unwrap_or(false);
            let target = prefix.as_ref().map_or_else(
                || format!("{}/{}", hbk.host, bucket),
                |prefix| format!("{}/{}/{}", hbk.host, bucket, prefix),
            );

            Ok(Action::DiskUsage {
                group_by,
                json,
                prefix,
                target,
            })
        }

        // ListObjects
        Some("ls") => {
            let sub_m = sub_m("ls")?;

            let older_than = sub_m.get_one::<AgeFilter>("older-than").copied();
            let prefix = resolve_prefix(sub_m)?;
            let start_after = sub_m.get_one("start-after").cloned();
            let json = sub_m.get_one("json").copied().unwrap_or(false);

            // option -n/--number
            // convert max_keys to string and default to None
            let max_kub = sub_m
                .get_one::<usize>("max-kub")
                .map(std::string::ToString::to_string);

            if older_than.is_some() && hbk.bucket.is_none() {
                return Err(anyhow!(
                    "--older-than requires a bucket or prefix target, for example s3/my-bucket or s3/my-bucket/prefix"
                ));
            }

            Ok(Action::ListObjects {
                bucket: hbk.bucket.clone(),
                json,
                list_multipart_uploads: sub_m
                    .get_one("ListMultipartUploads")
                    .copied()
                    .unwrap_or(false),
                max_kub,
                older_than,
                prefix,
                start_after,
            })
        }

        // CreateBucket
        Some("cb") => {
            let sub_m = sub_m("cb")?;
            let acl = sub_m
                .get_one("acl")
                .map_or_else(|| String::from("private"), |s: &String| s.clone());
            Ok(Action::CreateBucket { acl })
        }

        // DeleteObject or DeleteBucket
        Some("rm") => {
            let sub_m = sub_m("rm")?;
            let older_than = sub_m.get_one::<AgeFilter>("older-than").copied();

            let upload_id = sub_m
                .get_one("UploadId")
                .map_or_else(String::new, |s: &String| s.clone());

            let bucket = sub_m.get_one("bucket").copied().unwrap_or(false);
            let recursive = sub_m.get_one("recursive").copied().unwrap_or(false);
            let key = if bucket {
                String::new()
            } else if older_than.is_some() {
                if sub_m
                    .get_many::<String>("arguments")
                    .unwrap_or_default()
                    .count()
                    != 1
                {
                    return Err(anyhow!(
                        "--older-than expects exactly one bucket or prefix target"
                    ));
                }
                hbk.key.clone().unwrap_or_default()
            } else {
                get_key()?
            };

            if bucket && older_than.is_some() {
                return Err(anyhow!(
                    "--older-than is not supported with bucket deletion"
                ));
            }

            if !upload_id.is_empty() && older_than.is_some() {
                return Err(anyhow!("--older-than is not supported with --abort"));
            }

            Ok(Action::DeleteObject {
                key,
                upload_id,
                bucket,
                older_than,
                recursive,
                targets: Vec::new(),
            })
        }

        // ShareObject
        Some("share") => {
            let key = get_key()?;

            let sub_m = sub_m("share")?;

            let expire = sub_m.get_one::<usize>("expire").map_or_else(|| 0, |s| *s);

            Ok(Action::ShareObject { key, expire })
        }

        // PutObject
        _ => {
            let mut src: Option<String> = None;

            let args: Vec<&str> = matches
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(String::as_str)
                .collect();

            if args.len() == 2
                && let Some(arg) = args.first()
            {
                src = Some((*arg).to_string());

                // if src is provided, check if it exists
                if !Path::new(arg).exists() {
                    return Err(anyhow!("Source file does not exist: {}", arg.red()));
                }
            }

            log::info!(
                "Arguments: {:?}, Source file: {:?}",
                args,
                src.as_deref().unwrap_or(""),
            );

            let key = match get_key() {
                Ok(k) => k,
                Err(e) => {
                    if let Some(src) = &src {
                        src.clone()
                    } else {
                        return Err(e);
                    }
                }
            };

            log::info!("Key: {key}");

            // get ACL to apply to the object
            let acl = matches.get_one("acl").cloned();

            // get x-amz-meta- to apply to the object
            let meta = if let Some(meta_str) = matches.get_one::<String>("meta") {
                let mut metadata = BTreeMap::new();
                for item in meta_str.split(';') {
                    match item.split_once('=') {
                        Some((key, val)) => {
                            metadata.insert(format!("x-amz-meta-{key}"), val.to_owned());
                        }
                        None => {
                            return Err(anyhow!(
                                "Invalid metadata format: '{item}'. Expected 'key=value' pairs separated by ';'"
                            ));
                        }
                    }
                }
                Some(metadata)
            } else {
                None
            };

            // set compress
            if !global_args.compress {
                global_args.compress = matches.get_one("compress").copied().unwrap_or(false);
            }

            let pipe = matches.get_one("pipe").copied().unwrap_or(false);

            // Validate that we have either a source file or pipe mode enabled
            if src.is_none() && !pipe {
                return Err(anyhow!(
                    "Source file missing. Expected: {} {} {}\nFor more information try {}",
                    "<source file>".red(),
                    "<s3 provider>/<bucket>/<file name>".cyan(),
                    "[OPTIONS]".yellow(),
                    "--help".green()
                ));
            }

            Ok(Action::PutObject {
                acl,
                meta,
                buf_size,
                file: src,
                host: hbk.host,
                s3m_dir,
                key,
                pipe,
                quiet: matches.get_one("quiet").copied().unwrap_or(false),
                tmp_dir: matches.get_one::<PathBuf>("tmp-dir").map_or_else(
                    || std::env::temp_dir().join(format!("s3m-{}", std::process::id())),
                    ToOwned::to_owned,
                ),
                checksum_algorithm: matches.get_one("checksum").cloned(),
                number: matches.get_one::<u8>("number").copied().unwrap_or(1),
            })
        }
    }
}

/// # Errors
/// Will return an error if delete arguments are invalid or can not be resolved to hosts
pub fn finalize_action(
    action: Action,
    matches: &clap::ArgMatches,
    config: &Config,
    config_path: &Path,
) -> Result<Action> {
    let Action::DeleteObject {
        bucket,
        key,
        older_than,
        recursive,
        targets: _,
        upload_id,
    } = action
    else {
        return Ok(action);
    };

    if matches.subcommand_name() != Some("rm") {
        return Ok(Action::DeleteObject {
            bucket,
            key,
            older_than,
            recursive,
            targets: Vec::new(),
            upload_id,
        });
    }

    let sub_m = matches
        .subcommand_matches("rm")
        .context("Subcommand arguments missing")?;
    let args: Vec<&str> = sub_m
        .get_many::<String>("arguments")
        .unwrap_or_default()
        .map(String::as_str)
        .collect();

    if bucket {
        if args.len() != 1 {
            return Err(anyhow!("Bucket deletion expects exactly one bucket target"));
        }

        return Ok(Action::DeleteObject {
            bucket,
            key,
            older_than,
            recursive,
            targets: Vec::new(),
            upload_id,
        });
    }

    if !upload_id.is_empty() {
        if args.len() != 1 {
            return Err(anyhow!(
                "Abort multipart upload expects exactly one object target"
            ));
        }

        return Ok(Action::DeleteObject {
            bucket,
            key,
            older_than,
            recursive,
            targets: Vec::new(),
            upload_id,
        });
    }

    if older_than.is_some() {
        if args.len() != 1 {
            return Err(anyhow!(
                "--older-than expects exactly one bucket or prefix target"
            ));
        }

        return Ok(Action::DeleteObject {
            bucket,
            key,
            older_than,
            recursive,
            targets: Vec::new(),
            upload_id,
        });
    }

    let no_sign_request = matches
        .get_one::<bool>("no-sign-request")
        .copied()
        .unwrap_or(false);

    Ok(Action::DeleteObject {
        bucket,
        key,
        older_than,
        recursive,
        targets: build_delete_groups(&args, config, config_path, no_sign_request)?,
        upload_id,
    })
}

fn build_delete_groups(
    args: &[&str],
    config: &Config,
    config_path: &Path,
    no_sign_request: bool,
) -> Result<Vec<DeleteGroup>> {
    let mut groups: BTreeMap<(String, String), DeleteGroup> = BTreeMap::new();

    for arg in args {
        let location = parse_location(arg, false, false)?;
        let bucket = location.bucket.clone().ok_or_else(|| {
            anyhow!(
                "Bucket name missing, expected format: <s3 provider>/<bucket name>/<object key>"
            )
        })?;
        let key = location.key.clone().ok_or_else(|| {
            anyhow!("Object target missing key: {arg}. Expected <s3 provider>/<bucket>/<key>")
        })?;

        let group_key = (location.host.clone(), bucket.clone());

        if let Some(group) = groups.get_mut(&group_key) {
            group.objects.push(ObjectIdentifier {
                key,
                version_id: None,
            });
            continue;
        }

        let host = get_host(config, config_path, &location)?;
        let region = host.get_region()?;
        let credentials = Credentials::new(&host.access_key, &host.secret_key);

        groups.insert(
            group_key,
            DeleteGroup {
                objects: vec![ObjectIdentifier {
                    key,
                    version_id: None,
                }],
                s3: S3::new(&credentials, &region, Some(bucket), no_sign_request),
            },
        );
    }

    Ok(groups.into_values().collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::cli::{
        actions::Action,
        commands::{cmd_acl, cmd_cb, cmd_du, cmd_get, cmd_ls, cmd_rm, cmd_share, new},
        config::Config,
        globals::GlobalArgs,
        s3_location::host_bucket_key,
        start::get_host,
    };
    use clap::Command;
    use std::fs::File;
    use std::io::Write;
    use tempfile::Builder;

    const CONF: &str = r"---
hosts:
  s3:
    region: us-east-1
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket
  s3alt:
    endpoint: alt.example.test
    access_key: ALT
    secret_key: ZZZ";

    fn write_config_file() -> (tempfile::TempDir, PathBuf, Config) {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let parsed = Config::new(config_path.clone()).unwrap();

        (tmp_dir, config_path, parsed)
    }

    #[test]
    fn test_dispatch_bad_bucket() {
        let cmd = Command::new("test").subcommand(cmd_acl::command());

        let matches = cmd.try_get_matches_from(vec!["test", "acl", "s3/b/key"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_err());

        let err = s3_location.unwrap_err().to_string();

        assert!(err.contains("Invalid bucket name"));
        assert!(err.contains("Must be 3-63"));
    }

    #[test]
    fn test_dispatch_acl() {
        let cmd = Command::new("test").subcommand(cmd_acl::command());

        let matches = cmd.try_get_matches_from(vec!["test", "acl", "s3/bucket/key"]);
        assert!(matches.is_ok());

        let mut globals = GlobalArgs::new();

        let matches = matches.unwrap();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();

        match action {
            Action::ACL { key, acl } => {
                assert_eq!(key, "key");
                assert_eq!(acl, None);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_acl_put() {
        let cmd = Command::new("test").subcommand(cmd_acl::command());

        let matches =
            cmd.try_get_matches_from(vec!["test", "acl", "-a", "private", "s3/bucket/key"]);
        assert!(matches.is_ok());

        let mut globals = GlobalArgs::new();

        let matches = matches.unwrap();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();

        match action {
            Action::ACL { key, acl } => {
                assert_eq!(key, "key");
                assert_eq!(acl, Some("private".to_string()));
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_get_1() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd.try_get_matches_from(vec!["test", "get", "h/bucket/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::GetObject {
                key,
                metadata,
                dest,
                quiet,
                force,
                json,
                versions,
                version,
            } => {
                assert_eq!(key, "f");
                assert!(!metadata);
                assert_eq!(dest, None);
                assert!(!quiet);
                assert!(!force);
                assert!(!json);
                assert!(!versions);
                assert_eq!(version, None);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_get_2() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd.try_get_matches_from(vec!["test", "get", "h/bucket/f", "tmp/file"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::GetObject {
                key,
                metadata,
                dest,
                quiet,
                force,
                json,
                versions,
                version,
            } => {
                assert_eq!(key, "f");
                assert!(!metadata);
                assert_eq!(dest, Some("tmp/file".to_string()));
                assert!(!quiet);
                assert!(!force);
                assert!(!json);
                assert!(!versions);
                assert_eq!(version, None);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_get_quiet_force() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd.try_get_matches_from(vec!["test", "get", "h/bucket/key", "-q", "-f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::GetObject {
                key,
                metadata,
                dest,
                quiet,
                force,
                json,
                versions,
                version,
            } => {
                assert_eq!(key, "key");
                assert!(!metadata);
                assert_eq!(dest, None);
                assert!(quiet);
                assert!(force);
                assert!(!json);
                assert!(!versions);
                assert_eq!(version, None);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_ls() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());

        let matches = cmd.try_get_matches_from(vec!["test", "ls", "h/bucket/file"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::ListObjects {
                bucket,
                json,
                list_multipart_uploads,
                max_kub,
                older_than,
                prefix,
                start_after,
            } => {
                assert_eq!(bucket, Some("bucket".to_string()));
                assert!(!json);
                assert!(!list_multipart_uploads);
                assert_eq!(older_than, None);
                assert_eq!(prefix, Some("file".to_string()));
                assert_eq!(start_after, None);
                assert_eq!(max_kub, None);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_du_bucket() {
        let cmd = Command::new("test").subcommand(cmd_du::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "du", "s3/my-bucket"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::DiskUsage {
                group_by,
                json,
                prefix,
                target,
            } => {
                assert_eq!(group_by, None);
                assert!(!json);
                assert_eq!(prefix, None);
                assert_eq!(target, "s3/my-bucket");
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_du_prefix() {
        let cmd = Command::new("test").subcommand(cmd_du::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "du", "s3/my-bucket/backups/2026"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::DiskUsage {
                group_by,
                json,
                prefix,
                target,
            } => {
                assert_eq!(group_by, None);
                assert!(!json);
                assert_eq!(prefix.as_deref(), Some("backups/2026"));
                assert_eq!(target, "s3/my-bucket/backups/2026");
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_du_group_by_day() {
        let cmd = Command::new("test").subcommand(cmd_du::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "du", "s3/my-bucket", "--group-by", "day"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::DiskUsage {
                group_by,
                json,
                prefix,
                target,
            } => {
                assert_eq!(group_by, Some(DuGroupBy::Day));
                assert!(!json);
                assert_eq!(prefix, None);
                assert_eq!(target, "s3/my-bucket");
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_cb_1() {
        let cmd = Command::new("test").subcommand(cmd_cb::command());
        let matches = cmd.try_get_matches_from(vec!["test", "cb", "h/bucket/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::CreateBucket { acl } => {
                assert_eq!(acl, "private");
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_cb_2() {
        let cmd = Command::new("test").subcommand(cmd_cb::command());
        let matches = cmd.try_get_matches_from(vec!["test", "cb", "h"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_err());

        let err = s3_location.unwrap_err().to_string();

        println!("{err}");

        assert!(err.contains("Bucket name missing"));
    }

    #[test]
    fn test_dispatch_rm_key() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd.try_get_matches_from(vec!["test", "rm", "h/bucket/key"]);

        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
                older_than,
                recursive,
                targets,
            } => {
                assert_eq!(key, "key");
                assert_eq!(upload_id, "");
                assert!(!bucket);
                assert_eq!(older_than, None);
                assert!(!recursive);
                assert!(targets.is_empty());
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_rm_bucket() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd.try_get_matches_from(vec!["test", "rm", "-b", "h/bucket/file"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
                older_than,
                recursive,
                targets,
            } => {
                assert_eq!(key, "");
                assert_eq!(upload_id, "");
                assert!(bucket);
                assert_eq!(older_than, None);
                assert!(!recursive);
                assert!(targets.is_empty());
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_rm_bucket_recursive() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd.try_get_matches_from(vec!["test", "rm", "-b", "--recursive", "h/bucket"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
                older_than,
                recursive,
                targets,
            } => {
                assert_eq!(key, "");
                assert_eq!(upload_id, "");
                assert!(bucket);
                assert_eq!(older_than, None);
                assert!(recursive);
                assert!(targets.is_empty());
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_finalize_action_single_object_builds_single_group() {
        let (tmp_dir, config_path, config) = write_config_file();
        let filepath = config_path.as_os_str().to_str().unwrap();
        let cmd = new(&tmp_dir.keep());
        let matches = cmd
            .try_get_matches_from(vec!["test", "--config", filepath, "rm", "s3/bucket/key"])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        let action = finalize_action(action, &matches, &config, &config_path).unwrap();

        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
                older_than,
                recursive,
                targets,
            } => {
                let target = targets.first().unwrap();
                let object = target.objects.first().unwrap();
                assert_eq!(key, "key");
                assert_eq!(upload_id, "");
                assert!(!bucket);
                assert_eq!(older_than, None);
                assert!(!recursive);
                assert_eq!(targets.len(), 1);
                assert_eq!(target.objects.len(), 1);
                assert_eq!(object.key, "key");
                assert!(target.s3.endpoint().unwrap().as_str().contains("/bucket"));
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_finalize_action_groups_multiple_objects_by_bucket() {
        let (tmp_dir, config_path, config) = write_config_file();
        let filepath = config_path.as_os_str().to_str().unwrap();
        let cmd = new(&tmp_dir.keep());
        let matches = cmd
            .try_get_matches_from(vec![
                "test",
                "--config",
                filepath,
                "rm",
                "s3/bucket-a/a.txt",
                "s3/bucket-a/b.txt",
                "s3alt/bucket-b/c.txt",
            ])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        let action = finalize_action(action, &matches, &config, &config_path).unwrap();

        match action {
            Action::DeleteObject { targets, .. } => {
                let first = targets.first().unwrap();
                let second = targets.get(1).unwrap();
                let first_object = first.objects.first().unwrap();
                let second_object = first.objects.get(1).unwrap();
                let third_object = second.objects.first().unwrap();
                assert_eq!(targets.len(), 2);
                assert_eq!(first.objects.len(), 2);
                assert_eq!(second.objects.len(), 1);
                assert_eq!(first_object.key, "a.txt");
                assert_eq!(second_object.key, "b.txt");
                assert_eq!(third_object.key, "c.txt");
                assert!(first.s3.endpoint().unwrap().as_str().contains("/bucket-a"));
                assert!(second.s3.endpoint().unwrap().as_str().contains("/bucket-b"));
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_ls_older_than_from_prefix_target() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "ls", "h/bucket/logs/", "--older-than", "30d"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();
        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();

        match action {
            Action::ListObjects {
                bucket,
                older_than,
                prefix,
                ..
            } => {
                assert_eq!(bucket.as_deref(), Some("bucket"));
                assert_eq!(prefix.as_deref(), Some("logs/"));
                assert_eq!(
                    older_than.map(crate::cli::age_filter::AgeFilter::duration),
                    Some(chrono::Duration::days(30))
                );
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_ls_rejects_duplicate_prefix_sources() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "ls", "h/bucket/logs/", "--prefix", "alt/"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();
        let err = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals)
            .unwrap_err()
            .to_string();

        assert!(err.contains("Prefix provided twice"));
    }

    #[test]
    fn test_dispatch_ls_rejects_older_than_without_bucket() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "ls", "h", "--older-than", "30d"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();
        let err = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals)
            .unwrap_err()
            .to_string();

        assert!(err.contains("--older-than requires a bucket or prefix target"));
    }

    #[test]
    fn test_dispatch_rm_older_than_bucket_root() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "rm", "h/bucket", "--older-than", "90d"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();
        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();

        match action {
            Action::DeleteObject {
                key,
                older_than,
                bucket,
                ..
            } => {
                assert_eq!(key, "");
                assert!(!bucket);
                assert_eq!(
                    older_than.map(crate::cli::age_filter::AgeFilter::duration),
                    Some(chrono::Duration::days(90))
                );
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_rm_older_than_prefix() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "rm", "h/bucket/logs/", "--older-than", "12h"])
            .unwrap();

        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();
        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();

        match action {
            Action::DeleteObject {
                key,
                older_than,
                bucket,
                ..
            } => {
                assert_eq!(key, "logs/");
                assert!(!bucket);
                assert_eq!(
                    older_than.map(crate::cli::age_filter::AgeFilter::duration),
                    Some(chrono::Duration::hours(12))
                );
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_finalize_action_keeps_older_than_runtime_listing_path() {
        let (tmp_dir, config_path, config) = write_config_file();
        let filepath = config_path.as_os_str().to_str().unwrap();
        let cmd = new(&tmp_dir.keep());
        let matches = cmd
            .try_get_matches_from(vec![
                "test",
                "--config",
                filepath,
                "rm",
                "s3/bucket/logs/",
                "--older-than",
                "30d",
            ])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        let action = finalize_action(action, &matches, &config, &config_path).unwrap();

        match action {
            Action::DeleteObject {
                key,
                older_than,
                targets,
                ..
            } => {
                assert_eq!(key, "logs/");
                assert!(targets.is_empty());
                assert_eq!(
                    older_than.map(crate::cli::age_filter::AgeFilter::duration),
                    Some(chrono::Duration::days(30))
                );
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_finalize_action_rejects_multiple_older_than_targets() {
        let (tmp_dir, config_path, _config) = write_config_file();
        let filepath = config_path.as_os_str().to_str().unwrap();
        let cmd = new(&tmp_dir.keep());
        let matches = cmd
            .try_get_matches_from(vec![
                "test",
                "--config",
                filepath,
                "rm",
                "s3/bucket-a/logs/",
                "s3/bucket-b/logs/",
                "--older-than",
                "30d",
            ])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals)
            .unwrap_err()
            .to_string();
        assert!(action.contains("--older-than expects exactly one bucket or prefix target"));
    }

    #[test]
    fn test_finalize_action_rejects_bucket_and_object_mix() {
        let (tmp_dir, config_path, config) = write_config_file();
        let filepath = config_path.as_os_str().to_str().unwrap();
        let cmd = new(&tmp_dir.keep());
        let matches = cmd
            .try_get_matches_from(vec![
                "test",
                "--config",
                filepath,
                "rm",
                "s3/bucket-a/a.txt",
                "s3/bucket-b",
            ])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        let err = finalize_action(action, &matches, &config, &config_path)
            .unwrap_err()
            .to_string();

        assert!(err.contains("Object target missing key"));
    }

    #[test]
    fn test_dispatch_share() {
        let cmd = Command::new("test").subcommand(cmd_share::command());
        let matches = cmd.try_get_matches_from(vec!["test", "share", "h/bucket/share_file"]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::ShareObject { key, expire } => {
                assert_eq!(key, "share_file");
                assert_eq!(expire, 43200);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_default_put_1() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            filepath,
            filepath,
            "s3/bucket/key",
        ]);

        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        // assert!(s3_location.is_ok());

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, None);
                assert_eq!(meta, None);
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "key");
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    u8::try_from((num_cpus::get_physical() - 2).max(1)).unwrap_or(u8::MAX)
                );
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_default_put_2() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches =
            cmd.try_get_matches_from(vec!["test", "--config", filepath, "foo", "s3/bucket/key"]);

        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals);
        assert!(action.is_err());

        let err = action.unwrap_err().to_string();
        assert!(err.contains("Source file does not exist"));
    }

    #[test]
    fn test_dispatch_default_put_3() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches =
            cmd.try_get_matches_from(vec!["test", "--config", filepath, filepath, "s3/bucket/"]);

        assert!(matches.is_ok());

        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, None);
                assert_eq!(meta, None);
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, filepath.to_string());
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    u8::try_from((num_cpus::get_physical() - 2).max(1)).unwrap_or(u8::MAX)
                );
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_default_put_acl() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            filepath,
            filepath,
            "s3/bucket/key",
            "--acl",
            "public-read",
            "--number",
            "32",
        ]);

        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        let config = Config::new(config_path.clone()).unwrap();

        let host = get_host(&config, &config_path, &s3_location);
        assert!(host.is_ok());

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, Some("public-read".to_string()));
                assert_eq!(meta, None);
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "key");
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(number, 32);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_default_put_meta() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            filepath,
            filepath,
            "s3/bucket/f",
            "--meta",
            "key1=val1;key2=val2",
            "--checksum",
            "sha256",
            "-n",
            "4",
        ]);
        assert!(matches.is_ok());

        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        let config = Config::new(config_path.clone()).unwrap();

        let host = get_host(&config, &config_path, &s3_location);
        assert!(host.is_ok());

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, None);
                assert_eq!(
                    meta,
                    Some(
                        [
                            ("x-amz-meta-key1".to_string(), "val1".to_string()),
                            ("x-amz-meta-key2".to_string(), "val2".to_string())
                        ]
                        .iter()
                        .cloned()
                        .collect()
                    )
                );
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "f");
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, Some("sha256".to_string()));
                assert_eq!(number, 4);
                assert!(!globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_put_x() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            filepath,
            filepath,
            "s3/bucket/f",
            "-x",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches).unwrap();

        let config = Config::new(config_path.clone()).unwrap();

        let host = get_host(&config, &config_path, &s3_location);
        assert!(host.is_ok());

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, None);
                assert_eq!(meta, None);
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "f");
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    u8::try_from((num_cpus::get_physical() - 2).max(1)).unwrap_or(u8::MAX)
                );
                assert!(globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_put_compress() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            filepath,
            filepath,
            "s3/bucket/f",
            "--compress",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();

        let s3_location = host_bucket_key(&matches);

        assert!(s3_location.is_ok());

        let action = dispatch(
            s3_location.unwrap(),
            0,
            PathBuf::new(),
            &matches,
            &mut globals,
        )
        .unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
                host,
                s3m_dir,
                key,
                pipe,
                quiet,
                tmp_dir,
                checksum_algorithm,
                number,
            } => {
                assert_eq!(acl, None);
                assert_eq!(meta, None);
                assert_eq!(buf_size, 0);
                assert_eq!(file, Some(filepath.to_string()));
                assert_eq!(host, "s3");
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "f");
                assert!(!pipe);
                assert!(!quiet);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    u8::try_from((num_cpus::get_physical() - 2).max(1)).unwrap_or(u8::MAX)
                );
                assert!(globals.compress);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_metadata_parsing_valid() {
        // Test the metadata parsing logic directly
        let meta_str = "key1=value1;key2=value2;key3=value3";
        let mut metadata = BTreeMap::new();

        for item in meta_str.split(';') {
            match item.split_once('=') {
                Some((key, val)) => {
                    metadata.insert(format!("x-amz-meta-{key}"), val.to_owned());
                }
                None => {
                    panic!("Should not reach here with valid input");
                }
            }
        }

        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata.get("x-amz-meta-key1"), Some(&"value1".to_string()));
        assert_eq!(metadata.get("x-amz-meta-key2"), Some(&"value2".to_string()));
        assert_eq!(metadata.get("x-amz-meta-key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_metadata_parsing_invalid() {
        // Test that invalid format is properly detected
        let meta_str = "key1=value1;invalid_without_equals;key3=value3";
        let mut has_error = false;

        for item in meta_str.split(';') {
            if item.split_once('=').is_none() {
                has_error = true;
                assert_eq!(item, "invalid_without_equals");
                break;
            }
        }

        assert!(has_error, "Should detect invalid metadata format");
    }

    #[test]
    fn test_metadata_parsing_empty() {
        // Test empty metadata string
        let meta_str = "";
        let mut metadata = BTreeMap::new();

        for item in meta_str.split(';').filter(|s| !s.is_empty()) {
            match item.split_once('=') {
                Some((key, val)) => {
                    metadata.insert(format!("x-amz-meta-{key}"), val.to_owned());
                }
                None => {
                    panic!("Should not have invalid entries");
                }
            }
        }

        assert_eq!(metadata.len(), 0);
    }

    #[test]
    fn test_dispatch_missing_source_file() {
        // Test that missing source file without pipe flag returns error
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let filepath = config_path.as_os_str().to_str().unwrap();

        let cmd = new(&tmp_dir.keep());
        let matches = cmd.try_get_matches_from(vec!["test", "--config", filepath, "s3/bucket/key"]);

        assert!(matches.is_ok());

        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals);

        assert!(action.is_err());
        let err = action.unwrap_err().to_string();
        assert!(err.contains("Source file missing"));
    }

    #[test]
    fn test_dispatch_streams_defaults_to_list() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec![
                "s3m",
                "--config",
                config_path.to_str().unwrap(),
                "streams",
            ])
            .unwrap();

        let action = dispatch_streams(&matches, tmp_dir.path().to_path_buf(), config_path).unwrap();
        match action {
            Action::Streams {
                command: StreamCommand::List,
                ..
            } => {}
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn test_dispatch_streams_resume_uses_requested_number() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec![
                "s3m",
                "--config",
                config_path.to_str().unwrap(),
                "--number",
                "7",
                "streams",
                "resume",
                "stream-1",
            ])
            .unwrap();

        let action = dispatch_streams(&matches, tmp_dir.path().to_path_buf(), config_path).unwrap();
        match action {
            Action::Streams {
                command: StreamCommand::Resume { id },
                number,
                ..
            } => {
                assert_eq!(id, "stream-1");
                assert_eq!(number, 7);
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn test_dispatch_get_json_requires_metadata_or_versions() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "get", "h/bucket/key", "--json"])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let err = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--json is only supported"));
    }

    #[test]
    fn test_dispatch_ls_json() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "ls", "s3/bucket/prefix", "--json"])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::ListObjects { json, .. } => assert!(json),
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn test_dispatch_du_json() {
        let cmd = Command::new("test").subcommand(cmd_du::command());
        let matches = cmd
            .try_get_matches_from(vec!["test", "du", "s3/my-bucket", "--json"])
            .unwrap();
        let mut globals = GlobalArgs::new();
        let s3_location = host_bucket_key(&matches).unwrap();

        let action = dispatch(s3_location, 0, PathBuf::new(), &matches, &mut globals).unwrap();
        match action {
            Action::DiskUsage { json, .. } => assert!(json),
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn test_dispatch_streams_json() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec![
                "s3m",
                "--config",
                config_path.to_str().unwrap(),
                "streams",
                "ls",
                "--json",
            ])
            .unwrap();

        let action = dispatch_streams(&matches, tmp_dir.path().to_path_buf(), config_path).unwrap();
        match action {
            Action::Streams { json, .. } => assert!(json),
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn test_dispatch_streams_resume_rejects_json() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();

        let cmd = new(tmp_dir.path());
        let matches = cmd
            .try_get_matches_from(vec![
                "s3m",
                "--config",
                config_path.to_str().unwrap(),
                "streams",
                "resume",
                "stream-1",
                "--json",
            ])
            .unwrap();

        let err = dispatch_streams(&matches, tmp_dir.path().to_path_buf(), config_path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--json is not supported"));
    }
}
