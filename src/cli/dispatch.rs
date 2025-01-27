use crate::cli::{actions::Action, globals::GlobalArgs};
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::{borrow::ToOwned, collections::BTreeMap, path::PathBuf, string::String};

// return Action based on the command or subcommand
pub fn dispatch(
    hbp: Vec<&str>,
    bucket: Option<String>,
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

    // Closure to check if hpb is not empty and if not return the file key
    let hbp_empty = |hbp: Vec<&str>| -> Result<String> {
        if hbp.is_empty() {
            return Err(anyhow!(
                "file name missing, <s3 provider>/<bucket>/{}, For more information try {}",
                "<file name>".red(),
                "--help".green()
            ));
        }
        Ok(hbp.join("/"))
    };

    match matches.subcommand_name() {
        // ACL
        Some("acl") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("acl")?;
            let acl = sub_m.get_one("acl").map(|s: &String| s.to_string());
            Ok(Action::ACL { key, acl })
        }

        // GetObject
        Some("get") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("get")?;
            let metadata = sub_m.get_one("metadata").copied().unwrap_or(false);
            let args: Vec<&str> = sub_m
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(String::as_str)
                .collect();
            let quiet = sub_m.get_one("quiet").copied().unwrap_or(false);
            let force = sub_m.get_one("force").copied().unwrap_or(false);
            let versions = sub_m.get_one("versions").copied().unwrap_or(false);
            let version = sub_m.get_one("version").map(|s: &String| s.to_string());

            // get destination file/path
            let dest = if args.len() == 2 {
                Some(args[1].to_string())
            } else {
                None
            };

            Ok(Action::GetObject {
                dest,
                force,
                key,
                metadata,
                quiet,
                versions,
                version,
            })
        }

        // ListObjects
        Some("ls") => {
            let sub_m = sub_m("ls")?;
            let prefix = sub_m.get_one("prefix").map(|s: &String| s.to_string());
            let start_after = sub_m.get_one("start-after").map(|s: &String| s.to_string());
            // convert max_keys to string and default to None
            let max_kub = sub_m.get_one::<usize>("max-kub").map(|s| s.to_string());
            Ok(Action::ListObjects {
                bucket,
                list_multipart_uploads: sub_m
                    .get_one("ListMultipartUploads")
                    .copied()
                    .unwrap_or(false),
                max_kub,
                prefix,
                start_after,
            })
        }

        // CreateBucket
        Some("cb") => match bucket {
            Some(_) => {
                let sub_m = sub_m("cb")?;
                let acl = sub_m
                    .get_one("acl")
                    .map_or_else(|| String::from("private"), |s: &String| s.to_string());
                Ok(Action::CreateBucket { acl })
            }
            None => Err(anyhow!("Bucket name missing, <s3 provider>/<bucket>")),
        },

        // DeleteObject or DeleteBucket
        Some("rm") => {
            let mut key = String::new();
            let sub_m = sub_m("rm")?;
            let upload_id = sub_m
                .get_one("UploadId")
                .map_or_else(String::new, |s: &String| s.to_string());
            let bucket = sub_m.get_one("bucket").copied().unwrap_or(false);
            if !bucket {
                key = hbp_empty(hbp)?;
            }
            Ok(Action::DeleteObject {
                key,
                upload_id,
                bucket,
            })
        }

        // ShareObject
        Some("share") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("share")?;
            let expire = sub_m.get_one::<usize>("expire").map_or_else(|| 0, |s| *s);
            Ok(Action::ShareObject { key, expire })
        }

        // PutObject
        _ => {
            let key = hbp_empty(hbp)?;
            let mut src: Option<String> = None;
            let args: Vec<&str> = matches
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(String::as_str)
                .collect();
            if args.len() == 2 {
                src = Some(args[0].to_string());
            }

            // get ACL to apply to the object
            let acl = matches.get_one("acl").map(|s: &String| s.to_string());

            // get x-amz-meta- to apply to the object
            let meta = if matches
                .get_one::<String>("meta")
                .map(|s: &String| s.to_string())
                .is_some()
            {
                Some(
                    matches
                        .get_one("meta")
                        .map(|s: &String| s.to_string())
                        .unwrap_or_default()
                        .split(';')
                        .map(|s| s.split_once('=').unwrap())
                        .map(|(key, val)| {
                            (format!("x-amz-meta-{}", key.to_owned()), val.to_owned())
                        })
                        .collect::<BTreeMap<String, String>>(),
                )
            } else {
                None
            };

            // set compress
            if !global_args.compress {
                global_args.compress = matches.get_one("compress").copied().unwrap_or(false);
            }

            // set encrypt
            // TODO: implement encrypt

            Ok(Action::PutObject {
                acl,
                meta,
                buf_size,
                file: src,
                s3m_dir,
                key,
                pipe: matches.get_one("pipe").copied().unwrap_or(false),
                quiet: matches.get_one("quiet").copied().unwrap_or(false),
                tmp_dir: matches.get_one::<PathBuf>("tmp-dir").map_or_else(
                    || std::env::temp_dir().join(format!("s3m-{}", std::process::id())),
                    ToOwned::to_owned,
                ),
                checksum_algorithm: matches.get_one("checksum").map(|s: &String| s.to_string()),
                number: matches.get_one::<u8>("number").copied().unwrap_or(1),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{
        actions::Action,
        commands::{cmd_acl, cmd_cb, cmd_get, cmd_ls, cmd_rm, cmd_share, new},
        globals::GlobalArgs,
    };
    use clap::Command;
    use std::cmp;
    use std::fs::File;
    use std::io::Write;
    use tempfile::Builder;

    const CONF: &str = r#"---
hosts:
  s3:
    region: xx-region-y
    access_key: XXX
    secret_key: YYY
    bucket: my-bucket"#;

    #[test]
    fn test_dispatch_acl() {
        let cmd = Command::new("test").subcommand(cmd_acl::command());
        let matches = cmd.try_get_matches_from(vec!["test", "acl", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::ACL { key, acl } => {
                assert_eq!(key, "h/b/f");
                assert_eq!(acl, None);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_get() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd.try_get_matches_from(vec!["test", "get", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::GetObject {
                key,
                metadata,
                dest,
                quiet,
                force,
                versions,
                version,
            } => {
                assert_eq!(key, "h/b/f");
                assert_eq!(metadata, false);
                assert_eq!(dest, None);
                assert_eq!(quiet, false);
                assert_eq!(force, false);
                assert_eq!(versions, false);
                assert_eq!(version, None);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_get_quiet_force() {
        let cmd = Command::new("test").subcommand(cmd_get::command());
        let matches = cmd.try_get_matches_from(vec!["test", "get", "h/b/f", "-q", "-f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::GetObject {
                key,
                metadata,
                dest,
                quiet,
                force,
                versions,
                version,
            } => {
                assert_eq!(key, "h/b/f");
                assert_eq!(metadata, false);
                assert_eq!(dest, None);
                assert_eq!(quiet, true);
                assert_eq!(force, true);
                assert_eq!(versions, false);
                assert_eq!(version, None);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_ls() {
        let cmd = Command::new("test").subcommand(cmd_ls::command());
        let matches = cmd.try_get_matches_from(vec!["test", "ls", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::ListObjects {
                bucket,
                list_multipart_uploads,
                max_kub,
                prefix,
                start_after,
            } => {
                assert_eq!(bucket, None);
                assert_eq!(list_multipart_uploads, false);
                assert_eq!(prefix, None);
                assert_eq!(start_after, None);
                assert_eq!(max_kub, None);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_cb() {
        let cmd = Command::new("test").subcommand(cmd_cb::command());
        let matches = cmd.try_get_matches_from(vec!["test", "cb", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            Some("bucket-required".to_string()),
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::CreateBucket { acl } => {
                assert_eq!(acl, "private");
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_rm() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd.try_get_matches_from(vec!["test", "rm", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
            } => {
                assert_eq!(key, "h/b/f");
                assert_eq!(upload_id, "");
                assert_eq!(bucket, false);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_rm_bucket() {
        let cmd = Command::new("test").subcommand(cmd_rm::command());
        let matches = cmd.try_get_matches_from(vec!["test", "rm", "-b", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::DeleteObject {
                key,
                upload_id,
                bucket,
            } => {
                assert_eq!(key, "");
                assert_eq!(upload_id, "");
                assert_eq!(bucket, true);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_share() {
        let cmd = Command::new("test").subcommand(cmd_share::command());
        let matches = cmd.try_get_matches_from(vec!["test", "share", "h/b/f"]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::ShareObject { key, expire } => {
                assert_eq!(key, "h/b/f");
                assert_eq!(expire, 43200);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn test_dispatch_default_put() {
        let tmp_dir = Builder::new().prefix("test-s3m-").tempdir().unwrap();
        let config_path = tmp_dir.path().join("config.yaml");
        let mut config = File::create(&config_path).unwrap();
        config.write_all(CONF.as_bytes()).unwrap();
        let cmd = new(&tmp_dir.into_path());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            config_path.as_os_str().to_str().unwrap(),
            "path/to/file",
            "h/b/f",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
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
                assert_eq!(file, Some("path/to/file".to_string()));
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "h/b/f");
                assert_eq!(pipe, false);
                assert_eq!(quiet, false);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    cmp::min((num_cpus::get_physical() - 2).max(1) as u8, u8::MAX)
                );
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
        let cmd = new(&tmp_dir.into_path());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            config_path.as_os_str().to_str().unwrap(),
            "path/to/file",
            "h/b/f",
            "--acl",
            "public-read",
            "--number",
            "32",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
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
                assert_eq!(file, Some("path/to/file".to_string()));
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "h/b/f");
                assert_eq!(pipe, false);
                assert_eq!(quiet, false);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(number, 32);
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
        let cmd = new(&tmp_dir.into_path());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            config_path.as_os_str().to_str().unwrap(),
            "path/to/file",
            "h/b/f",
            "--meta",
            "key1=val1;key2=val2",
            "--checksum",
            "sha256",
            "-n",
            "4",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let action = dispatch(
            vec!["h/b/f"],
            None,
            0,
            PathBuf::new(),
            &matches,
            &mut GlobalArgs::new(),
        )
        .unwrap();
        match action {
            Action::PutObject {
                acl,
                meta,
                buf_size,
                file,
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
                assert_eq!(file, Some("path/to/file".to_string()));
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "h/b/f");
                assert_eq!(pipe, false);
                assert_eq!(quiet, false);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, Some("sha256".to_string()));
                assert_eq!(number, 4);
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
        let cmd = new(&tmp_dir.into_path());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            config_path.as_os_str().to_str().unwrap(),
            "path/to/file",
            "h/b/f",
            "-x",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();

        let mut globals = GlobalArgs::new();
        let action = dispatch(
            vec!["h/b/f"],
            None,
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
                assert_eq!(file, Some("path/to/file".to_string()));
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "h/b/f");
                assert_eq!(pipe, false);
                assert_eq!(quiet, false);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    cmp::min((num_cpus::get_physical() - 2).max(1) as u8, u8::MAX)
                );
                assert_eq!(globals.compress, true);
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
        let cmd = new(&tmp_dir.into_path());
        let matches = cmd.try_get_matches_from(vec![
            "test",
            "--config",
            config_path.as_os_str().to_str().unwrap(),
            "path/to/file",
            "h/b/f",
            "--compress",
        ]);
        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let mut globals = GlobalArgs::new();
        let action = dispatch(
            vec!["h/b/f"],
            None,
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
                assert_eq!(file, Some("path/to/file".to_string()));
                assert_eq!(s3m_dir, PathBuf::new());
                assert_eq!(key, "h/b/f");
                assert_eq!(pipe, false);
                assert_eq!(quiet, false);
                assert_eq!(tmp_dir, std::env::temp_dir());
                assert_eq!(checksum_algorithm, None);
                assert_eq!(
                    number,
                    cmp::min((num_cpus::get_physical() - 2).max(1) as u8, u8::MAX)
                );
                assert_eq!(globals.compress, true);
            }
            _ => panic!("wrong action"),
        }
    }
}
