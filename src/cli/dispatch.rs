use crate::cli::actions::Action;
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::{collections::BTreeMap, path::PathBuf};

// return Action based on the command or subcommand
pub fn dispatch(
    hbp: Vec<&str>,
    bucket: Option<String>,
    buf_size: usize,
    s3m_dir: PathBuf,
    matches: &clap::ArgMatches,
) -> Result<Action> {
    // Closure to return subcommand_matches
    let sub_m = |subcommand| -> Result<&clap::ArgMatches> {
        matches
            .subcommand_matches(subcommand)
            .context("arguments missing")
    };

    // Closure to check if hpb is not empty and if not return the file key
    let hbp_empty = |hbp: Vec<&str>| -> Result<String> {
        if hbp.is_empty() && matches.subcommand_matches("mb").is_none() {
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
            let get_head = sub_m.get_one("HeadObject").copied().unwrap_or(false);
            let args: Vec<&str> = sub_m
                .get_many::<String>("arguments")
                .unwrap_or_default()
                .map(|s| s.as_str())
                .collect();

            let dest = if args.len() == 2 {
                Some(args[1].to_string())
            } else {
                None
            };
            Ok(Action::GetObject {
                key,
                get_head,
                dest,
                quiet: sub_m.contains_id("quiet"),
            })
        }

        // ListObjects
        Some("ls") => {
            let sub_m = sub_m("ls")?;
            let prefix = sub_m.get_one("prefix").map(|s: &String| s.to_string());
            let start_after = sub_m.get_one("start-after").map(|s: &String| s.to_string());
            Ok(Action::ListObjects {
                bucket,
                list_multipart_uploads: sub_m
                    .get_one("ListMultipartUploads")
                    .copied()
                    .unwrap_or(false),
                prefix,
                start_after,
            })
        }

        // MakeBucket
        Some("mb") => match bucket {
            Some(_) => {
                let sub_m = sub_m("mb")?;
                let acl = sub_m
                    .get_one("acl")
                    .map_or_else(|| String::from("private"), |s: &String| s.to_string());
                Ok(Action::MakeBucket { acl })
            }
            None => Err(anyhow!("Bucket name missing, <s3 provider>/<bucket>")),
        },

        // DeleteObject
        Some("rm") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("rm")?;
            let upload_id = sub_m
                .get_one("UploadId")
                .map_or_else(String::new, |s: &String| s.to_string());
            Ok(Action::DeleteObject { key, upload_id })
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
                .map(|s| s.as_str())
                .collect();
            if args.len() == 2 {
                src = Some(args[0].to_string());
            }

            let acl = matches.get_one("acl").map(|s: &String| s.to_string());

            let meta = if matches
                .get_one("meta")
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
            Ok(Action::PutObject {
                acl,
                meta,
                buf_size,
                file: src,
                s3m_dir,
                key,
                pipe: matches.get_one("pipe").copied().unwrap_or(false),
                quiet: matches.get_one("quiet").copied().unwrap_or(false),
            })
        }
    }
}
