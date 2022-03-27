use crate::s3m::start::Action;
use anyhow::{anyhow, Context, Result};
use colored::Colorize;
use std::path::PathBuf;

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
        // GetObject
        Some("get") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("get")?;
            let get_head = sub_m.is_present("HeadObject");
            let args: Vec<&str> = sub_m.values_of("arguments").unwrap_or_default().collect();
            let dest = if args.len() == 2 {
                Some(args[1].to_string())
            } else {
                None
            };
            Ok(Action::GetObject {
                key,
                get_head,
                dest,
                quiet: sub_m.is_present("quiet"),
            })
        }

        // ListObjects
        Some("ls") => {
            let sub_m = sub_m("ls")?;
            let prefix = if sub_m.is_present("prefix") {
                Some(sub_m.value_of("prefix").unwrap_or_default().to_string())
            } else {
                None
            };
            let start_after = if sub_m.is_present("start-after") {
                Some(
                    sub_m
                        .value_of("start-after")
                        .unwrap_or_default()
                        .to_string(),
                )
            } else {
                None
            };
            Ok(Action::ListObjects {
                bucket,
                list_multipart_uploads: sub_m.is_present("ListMultipartUploads"),
                prefix,
                start_after,
            })
        }

        // DeleteObject
        Some("rm") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("rm")?;
            let upload_id = sub_m.value_of("UploadId").unwrap_or_default().to_string();
            Ok(Action::DeleteObject { key, upload_id })
        }

        // ShareObject
        Some("share") => {
            let key = hbp_empty(hbp)?;
            let sub_m = sub_m("share")?;
            let expire = sub_m.value_of("expire").unwrap().parse::<usize>()?;
            Ok(Action::ShareObject { key, expire })
        }

        // PutObject
        _ => {
            let key = hbp_empty(hbp)?;
            let mut src: Option<String> = None;
            let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
            if args.len() == 2 {
                src = Some(args[0].to_string());
            }
            let acl = if matches.is_present("acl") {
                Some(matches.value_of("acl").unwrap_or_default().to_string())
            } else {
                None
            };
            Ok(Action::PutObject {
                acl,
                buf_size,
                file: src,
                s3m_dir,
                key,
                pipe: matches.is_present("pipe"),
                quiet: matches.is_present("quiet"),
            })
        }
    }
}
