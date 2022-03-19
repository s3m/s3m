use crate::s3::Region;
use crate::s3m::Host;
use anyhow::{anyhow, Context, Result};

/// `hpb_src_dest`
///
/// hbp - host, bucket, path
/// src - file to upload
/// dest - path to save
///
/// returns tuple with `hbp`, `src`, `dest`
pub fn hbp_src_dest(
    matches: &clap::ArgMatches,
) -> Result<(Vec<&str>, Option<String>, Option<String>)> {
    // Host, Bucket, Path
    let hbp: Vec<&str>;

    // source file (file to upload)
    let mut src: Option<String> = None;

    // destination  (path to save)
    let mut dest: Option<String> = None;

    match matches.subcommand_name() {
        // GetObject
        Some("get") => {
            let get = matches
                .subcommand_matches("get")
                .context("arguments missing")?;
            let args: Vec<&str> = get.values_of("arguments").unwrap_or_default().collect();
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
            if args.len() == 2 {
                dest = Some(args[1].to_string());
            }
        }

        // ListObjects
        Some("ls") => {
            let ls = matches
                .subcommand_matches("ls")
                .context("arguments missing")?;
            let args: Vec<&str> = ls.values_of("arguments").unwrap_or_default().collect();
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
        }

        // DeleteObject
        Some("rm") => {
            let rm = matches
                .subcommand_matches("rm")
                .context("arguments missing")?;
            let args: Vec<&str> = rm.values_of("arguments").unwrap_or_default().collect();
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
        }

        //ShareObject
        Some("share") => {
            let share = matches
                .subcommand_matches("share")
                .context("arguments missing")?;
            let args: Vec<&str> = share.values_of("arguments").unwrap_or_default().collect();
            hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
        }

        // PutObject
        _ => {
            let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
            if args.len() == 2 {
                hbp = args[1].split('/').filter(|s| !s.is_empty()).collect();
                src = Some(args[0].to_string());
            } else if matches.is_present("pipe") {
                hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
            } else {
                return Err(anyhow!(
                "missing argument or use --pipe for standar input. For more information try: --help"
            ));
            }
        }
    }

    Ok((hbp, src, dest))
}

/// `get_region`
/// return Region
pub fn get_region(host: &Host) -> Result<Region> {
    Ok(match &host.region {
        Some(r) => r.parse::<Region>()?,
        None => {
            let r = host
                .endpoint
                .as_ref()
                .context("could not parse host need an endpoint or region")?;
            Region::Custom {
                name: "".to_string(),
                endpoint: r.to_string(),
            }
        }
    })
}
