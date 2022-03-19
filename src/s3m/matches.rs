use crate::s3::Region;
use crate::s3m::Host;
use anyhow::{anyhow, Context, Result};

/// returns the host, bucket and path from the arguments
pub fn host_bucket_path(matches: &clap::ArgMatches) -> Result<Vec<&str>> {
    // Host, Bucket, Path
    let hbp: Vec<&str>;

    let parse_args = |subcommand| -> Result<Vec<&str>> {
        let args: Vec<&str> = matches
            .subcommand_matches(subcommand)
            .context("arguments missing")?
            .values_of("arguments")
            .unwrap_or_default()
            .collect();
        Ok(args[0].split('/').filter(|s| !s.is_empty()).collect())
    };

    match matches.subcommand_name() {
        // GetObject
        Some("get") => {
            hbp = parse_args("get")?;
        }

        // ListObjects
        Some("ls") => {
            hbp = parse_args("ls")?;
        }

        // DeleteObject
        Some("rm") => {
            hbp = parse_args("rm")?;
        }

        // ShareObject
        Some("share") => {
            hbp = parse_args("share")?;
        }

        // PutObject
        _ => {
            let args: Vec<&str> = matches.values_of("arguments").unwrap_or_default().collect();
            if args.len() == 2 {
                hbp = args[1].split('/').filter(|s| !s.is_empty()).collect();
            } else if matches.is_present("pipe") {
                hbp = args[0].split('/').filter(|s| !s.is_empty()).collect();
            } else {
                return Err(anyhow!(
                "missing argument or use --pipe for standar input. For more information try: --help"
            ));
            }
        }
    }

    Ok(hbp)
}

/// find region or endpoint
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
