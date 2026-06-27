use clap::{Arg, ArgAction, Command};

pub fn command() -> Command {
    Command::new("object-lock")
        .about("Manage S3 Object Lock (WORM) settings")
        .after_long_help(
            "Examples:\n\
             \x20 # Bucket default retention (applies to every new object)\n\
             \x20 s3m object-lock set s3/vault --mode COMPLIANCE --days 30\n\
             \x20 s3m object-lock get s3/vault\n\n\
             \x20 # Per-object retention / legal hold\n\
             \x20 s3m object-lock set s3/vault/file.dat --mode GOVERNANCE --retain-until 2027-01-01T00:00:00Z\n\
             \x20 s3m object-lock set s3/vault/file.dat --legal-hold on\n\
             \x20 s3m object-lock get s3/vault/file.dat",
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("get")
                .about("Show the bucket lock config, or an object's retention + legal hold")
                .arg(
                    Arg::new("arguments")
                        .help("host/bucket or host/bucket/key")
                        .required(true)
                        .num_args(1),
                )
                .arg(
                    Arg::new("version-id")
                        .long("version-id")
                        .help("Object version id (object target only)")
                        .num_args(1),
                )
                .arg(
                    Arg::new("json")
                        .long("json")
                        .help("Output as JSON")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("set")
                .about("Set the bucket default retention, or an object's retention / legal hold")
                .arg(
                    Arg::new("arguments")
                        .help("host/bucket or host/bucket/key")
                        .required(true)
                        .num_args(1),
                )
                .arg(
                    Arg::new("mode")
                        .long("mode")
                        .help("Retention mode: GOVERNANCE or COMPLIANCE")
                        .value_parser(["GOVERNANCE", "COMPLIANCE"])
                        .num_args(1),
                )
                .arg(
                    Arg::new("days")
                        .long("days")
                        .help("Bucket default retention period in days (bucket target)")
                        .value_parser(clap::value_parser!(u32))
                        .conflicts_with_all(["years", "retain-until"])
                        .num_args(1),
                )
                .arg(
                    Arg::new("years")
                        .long("years")
                        .help("Bucket default retention period in years (bucket target)")
                        .value_parser(clap::value_parser!(u32))
                        .conflicts_with_all(["days", "retain-until"])
                        .num_args(1),
                )
                .arg(
                    Arg::new("retain-until")
                        .long("retain-until")
                        .help("Object retain-until date, RFC 3339 (object target)")
                        .conflicts_with_all(["days", "years"])
                        .num_args(1),
                )
                .arg(
                    Arg::new("legal-hold")
                        .long("legal-hold")
                        .help("Object legal hold: on or off (object target)")
                        .value_parser(["on", "off"])
                        .num_args(1),
                )
                .arg(
                    Arg::new("version-id")
                        .long("version-id")
                        .help("Object version id (object target only)")
                        .num_args(1),
                )
                .arg(
                    Arg::new("bypass-governance")
                        .long("bypass-governance")
                        .help("Send x-amz-bypass-governance-retention (to shorten GOVERNANCE retention)")
                        .action(ArgAction::SetTrue),
                ),
        )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_get_requires_argument() {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["object-lock", "get"]);
        assert!(m.is_err());
    }

    #[test]
    fn test_set_bucket_default() {
        let m = command()
            .try_get_matches_from(vec![
                "object-lock",
                "set",
                "s3/vault",
                "--mode",
                "COMPLIANCE",
                "--days",
                "30",
            ])
            .unwrap();
        let sub = m.subcommand_matches("set").unwrap();
        assert_eq!(
            sub.get_one::<String>("mode").map(String::as_str),
            Some("COMPLIANCE")
        );
        assert_eq!(sub.get_one::<u32>("days").copied(), Some(30));
    }

    #[test]
    fn test_set_days_years_conflict() {
        let m = command().try_get_matches_from(vec![
            "object-lock",
            "set",
            "s3/vault",
            "--mode",
            "GOVERNANCE",
            "--days",
            "1",
            "--years",
            "1",
        ]);
        assert!(m.is_err());
    }
}
