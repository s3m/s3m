use clap::{Arg, ArgAction, Command};

pub fn command() -> Command {
    Command::new("cb")
        .about("Create a bucket")
        .after_long_help("Examples:\n  s3m cb s3/new-bucket\n  s3m cb s3/new-bucket --acl private\n  s3m cb s3/vault --object-lock")
        .arg(
            Arg::new("arguments")
                .help("host/bucket")
                .long_help("Bucket to create.\n\nExample:\n  s3/new-bucket")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("acl")
                .help("The canned ACL to apply to the object example")
                .long_help("Set the canned ACL for the new bucket.")
                .long("acl")
                .value_parser([
                    "private",
                    "public-read",
                    "public-read-write",
                    "authenticated-read",
                    "aws-exec-read",
                    "bucket-owner-read",
                    "bucket-owner-full-control",
                ])
                .default_value("private")
                .short('a')
                .num_args(1),
        )
        .arg(
            Arg::new("object-lock")
                .help("Enable S3 Object Lock (WORM) on the new bucket")
                .long_help(
                    "Create the bucket with S3 Object Lock enabled.\n\n\
                     Object Lock can only be turned on at bucket creation and \
                     automatically enables versioning. Objects can then be \
                     uploaded with retention (--object-lock-mode/--retain-until) \
                     or a legal hold (--legal-hold).",
                )
                .long("object-lock")
                .action(ArgAction::SetTrue),
        )
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
    use anyhow::Result;

    #[test]
    fn test_check_acl() -> Result<()> {
        let tets = vec![
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ];
        for acl in tets {
            let cmd = command();
            let m = cmd.try_get_matches_from(vec!["s3m", "test", "--acl", acl]);
            assert!(m.is_ok());

            // get matches
            let m = m.unwrap();
            assert_eq!(m.get_one::<String>("acl").map(String::as_str), Some(acl));
        }
        Ok(())
    }

    #[test]
    fn test_check_arguments() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<String>("arguments").map(String::as_str),
            Some("test")
        );
        Ok(())
    }
}
