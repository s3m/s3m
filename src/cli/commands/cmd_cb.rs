use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("cb")
        .about("Create a bucket")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket name>")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("acl")
                .help("The canned ACL to apply to the object example")
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
