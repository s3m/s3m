use crate::cli::commands::validator_is_num;
use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("share")
        .about("Share object using a presigned URL")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("expire")
                .help("Time period in seconds, max value 604800 (seven days)")
                .long("expire")
                .short('e')
                .default_value("43200")
                .num_args(1)
                .value_parser(validator_is_num()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_check_expire() -> Result<()> {
        let cmd = command();
        let m = cmd.try_get_matches_from(vec!["s3m", "test", "--expire", "604800"]);
        assert!(m.is_ok());

        // get matches
        let m = m.unwrap();
        assert_eq!(
            m.get_one::<usize>("expire").map_or_else(|| 0, |v| *v),
            604800 as usize
        );
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
