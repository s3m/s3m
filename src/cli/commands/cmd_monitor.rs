use clap::{Arg, ArgAction, Command};

pub fn command() -> Command {
    Command::new("monitor")
        .about("Run configured bucket monitoring checks for one host")
        .after_long_help(
            "Examples:\n  s3m monitor s3\n  s3m monitor s3 --format influxdb\n  s3m monitor s3 --exit-on-check-failure",
        )
        .arg(
            Arg::new("arguments")
                .help("Configured host name from config.yml")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("format")
                .short('f')
                .long("format")
                .value_name("FORMAT")
                .help("Output format: prometheus (default) or influxdb")
                .default_value("prometheus")
                .value_parser(["prometheus", "influxdb"])
                .num_args(1),
        )
        .arg(
            Arg::new("exit-on-check-failure")
                .long("exit-on-check-failure")
                .help("Exit with status 1 if any check is missing, errors, or size-mismatched")
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
    fn test_monitor_host_argument() -> Result<()> {
        let matches = command().try_get_matches_from(vec!["monitor", "s3"])?;
        assert_eq!(
            matches.get_one::<String>("arguments").map(String::as_str),
            Some("s3")
        );
        Ok(())
    }

    #[test]
    fn test_monitor_format_default_is_prometheus() -> Result<()> {
        let matches = command().try_get_matches_from(vec!["monitor", "s3"])?;
        assert_eq!(
            matches.get_one::<String>("format").map(String::as_str),
            Some("prometheus")
        );
        Ok(())
    }

    #[test]
    fn test_monitor_format_influxdb() -> Result<()> {
        let matches =
            command().try_get_matches_from(vec!["monitor", "s3", "--format", "influxdb"])?;
        assert_eq!(
            matches.get_one::<String>("format").map(String::as_str),
            Some("influxdb")
        );
        Ok(())
    }

    #[test]
    fn test_monitor_exit_on_check_failure_flag() -> Result<()> {
        let matches =
            command().try_get_matches_from(vec!["monitor", "s3", "--exit-on-check-failure"])?;
        assert!(matches.get_flag("exit-on-check-failure"));
        Ok(())
    }
}
