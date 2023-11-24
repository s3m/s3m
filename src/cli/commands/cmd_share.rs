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
