use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("get")
        .about("Retrieves objects")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file> <optional path/file>")
                .required(true)
                .value_names(["S3M"])
                .num_args(1..=2),
        )
        .arg(
            Arg::new("metadata")
                .help("Retrieves metadata from an object without returning the object itself")
                .long("meta")
                .short('m')
                .num_args(0),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("Don't show progress bar")
                .num_args(0),
        )
        .arg(
            Arg::new("force")
                .long("force")
                .short('f')
                .help("Force overwrite")
                .num_args(0),
        )
}
