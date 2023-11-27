use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("get")
        .about("Retrieves objects")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .value_names(["S3M"])
                .num_args(1),
        )
        .arg(
            Arg::new("HeadObject")
                .help("Retrieves metadata from an object without returning the object itself")
                .long("head")
                .short('H')
                .num_args(0),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("Don't show progress bar"),
        )
}
