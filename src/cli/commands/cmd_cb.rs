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
