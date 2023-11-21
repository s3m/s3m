use clap::{Arg, Command};

pub fn subcommand_acl() -> Command {
    Command::new("acl")
        .about("PUT or GET object ACL")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .value_names(["S3M"])
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
                .short('a')
                .num_args(1),
        )
}
