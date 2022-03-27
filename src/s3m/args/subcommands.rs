use crate::s3m::args::validators;
use clap::{Arg, Command};

pub fn subcommand_ls<'a>() -> clap::Command<'a> {
    Command::new("ls")
        .about("List objects and in-progress multipart uploads")
        .arg(
            Arg::new("arguments")
                .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                .required(true)
                .min_values(1),
        )
        .arg(
            Arg::new("ListMultipartUploads")
                .help("Lists in-progress multipart uploads")
                .long("multipart")
                .short('m'),
        )
        .arg(
            Arg::new("prefix")
                .help("Limits the response to keys that begin with the specified prefix")
                .long("prefix")
                .short('p')
                .takes_value(true),
        )
        .arg(
            Arg::new("start-after")
                .help("Starts listing after this specified key")
                .long("start-after")
                .short('a')
                .takes_value(true),
        )
}

pub fn subcommand_rm<'a>() -> clap::Command<'a> {
    Command::new("rm")
        .about("Delete objects and aborts a multipart upload")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .min_values(1),
        )
        .arg(
            Arg::new("UploadId")
                .help("aborts a multipart upload")
                .long("abort")
                .short('a')
                .takes_value(true),
        )
}

pub fn subcommand_get<'a>() -> clap::Command<'a> {
    Command::new("get")
        .about("Retrieves objects")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .min_values(1),
        )
        .arg(
            Arg::new("HeadObject")
                .help("Retrieves metadata from an object without returning the object itself")
                .long("head")
                .short('H'),
        )
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .help("Don't show progress bar"),
        )
}

pub fn subcommand_share<'a>() -> clap::Command<'a> {
    Command::new("share")
        .about("Share object using a presigned URL")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .min_values(1),
        )
        .arg(
            Arg::new("expire")
                .help("Time period in seconds, max value 604800 (seven days)")
                .long("expire")
                .short('e')
                .default_value("43200")
                .takes_value(true)
                .validator(validators::is_num),
        )
}
