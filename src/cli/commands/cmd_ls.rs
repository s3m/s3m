use clap::{Arg, Command};

pub fn subcommand_ls() -> Command {
    Command::new("ls")
        .about("List objects and in-progress multipart uploads")
        .arg(
            Arg::new("arguments")
                .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                .required(true)
                .num_args(1),
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
                .num_args(1),
        )
        .arg(
            Arg::new("start-after")
                .help("Starts listing after this specified key")
                .long("start-after")
                .short('a')
                .num_args(1),
        )
}
