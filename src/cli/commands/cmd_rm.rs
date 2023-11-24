use clap::{Arg, Command};

pub fn command() -> Command {
    Command::new("rm")
        .about("Delete objects, bucket (-b) and aborts a multipart upload")
        .arg(
            Arg::new("arguments")
                .help("<s3 provider>/<bucket>/<file>")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("UploadId")
                .help("aborts a multipart upload")
                .long("abort")
                .short('a')
                .num_args(1),
        )
        .arg(
            Arg::new("bucket")
                .help("Delete bucket (All objects in the bucket must be deleted before it can be deleted)")
                .long("bucket")
                .short('b')
                .num_args(0),
        )
}
