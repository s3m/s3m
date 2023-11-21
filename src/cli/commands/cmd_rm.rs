use clap::{Arg, Command};

pub fn subcommand_rm() -> Command {
    Command::new("rm")
        .about("Delete objects and aborts a multipart upload")
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
}
