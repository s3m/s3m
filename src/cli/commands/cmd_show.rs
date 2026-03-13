use clap::Command;

pub fn command() -> Command {
    Command::new("show")
        .about("Show available hosts from the config file")
        .after_long_help("Example:\n  s3m show")
}
