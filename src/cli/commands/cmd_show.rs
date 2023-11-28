use clap::Command;

pub fn command() -> Command {
    Command::new("show").about("Show available hosts from the config file")
}
