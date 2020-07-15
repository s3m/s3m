use clap::{App, Arg, SubCommand};
use dirs;
use std::fs::metadata;
use std::process;

fn is_num(s: String) -> Result<(), String> {
    if let Err(..) = s.parse::<usize>() {
        return Err(String::from("Not a valid number!"));
    }
    Ok(())
}

fn is_file(s: String) -> Result<(), String> {
    if metadata(&s).map_err(|e| e.to_string())?.is_file() {
        Ok(())
    } else {
        Err(format!("cannot read file: {}", s))
    }
}

pub fn new() {
    let home_dir = match dirs::home_dir() {
        Some(h) => h.display().to_string(),
        None => "~".to_string(),
    };
    let home_dir = format!("{}/.s3m/config.yml", home_dir);
    let matches = App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name("config")
                .help("config.yml")
                .long("config")
                .default_value(&home_dir)
                .short("c")
                .required(true)
                .value_name("FILE")
                .validator(is_file),
        )
        .arg(
            Arg::with_name("buffer")
                .help("part size in bytes")
                .long("buffer")
                .default_value("5242880")
                .short("b")
                .required(true)
                .validator(is_num),
        )
        .subcommand(SubCommand::with_name("ls").about("list objects"))
        .get_matches();

    let _config = matches.value_of("config").unwrap_or_else(|| {
        eprintln!("Unable to open configuration file, use (\"-h for help\")");
        process::exit(1);
    });
}
