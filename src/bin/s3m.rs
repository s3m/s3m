use clap::{App, AppSettings, Arg, SubCommand};
use s3m::s3::{actions, Credentials, Region, S3};
use s3m::s3m::Config;

use std::fs::{create_dir_all, metadata, File};
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

#[tokio::main]
async fn main() {
    let home_dir = match dirs::home_dir() {
        Some(h) => h.display().to_string(),
        None => "~".to_string(),
    };

    create_dir_all(format!("{}/.s3m/", home_dir)).unwrap_or_else(|e| {
        eprintln!("Unable to create ~/.s3m dir: {}", e);
        process::exit(1);
    });

    let default_config = format!("{}/.s3m/config.yml", home_dir);

    let matches = App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::SubcommandsNegateReqs)
        .arg(
            Arg::with_name("config")
                .help("config.yml")
                .long("config")
                .default_value(&default_config)
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
        .arg(
            Arg::with_name("arguments")
                .required_unless("ls")
                .min_values(1),
        )
        .subcommand(
            SubCommand::with_name("ls")
                .about("list objects")
                .arg(Arg::with_name("arguments").required(true).min_values(1)),
        )
        .get_matches();

    let config = matches.value_of("config").unwrap_or_else(|| {
        eprintln!("Unable to open configuration file, use (\"-h for help\")");
        process::exit(1);
    });

    // TODO clean up this
    // unwrap because field "arguments" is required (should never fail)
    let args: Vec<_> = if let Some(m) = matches.subcommand_matches("ls") {
        m.values_of("arguments").unwrap().collect()
    } else {
        matches.values_of("arguments").unwrap().collect()
    };

    // parse config file
    let file = File::open(config).expect("Unable to open file");
    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            eprintln!("Error parsing configuration file: {}", e);
            process::exit(1);
        }
        Ok(yml) => yml,
    };

    // find host, bucket and path
    let mut hbp: Vec<_> = args[0].split('/').filter(|s| !s.is_empty()).collect();

    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp.remove(0);
        &config.hosts[key]
    } else {
        eprintln!("No \"host\" found, check ~/.s3m/config.yml");
        process::exit(1);
    };

    // find region
    let region = match &host.region {
        Some(h) => match h.parse::<Region>() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", e);
                process::exit(1);
            }
        },
        None => match &host.endpoint {
            Some(r) => Region::Custom {
                name: "".to_string(),
                endpoint: r.to_string(),
            },
            None => {
                eprintln!("Error parsing host need an endpoint or region");
                process::exit(1);
            }
        },
    };

    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    let bucket = if !hbp.is_empty() {
        Some(hbp.remove(0).to_string())
    } else if matches.subcommand_matches("ls").is_some() {
        None
    } else {
        eprintln!("No \"bucket\" found, try /{}/<bucket name>", args[0]);
        process::exit(1);
    };

    let s3 = S3::new(&credentials, &region, bucket.clone());

    if matches.subcommand_matches("ls").is_some() {
        if bucket.is_some() {
            let mut action = actions::ListObjectsV2::new();
            action.prefix = Some(String::from(""));
            match action.request(s3).await {
                Ok(o) => println!("objects: {:#?}", o),
                Err(e) => eprintln!("Error parsing output: {}", e),
            }
        } else {
            // list buckets
            let action = actions::ListBuckets::new();
            match action.request(s3).await {
                Ok(o) => println!("objects: {:#?}", o),
                Err(e) => eprintln!("Error parsing output: {}", e),
            }
        }
    } else {
        // Test Put
        let action = actions::PutObject::new("x.pdf".to_string(), "/tmp/x.pdf".to_string());
        match action.request(s3).await {
            Ok(o) => println!("objects: {:#?}", o),
            Err(e) => eprintln!("Err: {}", e),
        }
    }
}
