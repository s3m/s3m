use clap::{App, Arg, SubCommand};
use s3m::s3::{actions, Credentials, Region, S3};
use s3m::s3m::{Config, Host};

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
        .arg(Arg::with_name("arguments").required(true).min_values(1))
        .subcommand(SubCommand::with_name("ls").about("list objects"))
        .get_matches();

    let config = matches.value_of("config").unwrap_or_else(|| {
        eprintln!("Unable to open configuration file, use (\"-h for help\")");
        process::exit(1);
    });

    // unwrap because field "arguments" is required (should never fail)
    let args: Vec<_> = matches.values_of("arguments").unwrap().collect();

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
    let mut hbp = args[0]
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>();

    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp[0];
        hbp.remove(0);
        &config.hosts[key]
    } else {
        &config.hosts["default"]
    };

    let region = match get_host(&host) {
        Ok(h) => h,
        Err(_) => {
            eprintln!("Error parsing host need an endpoint or region");
            process::exit(1);
        }
    };
    println!("hbp: {:#?}", hbp.len());
    println!("region: {}", region);
    todo!();

    let credentials = Credentials::new(&host.access_key, &host.secret_key);

    //let region = Region::Custom {
    //name: "".to_string(),
    //endpoint: "s11s3.swisscom.com".to_string(),
    //};
    let region = Region::default();
    println!("region: {}", region.endpoint());
    let bucket = String::from("s3mon");
    let s3 = S3::new(&bucket, &credentials, &region);

    // Test List bucket
    let mut action = actions::ListObjectsV2::new();
    action.prefix = Some(String::from(""));
    if let Ok(objects) = action.request(s3.clone()).await {
        println!("objects: {:#?}", objects);
    }

    // Test Put
    let action = actions::PutObject::new("x.pdf".to_string(), "/tmp/x.pdf".to_string());
    match action.request(s3.clone()).await {
        Ok(o) => println!("objects: {:#?}", o),
        Err(e) => eprintln!("Err: {}", e),
    }
}

fn get_host(host: &Host) -> Result<String, ()> {
    match &host.region {
        Some(r) => Ok(r.to_string()),
        None => match &host.endpoint {
            Some(r) => Ok(r.to_string()),
            None => Err(()),
        },
    }
}
