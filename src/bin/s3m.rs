use clap::{App, Arg};
use s3m::s3::{actions, Credentials, Region, S3};
use std::fs::metadata;
use std::process;

#[tokio::main]
async fn main() {
    let matches = App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name("config")
                .help("s3m.yml")
                .long("config")
                .default_value("s3m.yml")
                .short("c")
                .required(true)
                .value_name("FILE")
                .validator(is_file),
        )
        .get_matches();

    let _config = matches.value_of("config").unwrap_or_else(|| {
        eprintln!("Unable to open configuration file, use (\"-h for help\")");
        process::exit(1);
    });

    let credentials = Credentials::new("", "");

    //let region = region::Region::Custom {
    //name: "foo".to_string(),
    // endpoint: "ds11s3.swisscom.com".to_string(),
    //};
    let region = Region::default();
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

    /*
    let file = File::open(&config).expect("Unable to open file");
    let yml: config::Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            eprintln!("Error parsing configuration file: {}", e);
            process::exit(1);
        }
        Ok(yml) => yml,
    };
    println!("{:#?}", yml);
    */
}

fn is_file(s: String) -> Result<(), String> {
    if metadata(&s).map_err(|e| e.to_string())?.is_file() {
        Ok(())
    } else {
        Err(format!("cannot read file: {}", s))
    }
}
