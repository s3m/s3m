use clap::{App, AppSettings, Arg, SubCommand};
use s3m::s3::{actions, Credentials, Region, S3};
use s3m::s3m::{multipart_upload, upload, Config};
use std::env;
use std::fs::{create_dir_all, metadata, File};
use std::process;

const MAX_PARTS_PER_UPLOAD: u64 = 10_000;
const MAX_PART_SIZE: u64 = 5_368_709_120;

fn me() -> Option<String> {
    std::env::current_exe()
        .ok()?
        .file_name()?
        .to_str()?
        .to_owned()
        .into()
}

fn is_num(s: String) -> Result<(), String> {
    if let Err(..) = s.parse::<u64>() {
        return Err(String::from("Not a valid number!"));
    }
    Ok(())
}

fn is_file(s: String) -> Result<(), String> {
    if metadata(&s).map_err(|e| e.to_string())?.is_file() {
        Ok(())
    } else {
        Err(format!(
            "cannot read the file: {}, verify file exist and is not a directory.",
            s
        ))
    }
}

fn file_size(path: &str) -> Result<u64, String> {
    metadata(path)
        .map(|m| {
            if m.is_file() {
                Ok(m.len())
            } else {
                Err(format!(
                    "cannot read the file: {}, verify file exist and is not a directory.",
                    path
                ))
            }
        })
        .map_err(|e| e.to_string())?
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
    let default_threads = if num_cpus::get() > 8 {
        String::from("8")
    } else {
        format!("{}", num_cpus::get())
    };

    let matches = App::new("s3m")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::SubcommandsNegateReqs)
        .arg(
            Arg::with_name("buffer")
                .help("Part size in bytes, max value: 5 GB (5,368,709,120 bytes)")
                .long("buffer")
                .default_value("10485760")
                .short("b")
                .required(true)
                .validator(is_num),
        )
        .arg(
            Arg::with_name("threads")
                .help("Number of threads to use")
                .long("threads")
                .default_value(&default_threads)
                .short("t")
                .required(true)
                .validator(is_num),
        )
        .arg(
            Arg::with_name("multipart")
                .short("m")
                .long("multipart")
                .help(
                    "Multipart upload, used when reading from stdin or file > part size (--buffer)",
                ),
        )
        .arg(
            Arg::with_name("config")
                .help("config.yml")
                .long("config")
                .default_value(&default_config)
                .short("c")
                .required(true)
                .value_name("config.yml")
                .validator(is_file),
        )
        .arg(
            Arg::with_name("arguments")
                .help("/path/to/file <host>/bucket/<file>")
                .required_unless("ls")
                .min_values(2),
        )
        .subcommand(
            SubCommand::with_name("ls").about("list objects").arg(
                Arg::with_name("arguments")
                    .help("\"host\" to list buckets or \"host/bucket\" to list bucket contents")
                    .required(true)
                    .min_values(1),
            ),
        )
        .get_matches();

    // parse config file
    let config = matches.value_of("config").unwrap();
    let file = File::open(config).expect("Unable to open file");
    let config: Config = match serde_yaml::from_reader(file) {
        Err(e) => {
            eprintln!("Error parsing configuration file: {}", e);
            process::exit(1);
        }
        Ok(yml) => yml,
    };

    let buffer = matches.value_of("buffer").unwrap();
    let threads = matches
        .value_of("threads")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    // unwrap because field "arguments" is required (should never fail)
    let args: Vec<_> = if let Some(ls) = matches.subcommand_matches("ls") {
        ls.values_of("arguments").unwrap().collect()
    } else {
        matches.values_of("arguments").unwrap().collect()
    };

    // find host, bucket and path
    let mut hbp: Vec<_> = if matches.subcommand_matches("ls").is_some() {
        args[0].split('/').filter(|s| !s.is_empty()).collect()
    } else {
        args[1].split('/').filter(|s| !s.is_empty()).collect()
    };

    // HOST
    let host = if config.hosts.contains_key(hbp[0]) {
        let key = hbp.remove(0);
        &config.hosts[key]
    } else {
        eprintln!("No \"host\" found, check ~/.s3m/config.yml");
        process::exit(1);
    };

    // REGION
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
        eprintln!(
            "No \"bucket\" found, try: {} /path/to/file {}/<bucket name>/file",
            me().unwrap_or_else(|| "s3m".to_string()),
            args[1]
        );
        process::exit(1);
    };

    let s3 = S3::new(&credentials, &region, bucket.clone());

    if matches.subcommand_matches("ls").is_some() {
        if bucket.is_some() {
            let mut action = actions::ListObjectsV2::new();
            action.prefix = Some(String::from(""));
            match action.request(&s3).await {
                Ok(o) => println!("objects: {:#?}", o),
                Err(e) => eprintln!("{}", e),
            }
        } else {
            // list buckets
            let action = actions::ListBuckets::new();
            match action.request(&s3).await {
                Ok(o) => println!("objects: {:#?}", o),
                Err(e) => eprintln!("{}", e),
            }
        }
    } else {
        // Upload a file if > buffer size try multipart
        if hbp.is_empty() {
            eprintln!(
                "File name missing, try: {} {} <provider>/<bucket>/{}",
                me().unwrap_or_else(|| "s3m".to_string()),
                args[0],
                args[0].split('/').next_back().unwrap_or("")
            );
            process::exit(1);
        }

        let file_size = match file_size(args[0]) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{}", e);
                process::exit(1);
            }
        };

        // <https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/>
        if file_size > 5_497_558_138_880 {
            eprintln!("object size limit 5 TB");
            process::exit(1);
        }

        // unwrap because of previous is_num validator
        let mut chunk_size = buffer.parse::<u64>().unwrap();

        // calculate the chunk size
        let mut parts = file_size / chunk_size;
        while parts > MAX_PARTS_PER_UPLOAD {
            chunk_size *= 2;
            parts = file_size / chunk_size;
        }

        if chunk_size > MAX_PART_SIZE {
            eprintln!("max part size 5 GB");
            process::exit(1);
        }

        if file_size > chunk_size {
            match multipart_upload(
                s3,
                hbp[0].into(),
                args[0].into(),
                file_size,
                chunk_size,
                threads,
            )
            .await
            {
                Ok(o) => println!("{}", o),
                Err(e) => eprintln!("{}", e),
            }
        } else {
            match upload(s3, hbp[0].into(), args[0].into(), file_size).await {
                Ok(o) => println!("{}", o),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
