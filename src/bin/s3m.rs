use clap::{App, AppSettings, Arg, SubCommand};
use indicatif::{ProgressBar, ProgressStyle};
use s3m::s3::{actions, tools, Credentials, Region, S3};
use s3m::s3m::{multipart_upload, upload, Config, Stream};
use std::env;
use std::fs::{create_dir_all, metadata, remove_dir_all, File};
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
        .after_help(format!("The checksum of the file is calculated before uploading it and is used to keep a reference of where the file has been uploaded to prevent uploading it again, this is stored in [{}/.s3m/streams] use the option (-r) to clean up the directory.\n\nIf the file is bigger than the buffer size (-b 10MB default) is going to be uploaded in parts. The upload process can be interrupted at any time and in the next attempt, it will be resumed in the position that was left when possible.\n\nhttps://s3m.stream", home_dir).as_ref())
        .arg(
            Arg::with_name("remove")
                .short("r")
                .long("remove")
                .help(&format!("remove {}/.s3m/streams directory", home_dir)),
        )
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
                .required_unless_one(&["ls", "remove"])
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

    if matches.is_present("remove") {
        let streams = format!("{}/.s3m/streams", home_dir);
        if let Err(e) = remove_dir_all(&streams) {
            eprintln!("{}: {}", streams, e);
        }
        return;
    }

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

        // progress bar for the checksum
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(200);
        pb.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(&[
                    "\u{2801}", "\u{2802}", "\u{2804}", "\u{2840}", "\u{2880}", "\u{2820}",
                    "\u{2810}", "\u{2808}", "",
                ])
                .template("checksum: {msg}{spinner:.green}"),
        );
        let checksum = match tools::blake3(args[0]) {
            Ok(c) => c,
            Err(e) => {
                pb.finish_and_clear();
                eprintln!(
                    "could not calculate the checksum for file: {}, {}",
                    &args[0], e
                );
                process::exit(1);
            }
        };
        pb.set_message(&checksum);
        pb.finish();

        let key_path = hbp.join("/");

        let db = match Stream::new(&s3, &key_path, &checksum, &home_dir) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("could not create stream tree, {}", e);
                process::exit(1);
            }
        };

        // check if file has been uploded already
        match &db.check() {
            Ok(s) => {
                if let Some(etag) = s {
                    return println!("{}", etag);
                }
            }
            Err(e) => {
                eprintln!("could not query stream tree: {}", e);
                process::exit(1);
            }
        }

        // upload in multipart or in one shot
        if file_size > chunk_size {
            // &hbp[0] is the name of the file
            // &args[0] is the file_path
            match multipart_upload(&s3, &key_path, args[0], file_size, chunk_size, threads, &db)
                .await
            {
                Ok(o) => println!("{}", o),
                Err(e) => eprintln!("{}", e),
            }
        } else {
            match upload(&s3, &key_path, args[0], file_size).await {
                Ok(o) => println!("{}", o),
                Err(e) => eprintln!("{}", e),
            }
        }
    }
}
