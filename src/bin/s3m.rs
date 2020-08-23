use indicatif::{ProgressBar, ProgressStyle};
use s3m::s3::{actions, tools, S3};
// use s3m::s3m::{multipart_upload, options, prebuffer, upload, Config, Db};
use s3m::s3m::start;
use std::fs::{create_dir_all, metadata, remove_dir_all, File, Metadata};
use std::process::exit;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PARTS_PER_UPLOAD: u64 = 10_000;
const MAX_PART_SIZE: u64 = 5_368_709_120;

#[tokio::main]
async fn main() {
    let action = match start() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        }
    };

    println!("{:#?}", action);

    /*
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

            let mut chunk_size = buffer.parse::<u64>().unwrap();

            if input_from_stdin {
                prebuffer(chunk_size).await.unwrap();
                return;
            }

            let file_meta = match file_metadata(args[0]) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{}", e);
                    process::exit(1);
                }
            };

            let file_size: u64 = file_meta.len();
            let file_mtime = {
                let mtime = match file_meta.modified() {
                    Ok(mtime) => mtime,
                    Err(_) => SystemTime::now(),
                };
                match mtime.duration_since(UNIX_EPOCH) {
                    Ok(n) => n.as_millis(),
                    Err(e) => {
                        eprintln!("{}", e);
                        process::exit(1);
                    }
                }
            };

            // <https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/>
            if file_size > 5_497_558_138_880 {
                eprintln!("object size limit 5 TB");
                process::exit(1);
            }

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

            let db = match Db::new(&s3, &key_path, &checksum, file_mtime, &home_dir) {
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
                match upload(&s3, &key_path, args[0], file_size, &db).await {
                    Ok(o) => println!("{}", o),
                    Err(e) => eprintln!("{}", e),
                }
            }
        }
    */
}
