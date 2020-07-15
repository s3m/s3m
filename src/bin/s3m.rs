use s3m::{
    options,
    s3::{actions, Credentials, Region, S3},
};

#[tokio::main]
async fn main() {
    let _ = options::new();
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
