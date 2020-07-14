use ring::digest::{Context, SHA256};
use std::error::Error;
use std::fmt::Write;
use std::fs;
use std::io::{BufRead, BufReader, Read};

pub fn sha256_digest(file_path: &str) -> Result<(String, String), Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut context = Context::new(&SHA256);
    let mut file = String::new();

    loop {
        let consummed = {
            let mut buffer = reader.fill_buf()?;
            if buffer.is_empty() {
                break;
            }
            context.update(buffer);
            buffer.read_to_string(&mut file)?
        };
        reader.consume(consummed);
    }

    let digest = context.finish();

    Ok((write_hex_bytes(digest.as_ref()), file))
}

fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
