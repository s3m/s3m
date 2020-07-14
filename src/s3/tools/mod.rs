use ring::{
    digest,
    digest::{Context, SHA256},
    hmac,
};
use std::error::Error;
use std::fmt::Write;
use std::fs;
use std::io::{BufRead, BufReader, Read};

pub fn sha256_digest(file_path: &str) -> Result<(String, Vec<u8>), Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut context = Context::new(&SHA256);
    let mut file_content = Vec::new();

    loop {
        let consummed = {
            let mut buffer = reader.fill_buf()?;
            if buffer.is_empty() {
                break;
            }
            context.update(buffer);
            buffer.read_to_end(&mut file_content)?
        };
        reader.consume(consummed);
    }

    let digest = context.finish();

    Ok((write_hex_bytes(digest.as_ref()), file_content))
}

pub fn sha256_digest_string(string: &str) -> String {
    write_hex_bytes(digest::digest(&digest::SHA256, string.as_bytes()).as_ref())
}

pub fn sha256_hmac(key: &[u8], msg: &[u8]) -> hmac::Tag {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&s_key, msg)
}

pub fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
