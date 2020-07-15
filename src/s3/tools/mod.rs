use ring::{
    digest,
    digest::{Context, SHA256},
    hmac,
};
use std::error::Error;
use std::fmt::Write;
use std::fs;
use std::io::{BufRead, BufReader};

pub fn sha256_digest(file_path: &str) -> Result<(String, usize), Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut context = Context::new(&SHA256);
    let mut length: usize = 0;

    loop {
        let consummed = {
            let buffer = reader.fill_buf()?;
            if buffer.is_empty() {
                break;
            }
            context.update(buffer);
            buffer.len()
        };
        length = length + consummed;
        reader.consume(consummed);
    }

    let digest = context.finish();

    Ok((write_hex_bytes(digest.as_ref()), length))
}

#[must_use]
pub fn sha256_digest_string(string: &str) -> String {
    write_hex_bytes(digest::digest(&digest::SHA256, string.as_bytes()).as_ref())
}

#[must_use]
pub fn sha256_hmac(key: &[u8], msg: &[u8]) -> hmac::Tag {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&s_key, msg)
}

#[must_use]
pub fn write_hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();
    for byte in bytes {
        write!(&mut s, "{:02x}", byte).expect("Unable to write");
    }
    s
}
