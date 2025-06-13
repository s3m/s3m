use crate::cli::progressbar::Bar;
use anyhow::{anyhow, Context, Result};
use chacha20poly1305::{
    aead::{generic_array::GenericArray, stream::DecryptorBE32, KeyInit},
    ChaCha20Poly1305,
};
use std::{
    fs::File,
    io::{self, Read, Write}, // Import io for specific error types
    path::PathBuf,
};

/// # Errors
/// Will return an error if the action fails
pub fn decrypt(enc_file: &PathBuf, enc_key: &str) -> Result<()> {
    let mut encrypted_file = File::open(enc_file)
        .with_context(|| format!("Failed to open encrypted file: {}", enc_file.display()))?;

    if enc_key.len() != 32 {
        return Err(anyhow!("Encryption key must be 32 characters long."));
    }

    let decrypted_file = enc_file.with_extension("decrypted");

    let mut decrypted_file = File::create(&decrypted_file).with_context(|| {
        format!(
            "Failed to create decrypted file: {}",
            decrypted_file.display()
        )
    })?;

    // get the file_size in bytes by using the content_length
    let file_size = encrypted_file
        .metadata()
        .with_context(|| format!("Failed to get metadata for file: {}", enc_file.display()))?
        .len();

    // if quiet is true, then use a default progress bar
    let pb = Bar::new(file_size);

    // get the nonce length byte
    let mut nonce_len_buf = [0u8; 1];
    encrypted_file
        .read_exact(&mut nonce_len_buf)
        .context("Failed to read nonce length byte")?;

    // The nonce length is expected to be 7 bytes for ChaCha20Poly1305
    let nonce_len = nonce_len_buf[0] as usize;
    if nonce_len != 7 {
        return Err(anyhow!("Expected nonce length 7, got {}", nonce_len));
    }

    // Read the nonce bytes
    let mut nonce = vec![0u8; nonce_len];
    encrypted_file
        .read_exact(&mut nonce)
        .context("Failed to read nonce bytes")?;

    let cipher = ChaCha20Poly1305::new(enc_key.as_bytes().into());
    let mut decryptor = DecryptorBE32::from_aead(cipher, GenericArray::from_slice(&nonce));

    let mut chunk_idx = 0;
    let mut total_decrypted_bytes = 0u64;

    loop {
        chunk_idx += 1;
        let mut len_buf = [0u8; 4];

        match encrypted_file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                return Err(e).context(format!(
                    "Chunk {}: Failed to read encrypted length",
                    chunk_idx
                ));
            }
        }

        let chunk_len = u32::from_be_bytes(len_buf) as usize;

        let mut encrypted_chunk = vec![0u8; chunk_len];
        encrypted_file
            .read_exact(&mut encrypted_chunk)
            .with_context(|| format!("Chunk {}: Failed to read encrypted chunk", chunk_idx))?;

        let mut decrypted_chunk = encrypted_chunk.clone();
        decryptor
            .decrypt_next_in_place(&[], &mut decrypted_chunk)
            .map_err(|e| anyhow::anyhow!("Chunk {}: Decryption failed: {:?}", chunk_idx, e))?;

        decrypted_file
            .write_all(&decrypted_chunk)
            .with_context(|| format!("Chunk {}: Failed to write decrypted chunk", chunk_idx))?;

        total_decrypted_bytes += decrypted_chunk.len() as u64;

        if let Some(pb) = pb.progress.as_ref() {
            pb.set_position(total_decrypted_bytes);
        }
    }

    if let Some(pb) = pb.progress.as_ref() {
        pb.finish();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bad_enc_key() {
        let enc_file = PathBuf::from("testdata/encrypted_file.enc");
        let enc_key = "abc";

        let result = decrypt(&enc_file, enc_key);
        assert!(result.is_err(), "Decryption should fail with short key");
    }

    #[test]
    fn test_nonexistent_file() {
        let enc_file = PathBuf::from("testdata/nonexistent_file.enc");
        let enc_key = "01234567890123456789012345678901";

        let result = decrypt(&enc_file, enc_key);
        assert!(
            result.is_err(),
            "Decryption should fail with nonexistent file"
        );
    }

    #[test]
    fn test_decrypt() {
        let enc_file = PathBuf::from("testdata/encrypted_file.enc");
        let enc_key = "01234567890123456789012345678901";

        // Ensure the encrypted file exists
        assert!(enc_file.exists(), "Encrypted file does not exist");

        // Call the decrypt function
        let result = decrypt(&enc_file, enc_key);
        assert!(result.is_ok(), "Decryption failed: {:?}", result);

        // Check if the decrypted file was created
        let decrypted_file = enc_file.with_extension("decrypted");
        assert!(decrypted_file.exists(), "Decrypted file was not created");

        // check content of decrypted file
        let mut decrypted_content = String::new();
        File::open(&decrypted_file)
            .expect("Failed to open decrypted file")
            .read_to_string(&mut decrypted_content)
            .expect("Failed to read decrypted file");
        assert!(!decrypted_content.is_empty(), "Decrypted file is empty");
        assert_eq!(
            decrypted_content,
            "The quick brown fox jumps over the lazy dog.\n"
        );

        // Clean up test files
        fs::remove_file(decrypted_file).unwrap();
    }
}
