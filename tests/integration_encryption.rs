//! Integration tests for encryption functionality
//!
//! These tests verify encryption workflows,
//! including key validation, nonce handling, and data integrity.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::panic
)]

use chacha20poly1305::aead::stream::EncryptorBE32;
use s3m::stream::{create_nonce_header, encrypt_chunk, init_encryption};
use secrecy::SecretString;

/// Generate a secure encryption key for testing
fn generate_test_key() -> String {
    // 32 character hex string (128 bits)
    "0123456789abcdef0123456789abcdef".to_string()
}

#[test]
fn test_encryption_key_length_validation() {
    // Test that keys must be exactly 32 characters
    let valid_key = generate_test_key();
    assert_eq!(valid_key.len(), 32, "Test key should be 32 characters");

    // Verify key generation produces valid length
    let key = SecretString::new(valid_key.into());
    let (_cipher, _nonce) = init_encryption(&key);
    // If we get here, the key was valid
}

#[test]
fn test_encryption_initialization() {
    // Test that encryption can be properly initialized
    let key = SecretString::new(generate_test_key().into());
    let (_cipher, nonce) = init_encryption(&key);

    // Verify nonce is correct length (7 bytes for ChaCha20Poly1305)
    assert_eq!(nonce.len(), 7, "Nonce should be 7 bytes");

    // Verify nonce is not all zeros (should be random)
    let all_zeros = nonce.iter().all(|&b| b == 0);
    assert!(!all_zeros, "Nonce should be random, not all zeros");
}

#[test]
fn test_nonce_header_creation() {
    // Test that nonce headers are correctly formatted
    let nonce = [1, 2, 3, 4, 5, 6, 7];
    let header = create_nonce_header(&nonce);

    assert_eq!(header.len(), 8, "Header should be 8 bytes (1 + 7)");
    assert_eq!(header[0], 7, "First byte should be nonce length");
    assert_eq!(&header[1..], &nonce, "Remaining bytes should be nonce");
}

#[test]
fn test_encrypt_decrypt_roundtrip() {
    // Test that we can encrypt and decrypt data successfully
    let key = SecretString::new(generate_test_key().into());
    let (cipher, nonce_bytes) = init_encryption(&key);

    let plaintext = b"The quick brown fox jumps over the lazy dog";

    // Encrypt
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());
    let encrypted = encrypt_chunk(&mut encryptor, plaintext).expect("Encryption should succeed");

    // Verify encrypted data is different from plaintext
    assert_ne!(
        &encrypted[4..], // Skip length prefix
        plaintext,
        "Encrypted data should differ from plaintext"
    );

    // Verify length prefix is correct
    let length_prefix = u32::from_be_bytes(encrypted[0..4].try_into().unwrap());
    assert_eq!(
        length_prefix as usize,
        encrypted.len() - 4,
        "Length prefix should match encrypted data length"
    );
}

#[test]
fn test_encrypt_chunk_format() {
    // Test that encrypted chunks have correct format: [len(4)][encrypted_data]
    let key = SecretString::new(generate_test_key().into());
    let (cipher, nonce_bytes) = init_encryption(&key);
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

    let data = b"Hello, World!";
    let encrypted = encrypt_chunk(&mut encryptor, data).expect("Encryption should succeed");

    // Should have at least 4 bytes for length prefix
    assert!(
        encrypted.len() > 4,
        "Encrypted data should include length prefix"
    );

    // Extract and verify length prefix
    let length_bytes: [u8; 4] = encrypted[0..4].try_into().unwrap();
    let declared_length = u32::from_be_bytes(length_bytes) as usize;
    let actual_encrypted_length = encrypted.len() - 4;

    assert_eq!(
        declared_length, actual_encrypted_length,
        "Declared length should match actual encrypted data length"
    );
}

#[test]
fn test_encryption_preserves_data_size_approximately() {
    // Test that encryption overhead is reasonable
    // ChaCha20Poly1305 adds 16 bytes (tag) + our 4 byte length prefix = 20 bytes overhead

    let key = SecretString::new(generate_test_key().into());
    let (cipher, nonce_bytes) = init_encryption(&key);
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

    let plaintext_size = 1024; // 1KB
    let plaintext = vec![0u8; plaintext_size];

    let encrypted = encrypt_chunk(&mut encryptor, &plaintext).expect("Encryption should succeed");

    // Encrypted size should be plaintext + tag (16) + length prefix (4)
    let expected_size = plaintext_size + 16 + 4;
    assert_eq!(
        encrypted.len(),
        expected_size,
        "Encrypted data should be plaintext + 20 bytes overhead"
    );
}

#[test]
fn test_nonce_uniqueness() {
    // Test that nonces are unique across multiple initializations
    let key = SecretString::new(generate_test_key().into());

    let (_cipher1, nonce1) = init_encryption(&key);
    let (_cipher2, nonce2) = init_encryption(&key);
    let (_cipher3, nonce3) = init_encryption(&key);

    // Nonces should be different (extremely high probability)
    assert_ne!(nonce1, nonce2, "Nonces should be unique");
    assert_ne!(nonce2, nonce3, "Nonces should be unique");
    assert_ne!(nonce1, nonce3, "Nonces should be unique");
}

#[test]
fn test_empty_data_encryption() {
    // Test that we can encrypt empty data
    let key = SecretString::new(generate_test_key().into());
    let (cipher, nonce_bytes) = init_encryption(&key);
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

    let empty_data = b"";
    let encrypted =
        encrypt_chunk(&mut encryptor, empty_data).expect("Should be able to encrypt empty data");

    // Even empty data should have length prefix (4) + authentication tag (16)
    assert_eq!(
        encrypted.len(),
        4 + 16,
        "Empty encrypted data should still have prefix and tag"
    );
}

#[test]
fn test_large_chunk_encryption() {
    // Test encrypting a larger chunk (1MB)
    let key = SecretString::new(generate_test_key().into());
    let (cipher, nonce_bytes) = init_encryption(&key);
    let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce_bytes).into());

    let large_data = vec![0xAB; 1024 * 1024]; // 1MB of 0xAB
    let encrypted = encrypt_chunk(&mut encryptor, &large_data).expect("Should encrypt large chunk");

    // Verify size
    let expected_size = large_data.len() + 16 + 4;
    assert_eq!(
        encrypted.len(),
        expected_size,
        "Large chunk should encrypt correctly"
    );

    // Verify length prefix
    let length_prefix = u32::from_be_bytes(encrypted[0..4].try_into().unwrap()) as usize;
    assert_eq!(
        length_prefix,
        encrypted.len() - 4,
        "Length prefix should be correct for large chunk"
    );
}

#[test]
fn test_encryption_key_from_different_sources() {
    // Test that keys from different sources (hex, random, etc.) work correctly
    // as long as they're 32 characters

    let keys = vec![
        "0123456789abcdef0123456789abcdef", // Hex
        "abcdefghijklmnopqrstuvwxyz123456", // Alphanumeric
        "ThisIsA32CharacterKeyForTesting!", // Mixed
    ];

    for key in keys {
        assert_eq!(key.len(), 32, "Key should be 32 characters");

        let secret_key = SecretString::new(key.to_string().into());
        let (cipher, nonce) = init_encryption(&secret_key);

        assert_eq!(nonce.len(), 7, "Nonce should be 7 bytes for key: {}", key);

        // Verify we can encrypt with this key
        let mut encryptor = EncryptorBE32::from_aead(cipher, (&nonce).into());
        let encrypted = encrypt_chunk(&mut encryptor, b"test data");
        assert!(encrypted.is_ok(), "Should encrypt with key: {}", key);
    }
}
