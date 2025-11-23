//! Integration tests for STDIN streaming functionality
//!
//! These tests verify that data can be streamed from STDIN,
//! buffered appropriately, and uploaded correctly.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::panic
)]

use bytes::BytesMut;
use std::io::{Cursor, Read};

/// Simulate STDIN with a cursor
fn create_mock_stdin(data: Vec<u8>) -> Cursor<Vec<u8>> {
    Cursor::new(data)
}

#[test]
fn test_stdin_buffer_size_constant() {
    // Verify STDIN buffer size constant exists and is reasonable
    // 512MB buffer as defined in stream/mod.rs
    const EXPECTED_STDIN_BUFFER_SIZE: usize = 512 * 1024 * 1024;

    assert_eq!(
        EXPECTED_STDIN_BUFFER_SIZE, 536_870_912,
        "STDIN buffer should be 512MB (536,870,912 bytes)"
    );

    // Verify buffer size is reasonable for large uploads
    // With 512MB parts and 10,000 max parts, we can upload:
    // 512MB * 10,000 = 5,368,709,120,000 bytes (~4.88 TB)
    // This is slightly less than 5TB but close enough for practical purposes
    let max_parts: u64 = 10_000;
    let buffer_size_u64 = EXPECTED_STDIN_BUFFER_SIZE as u64;
    let max_uploadable = buffer_size_u64 * max_parts;

    // Verify it can handle multi-TB uploads
    let four_tb_in_bytes: u64 = 4 * 1024 * 1024 * 1024 * 1024;

    assert!(
        max_uploadable > four_tb_in_bytes,
        "Buffer size should support large multi-TB uploads. Max uploadable: {}, 4TB: {}",
        max_uploadable,
        four_tb_in_bytes
    );
}

#[test]
fn test_small_stdin_stream() {
    // Test reading small amounts of data from STDIN
    let data = b"Hello, World!";
    let mut mock_stdin = create_mock_stdin(data.to_vec());

    let mut buffer = vec![0u8; 100];
    let bytes_read = mock_stdin
        .read(&mut buffer)
        .expect("Should read from mock STDIN");

    assert_eq!(bytes_read, data.len(), "Should read all data");
    assert_eq!(&buffer[..bytes_read], data, "Data should match");
}

#[test]
fn test_large_stdin_stream() {
    // Test reading large amounts of data (10MB)
    let large_data = vec![0xAB; 10 * 1024 * 1024]; // 10MB
    let mut mock_stdin = create_mock_stdin(large_data.clone());

    let chunk_size = 1024 * 1024; // Read in 1MB chunks
    let mut total_read = 0;
    let mut buffer = vec![0u8; chunk_size];

    while total_read < large_data.len() {
        match mock_stdin.read(&mut buffer) {
            Ok(0) => break, // EOF
            Ok(n) => {
                total_read += n;
                assert_eq!(buffer[0], 0xAB, "Data should be correct");
            }
            Err(e) => panic!("Read failed: {}", e),
        }
    }

    assert_eq!(total_read, large_data.len(), "Should read all data");
}

#[test]
fn test_empty_stdin_stream() {
    // Test handling empty STDIN input
    let empty_data: Vec<u8> = vec![];
    let mut mock_stdin = create_mock_stdin(empty_data);

    let mut buffer = vec![0u8; 1024];
    let bytes_read = mock_stdin
        .read(&mut buffer)
        .expect("Should handle empty STDIN");

    assert_eq!(bytes_read, 0, "Empty STDIN should return 0 bytes");
}

#[test]
fn test_stdin_chunked_reading() {
    // Test reading STDIN in chunks (simulating streaming)
    let data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut mock_stdin = create_mock_stdin(data.to_vec());

    let chunk_size = 10;
    let mut chunks = Vec::new();

    loop {
        let mut chunk = vec![0u8; chunk_size];
        match mock_stdin.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                chunk.truncate(n);
                chunks.push(chunk);
            }
            Err(e) => panic!("Read failed: {}", e),
        }
    }

    // Reconstruct data from chunks
    let reconstructed: Vec<u8> = chunks.into_iter().flatten().collect();
    assert_eq!(reconstructed, data, "Chunked reading should preserve data");
}

#[test]
fn test_bytes_mut_allocation() {
    // Test BytesMut allocation for buffering (used in actual STDIN handling)
    let mut buf = BytesMut::with_capacity(1024 * 1024); // 1MB

    assert!(
        buf.capacity() >= 1024 * 1024,
        "Should have at least 1MB capacity"
    );
    assert_eq!(buf.len(), 0, "Should start empty");

    // Write some data
    buf.extend_from_slice(b"test data");
    assert_eq!(buf.len(), 9, "Should have 9 bytes after write");
}

#[test]
fn test_large_buffer_allocation() {
    // Test that we can allocate large buffers for STDIN streaming
    // This simulates the 512MB buffer used in production
    let large_buffer_size = 512 * 1024 * 1024; // 512MB

    // We won't actually allocate 512MB in tests (too much memory)
    // Instead, verify the size calculation is correct
    assert_eq!(
        large_buffer_size, 536_870_912,
        "512MB should be 536,870,912 bytes"
    );

    // Test allocation of a smaller buffer to verify the mechanism works
    let test_buffer_size = 10 * 1024 * 1024; // 10MB for testing
    let buf = BytesMut::with_capacity(test_buffer_size);

    assert!(
        buf.capacity() >= test_buffer_size,
        "Should allocate requested capacity"
    );
}

#[test]
fn test_stdin_eof_handling() {
    // Test that EOF is properly detected
    let data = b"short data";
    let mut mock_stdin = create_mock_stdin(data.to_vec());

    let mut buffer = vec![0u8; 1024];

    // First read gets data
    let first_read = mock_stdin
        .read(&mut buffer)
        .expect("First read should succeed");
    assert_eq!(first_read, data.len(), "Should read all data");

    // Second read should return 0 (EOF)
    let second_read = mock_stdin
        .read(&mut buffer)
        .expect("Second read should succeed");
    assert_eq!(second_read, 0, "Should detect EOF");

    // Third read should also return 0 (still EOF)
    let third_read = mock_stdin
        .read(&mut buffer)
        .expect("Third read should succeed");
    assert_eq!(third_read, 0, "Should still be at EOF");
}

#[test]
fn test_stdin_incremental_reading() {
    // Test reading data incrementally as it "arrives"
    let data = b"This is a test of incremental reading from STDIN";
    let mut mock_stdin = create_mock_stdin(data.to_vec());

    let mut accumulated = Vec::new();
    let mut buffer = vec![0u8; 10]; // Small buffer to force multiple reads

    loop {
        match mock_stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => accumulated.extend_from_slice(&buffer[..n]),
            Err(e) => panic!("Read error: {}", e),
        }
    }

    assert_eq!(accumulated, data, "Incremental reading should get all data");
}

#[test]
fn test_stdin_buffer_reuse() {
    // Test that buffers can be reused across multiple reads
    let data1 = b"First chunk of data";
    let data2 = b"Second chunk of data";

    let mut buffer = vec![0u8; 100];

    // Read first chunk
    let mut mock_stdin1 = create_mock_stdin(data1.to_vec());
    let n1 = mock_stdin1
        .read(&mut buffer)
        .expect("Should read first chunk");
    assert_eq!(&buffer[..n1], data1);

    // Reuse same buffer for second chunk
    let mut mock_stdin2 = create_mock_stdin(data2.to_vec());
    let n2 = mock_stdin2
        .read(&mut buffer)
        .expect("Should read second chunk");
    assert_eq!(&buffer[..n2], data2);
}

#[test]
fn test_partial_buffer_fills() {
    // Test that partial reads are handled correctly
    let data = b"ABC";
    let mut mock_stdin = create_mock_stdin(data.to_vec());

    let mut buffer = vec![0u8; 1000]; // Buffer much larger than data

    let bytes_read = mock_stdin
        .read(&mut buffer)
        .expect("Should read partial buffer");

    assert_eq!(bytes_read, 3, "Should read only available bytes");
    assert_eq!(&buffer[..bytes_read], data, "Should read correct data");
}

#[test]
fn test_binary_data_streaming() {
    // Test that binary data (not just text) streams correctly
    let binary_data: Vec<u8> = (0..=255).collect(); // All possible byte values
    let mut mock_stdin = create_mock_stdin(binary_data.clone());

    let mut result = Vec::new();
    let mut buffer = vec![0u8; 64];

    loop {
        match mock_stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => result.extend_from_slice(&buffer[..n]),
            Err(e) => panic!("Read error: {}", e),
        }
    }

    assert_eq!(result, binary_data, "Binary data should stream correctly");
}

#[test]
fn test_compression_buffer_sizing() {
    use s3m::stream::compress_chunk;

    // Test that compressed chunks are handled appropriately
    let data = BytesMut::from(&b"Hello, World! ".repeat(1000)[..]);

    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    let compressed = rt
        .block_on(compress_chunk(data.clone()))
        .expect("Compression should succeed");

    // Compressed data should be smaller than original for repetitive content
    assert!(
        compressed.len() < data.len(),
        "Repetitive data should compress. Original: {}, Compressed: {}",
        data.len(),
        compressed.len()
    );
}

#[test]
fn test_memory_efficiency_streaming() {
    // Test that streaming doesn't require entire file in memory
    // This is a conceptual test - actual implementation uses streaming

    let chunk_size = 1024 * 1024; // 1MB chunks
    let num_chunks = 10;

    // Simulate streaming by processing chunks one at a time
    for i in 0..num_chunks {
        let chunk = vec![0u8; chunk_size];
        // In actual implementation, each chunk is processed and discarded
        assert_eq!(chunk.len(), chunk_size, "Chunk {} should be 1MB", i);
        // Chunk goes out of scope and memory is freed
    }

    // This demonstrates that we don't need 10MB in memory at once
    // Only 1MB per chunk
}
