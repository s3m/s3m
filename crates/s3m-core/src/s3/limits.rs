//! AWS S3 service limits and constants
//!
//! This module centralizes all AWS S3 service limits to make updates easy
//! when AWS changes their quotas.
//!
//! # References
//! - [S3 Quotas](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html)
//! - [Multipart Upload Overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
//!
//! # When AWS Updates These Limits
//!
//! If AWS increases object size or part limits, update the constants below:
//!
//! 1. Update the constants (e.g., `MAX_OBJECT_SIZE_BYTES`)
//! 2. Update the doc comments with new values and date
//! 3. Run tests: `cargo test`
//! 4. That's it! All code will automatically use new limits.

/// Maximum size of a single S3 object (5 TB as of 2024)
///
/// AWS limit: 5 TiB (5,497,558,138,880 bytes)
///
/// **Last verified**: 2024-01-01
///
/// **Update this when**: AWS announces increased object size limits
pub const MAX_OBJECT_SIZE_BYTES: u64 = 5_497_558_138_880; // 5 TB

/// Maximum size of a single multipart upload part (5 GB as of 2024)
///
/// AWS limit: 5 GiB (5,368,709,120 bytes)
///
/// Each part in a multipart upload (except the last) must be at least 5 MB
/// and at most 5 GB.
///
/// **Last verified**: 2024-01-01
///
/// **Update this when**: AWS announces increased part size limits
pub const MAX_PART_SIZE_BYTES: u64 = 5_368_709_120; // 5 GB

/// Maximum number of parts in a multipart upload (10,000 as of 2024)
///
/// AWS limit: 10,000 parts numbered 1 to 10,000
///
/// This limit has been stable since multipart uploads were introduced.
/// If AWS increases this, you may also need to update the part number type
/// from `u16` to a larger type.
///
/// **Last verified**: 2024-01-01
///
/// **Update this when**: AWS announces increased part count limits
pub const MAX_PARTS_PER_UPLOAD: usize = 10_000;

/// Minimum size of a multipart upload part (5 MB as of 2024)
///
/// AWS limit: 5 MiB (5,242,880 bytes)
///
/// The last part can be smaller than this minimum.
///
/// **Last verified**: 2024-01-01
pub const MIN_PART_SIZE_BYTES: u64 = 5_242_880; // 5 MB

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limits_are_sane() {
        // Verify relationships between limits using const assertions
        const _: () = assert!(MAX_OBJECT_SIZE_BYTES > MAX_PART_SIZE_BYTES);
        const _: () = assert!(MAX_PART_SIZE_BYTES > MIN_PART_SIZE_BYTES);

        // Verify we can upload max object size with max parts
        let max_uploadable = MAX_PART_SIZE_BYTES * MAX_PARTS_PER_UPLOAD as u64;
        assert!(
            max_uploadable >= MAX_OBJECT_SIZE_BYTES,
            "Should be able to upload max object size: {MAX_OBJECT_SIZE_BYTES} bytes \
             with {MAX_PARTS_PER_UPLOAD} parts of {MAX_PART_SIZE_BYTES} bytes each \
             (total capacity: {max_uploadable} bytes)"
        );
    }

    #[test]
    fn test_part_number_type_sufficient() {
        // Verify u16 is sufficient for current MAX_PARTS_PER_UPLOAD
        assert!(
            u16::try_from(MAX_PARTS_PER_UPLOAD).is_ok(),
            "u16 must be able to represent MAX_PARTS_PER_UPLOAD ({MAX_PARTS_PER_UPLOAD})"
        );

        // If this test fails after AWS increases the limit, consider:
        // 1. Using u32 for part numbers if MAX_PARTS_PER_UPLOAD > 65,535
        // 2. Updating BTreeMap<u16, Part> to BTreeMap<u32, Part>
    }

    #[test]
    fn test_documented_values_match_constants() {
        // These tests serve as documentation and will fail if limits change
        assert_eq!(MAX_OBJECT_SIZE_BYTES, 5_497_558_138_880, "5 TB");
        assert_eq!(MAX_PART_SIZE_BYTES, 5_368_709_120, "5 GB");
        assert_eq!(MIN_PART_SIZE_BYTES, 5_242_880, "5 MB");
        assert_eq!(MAX_PARTS_PER_UPLOAD, 10_000, "10,000 parts");
    }
}
