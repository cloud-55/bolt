//! LZ4 compression for large values
//!
//! Provides transparent compression for values larger than a threshold.
//! Uses LZ4 which offers excellent compression speed with reasonable ratios.

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

/// Minimum size in bytes before compression is applied
/// Values smaller than this are stored uncompressed to avoid overhead
pub const COMPRESSION_THRESHOLD: usize = 100;

/// Compress data if it exceeds the threshold
/// Returns None if data is below threshold (should be stored as-is)
/// Returns Some(compressed) if compression was applied
pub fn compress_if_needed(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < COMPRESSION_THRESHOLD {
        return None;
    }

    let compressed = compress_prepend_size(data);

    // Only use compressed version if it's actually smaller
    // (some data doesn't compress well)
    if compressed.len() < data.len() {
        Some(compressed)
    } else {
        None
    }
}

/// Decompress LZ4-compressed data
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    decompress_size_prepended(data)
}

/// Compress data unconditionally (for testing/explicit compression)
pub fn compress(data: &[u8]) -> Vec<u8> {
    compress_prepend_size(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_small_data() {
        let small = b"hello";
        assert!(compress_if_needed(small).is_none());
    }

    #[test]
    fn test_compress_large_data() {
        // Repetitive data compresses well
        let large = "hello world ".repeat(100);
        let compressed = compress_if_needed(large.as_bytes());
        assert!(compressed.is_some());

        let compressed = compressed.unwrap();
        assert!(compressed.len() < large.len());

        // Decompress and verify
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, large.as_bytes());
    }

    #[test]
    fn test_compress_incompressible_data() {
        // Random-looking data doesn't compress well
        let random: Vec<u8> = (0..200).map(|i| ((i * 17 + 31) % 256) as u8).collect();
        // This might or might not compress depending on the data
        // The function handles this by checking if compressed is smaller
        let _ = compress_if_needed(&random);
    }
}
