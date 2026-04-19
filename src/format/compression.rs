use std::io::Cursor;

use crate::{error::WalError, format::segment_header::compression_algorithms};

pub fn decompress_payload(
    compression_algorithm: u8,
    is_compressed: bool,
    payload: &[u8],
) -> Result<Vec<u8>, WalError> {
    if !is_compressed {
        return Ok(payload.to_vec());
    }

    match compression_algorithm {
        compression_algorithms::NONE => Err(WalError::decompression_error(
            "record is marked compressed but segement compression algorithm is NONE",
        )),

        compression_algorithms::LZ4 => {
            lz4_flex::decompress_size_prepended(payload).map_err(|err| {
                WalError::decompression_error(format!("lz4 decompression failed: {err}"))
            })
        }

        compression_algorithms::ZSTD => {
            zstd::stream::decode_all(Cursor::new(payload)).map_err(|err| {
                WalError::decompression_error(format!("zstd decompression failed: {err}"))
            })
        }
        other => Err(WalError::UnsupportedCompressionAlgorithm { found: other }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncompressed_payload_is_returned_unchanged() {
        let payload = b"hello wal";

        let decoded = decompress_payload(compression_algorithms::NONE, false, payload).unwrap();

        assert_eq!(decoded, payload);
    }

    #[test]
    fn compressed_flag_with_none_algorithm_is_rejected() {
        let err = decompress_payload(compression_algorithms::NONE, true, b"payload").unwrap_err();

        assert!(matches!(err, WalError::DecompressionError { .. }));
    }

    #[test]
    fn lz4_payload_decompresses_successfully() {
        let original = b"this payload should compress this payload should compress";
        let compressed = lz4_flex::compress_prepend_size(original);

        let decoded = decompress_payload(compression_algorithms::LZ4, true, &compressed).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn zstd_payload_decompresses_successfully() {
        let original = b"this payload should also compress under zstd";
        let compressed = zstd::stream::encode_all(Cursor::new(original), 0).unwrap();

        let decoded = decompress_payload(compression_algorithms::ZSTD, true, &compressed).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn unsupported_algorithm_is_rejected() {
        let err = decompress_payload(99, true, b"payload").unwrap_err();

        assert!(matches!(
            err,
            WalError::UnsupportedCompressionAlgorithm { found: 99 }
        ));
    }

    #[test]
    fn malformed_compressed_payload_returns_decompression_error() {
        let err = decompress_payload(compression_algorithms::LZ4, true, b"not-lz4").unwrap_err();

        assert!(matches!(err, WalError::DecompressionError { .. }));
    }
}
