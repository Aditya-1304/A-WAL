use wal::{
    error::WalError,
    format::{
        codec::crc32c,
        record_header::RecordHeader,
        segment_header::{SegmentHeader, checksum_algorithms, compression_algorithms},
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_flags, record_types},
};

fn sample_segment_header() -> SegmentHeader {
    let mut header = SegmentHeader::new(
        WalIdentity::new(11, 22, 1),
        7,
        Lsn::new(4096),
        compression_algorithms::NONE,
        SegmentHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum();
    header
}

fn sample_payload() -> &'static [u8] {
    b"hello wal"
}

fn sample_record_header() -> RecordHeader {
    let mut header = RecordHeader::new(
        record_types::SEGMENT_SEAL,
        record_flags::NONE,
        sample_payload().len() as u32,
        Lsn::new(8192),
        RecordHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum(sample_payload()).unwrap();
    header
}

#[test]
fn crc32c_matches_known_vectors() {
    assert_eq!(crc32c(b""), 0x0000_0000);
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn segment_header_encoded_len_matches_spec() {
    assert_eq!(SegmentHeader::ENCODED_LEN, 68);
}

#[test]
fn record_header_encoded_len_matches_spec() {
    assert_eq!(RecordHeader::ENCODED_LEN, 32);
}

#[test]
fn segment_header_encodes_fields_in_little_endian_order() {
    let mut header = SegmentHeader::new(
        WalIdentity::new(
            0x0102_0304_0506_0708,
            0x1112_1314_1516_1718,
            0x2122_2324_2526_2728,
        ),
        0x3132_3334_3536_3738,
        Lsn::new(0x4142_4344_4546_4748),
        compression_algorithms::LZ4,
        SegmentHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum();

    let bytes = header.encode();

    assert_eq!(&bytes[0..4], &SegmentHeader::MAGIC.to_le_bytes());
    assert_eq!(
        &bytes[4..6],
        &SegmentHeader::SUPPORTED_VERSION.to_le_bytes()
    );
    assert_eq!(
        &bytes[6..8],
        &(SegmentHeader::ENCODED_LEN as u16).to_le_bytes()
    );
    assert_eq!(&bytes[8..16], &0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(&bytes[16..24], &0x1112_1314_1516_1718u64.to_le_bytes());
    assert_eq!(&bytes[24..32], &0x2122_2324_2526_2728u64.to_le_bytes());
    assert_eq!(&bytes[32..40], &0x3132_3334_3536_3738u64.to_le_bytes());
    assert_eq!(&bytes[40..48], &0x4142_4344_4546_4748u64.to_le_bytes());
    assert_eq!(bytes[48], checksum_algorithms::CRC32C);
    assert_eq!(bytes[49], compression_algorithms::LZ4);
    assert_eq!(&bytes[54..68], &[0u8; 14]);
}

#[test]
fn record_header_encodes_fields_in_little_endian_order() {
    let payload = b"payload-1";
    let mut header = RecordHeader::new(
        RecordType::new(0x1234),
        record_flags::COMPRESSED,
        payload.len() as u32,
        Lsn::new(0x0102_0304_0506_0708),
        RecordHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum(payload).unwrap();

    let bytes = header.encode();

    assert_eq!(&bytes[0..4], &RecordHeader::MAGIC.to_le_bytes());
    assert_eq!(&bytes[4..6], &RecordHeader::SUPPORTED_VERSION.to_le_bytes());
    assert_eq!(&bytes[6..8], &0x1234u16.to_le_bytes());
    assert_eq!(
        &bytes[8..10],
        &(RecordHeader::ENCODED_LEN as u16).to_le_bytes()
    );
    assert_eq!(&bytes[10..12], &record_flags::COMPRESSED.to_le_bytes());
    assert_eq!(&bytes[12..16], &(payload.len() as u32).to_le_bytes());
    assert_eq!(&bytes[16..24], &0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(&bytes[28..32], &0u32.to_le_bytes());
}

#[test]
fn segment_header_round_trips_and_reencodes_stably() {
    let header = sample_segment_header();

    let bytes = header.encode();
    let decoded = SegmentHeader::decode(&bytes).unwrap();

    assert_eq!(decoded, header);
    assert_eq!(decoded.encode(), bytes);
    assert!(decoded.verify_checksum(&bytes).is_ok());
}

#[test]
fn record_header_round_trips_and_reencodes_stably() {
    let header = sample_record_header();

    let bytes = header.encode();
    let decoded = RecordHeader::decode(&bytes).unwrap();

    assert_eq!(decoded, header);
    assert_eq!(decoded.encode(), bytes);
    assert!(decoded.verify_checksum(&bytes, sample_payload()).is_ok());
}

#[test]
fn segment_header_identity_accessor_rebuilds_identity_tuple() {
    let header = sample_segment_header();
    assert_eq!(header.identity(), WalIdentity::new(11, 22, 1));
}

#[test]
fn record_header_total_len_is_header_plus_payload() {
    let header = sample_record_header();
    assert_eq!(header.total_len(), 32 + sample_payload().len() as u32);
}

#[test]
fn record_header_is_compressed_checks_flag_bit() {
    let header = RecordHeader::new(
        record_types::SEGMENT_SEAL,
        record_flags::COMPRESSED,
        4,
        Lsn::new(1),
        RecordHeader::SUPPORTED_VERSION,
    );

    assert!(header.is_compressed());
}

#[test]
fn segment_header_decode_rejects_short_read() {
    let bytes = [0u8; 10];
    let err = SegmentHeader::decode(&bytes).unwrap_err();

    assert!(matches!(err, WalError::ShortRead));
}

#[test]
fn record_header_decode_rejects_short_read() {
    let bytes = [0u8; 10];
    let err = RecordHeader::decode(&bytes).unwrap_err();

    assert!(matches!(err, WalError::ShortRead));
}

#[test]
fn segment_header_decode_rejects_bad_magic() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[0] ^= 0xFF;

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadMagic { .. }));
}

#[test]
fn record_header_decode_rejects_bad_magic() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[0] ^= 0xFF;

    let err = RecordHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadMagic { .. }));
}

#[test]
fn segment_header_decode_rejects_unsupported_version() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[4..6].copy_from_slice(&999u16.to_le_bytes());

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        WalError::UnsupportedVersion { found: 999, .. }
    ));
}

#[test]
fn record_header_decode_rejects_unsupported_version() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[4..6].copy_from_slice(&999u16.to_le_bytes());

    let err = RecordHeader::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        WalError::UnsupportedVersion { found: 999, .. }
    ));
}

#[test]
fn segment_header_decode_rejects_bad_header_len() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[6..8].copy_from_slice(&0u16.to_le_bytes());

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadSegmentHeader));
}

#[test]
fn record_header_decode_rejects_bad_header_len() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[8..10].copy_from_slice(&0u16.to_le_bytes());

    let err = RecordHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadRecordHeader));
}

#[test]
fn segment_header_decode_rejects_unsupported_checksum_algorithm() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[48] = 99;

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        WalError::UnsupportedChecksumAlgorithm { found: 99 }
    ));
}

#[test]
fn segment_header_decode_rejects_unsupported_compression_algorithm() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[49] = 99;

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        WalError::UnsupportedCompressionAlgorithm { found: 99 }
    ));
}

#[test]
fn segment_header_decode_rejects_non_zero_reserved_bytes() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[67] = 1;

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadSegmentHeader));
}

#[test]
fn record_header_decode_rejects_unknown_flag_bits() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[10..12].copy_from_slice(&0x0002u16.to_le_bytes());

    let err = RecordHeader::decode(&bytes).unwrap_err();
    assert!(matches!(
        err,
        WalError::UnsupportedRecordFlags { found: 0x0002 }
    ));
}

#[test]
fn record_header_decode_rejects_non_zero_reserved() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[28..32].copy_from_slice(&1u32.to_le_bytes());

    let err = RecordHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::BadRecordHeader));
}

#[test]
fn segment_header_tampering_is_detected_by_checksum() {
    let header = sample_segment_header();
    let mut bytes = header.encode();
    bytes[8] ^= 0x01;

    let err = SegmentHeader::decode(&bytes).unwrap_err();
    assert!(matches!(err, WalError::ChecksumMismatch { lsn: None }));
}

#[test]
fn record_header_detects_tampered_payload() {
    let header = sample_record_header();
    let bytes = header.encode();

    let err = header.verify_checksum(&bytes, b"jello wal").unwrap_err();

    assert!(matches!(
        err,
        WalError::ChecksumMismatch { lsn: Some(lsn) } if lsn == Lsn::new(8192)
    ));
}

#[test]
fn record_header_detects_tampered_header_bytes() {
    let header = sample_record_header();
    let mut bytes = header.encode();
    bytes[16] ^= 0x01;

    let decoded = RecordHeader::decode(&bytes).unwrap();
    let err = decoded
        .verify_checksum(&bytes, sample_payload())
        .unwrap_err();

    assert!(matches!(err, WalError::ChecksumMismatch { lsn: Some(_) }));
}

#[test]
fn record_checksum_depends_on_header_bytes_not_only_payload() {
    let payload = b"same payload";

    let mut first = RecordHeader::new(
        record_types::SEGMENT_SEAL,
        record_flags::NONE,
        payload.len() as u32,
        Lsn::new(100),
        RecordHeader::SUPPORTED_VERSION,
    );
    first.finalize_checksum(payload).unwrap();

    let mut second = RecordHeader::new(
        record_types::SEGMENT_SEAL,
        record_flags::NONE,
        payload.len() as u32,
        Lsn::new(200),
        RecordHeader::SUPPORTED_VERSION,
    );
    second.finalize_checksum(payload).unwrap();

    assert_ne!(first.checksum, second.checksum);
}
