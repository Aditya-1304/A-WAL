use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, WalConfig},
    error::WalError,
    format::{
        record_header::RecordHeader,
        segment_header::{SegmentHeader, compression_algorithms},
    },
    io::{
        directory::{FsSegmentDirectory, NewSegment, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_flags, record_types},
    wal::engine::{SegmentSealPayload, Wal},
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "wal-iterator-it-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: sample_identity(),
            ..WalConfig::default()
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn sample_identity() -> WalIdentity {
    WalIdentity::new(11, 22, 1)
}

fn open_wal(test_dir: &TestDir) -> Wal<FsSegmentDirectory, ()> {
    Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .expect("failed to open wal")
}

fn open_wal_with_config(test_dir: &TestDir, mut config: WalConfig) -> Wal<FsSegmentDirectory, ()> {
    config.dir = test_dir.path().to_path_buf();
    config.identity = sample_identity();

    Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .expect("failed to open wal")
}

fn encode_record(
    record_type: RecordType,
    flags: u16,
    payload: &[u8],
    lsn: Lsn,
    alignment: u32,
) -> Vec<u8> {
    let mut header = RecordHeader::new(
        record_type,
        flags,
        payload.len() as u32,
        lsn,
        RecordHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum(payload).unwrap();

    let mut encoded = header.encode();
    encoded.extend_from_slice(payload);

    let logical_len = header.total_len() as usize;
    let physical_len = if alignment == 0 {
        logical_len
    } else {
        let alignment = alignment as usize;
        let remainder = logical_len % alignment;
        if remainder == 0 {
            logical_len
        } else {
            logical_len + (alignment - remainder)
        }
    };

    encoded.resize(physical_len, 0);
    encoded
}

fn create_manual_segment(
    directory: &FsSegmentDirectory,
    segment_id: u64,
    base_lsn: Lsn,
    compression_algorithm: u8,
    records: &[(RecordType, u16, Vec<u8>, Lsn)],
    alignment: u32,
) {
    let mut header = SegmentHeader::new(
        sample_identity(),
        segment_id,
        base_lsn,
        compression_algorithm,
        SegmentHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum();

    let mut file = directory
        .create_segment(NewSegment {
            segment_id,
            base_lsn,
            header,
        })
        .unwrap();

    for (record_type, flags, payload, lsn) in records {
        let encoded = encode_record(*record_type, *flags, payload, *lsn, alignment);
        file.append_all(&encoded).unwrap();
    }

    file.sync().unwrap();
}

fn decode_seal_payload(bytes: &[u8]) -> SegmentSealPayload {
    assert_eq!(bytes.len(), SegmentSealPayload::ENCODED_LEN);

    let segment_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let record_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let logical_bytes = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

    SegmentSealPayload {
        segment_id,
        record_count,
        logical_bytes,
    }
}

#[test]
fn iter_from_zero_on_empty_wal_is_empty() {
    let test_dir = TestDir::new("empty");
    let wal = open_wal(&test_dir);

    let mut iter = wal.iter_from(Lsn::ZERO).unwrap();
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn read_at_returns_exact_record_after_sync() {
    let test_dir = TestDir::new("read-at");
    let mut wal = open_wal(&test_dir);

    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.sync().unwrap();

    let record = wal.read_at(lsn).unwrap();

    assert_eq!(record.lsn, lsn);
    assert_eq!(record.record_type, RecordType::new(record_types::USER_MIN));
    assert_eq!(record.payload, b"hello");
    assert_eq!(record.total_len, 37);
}

#[test]
fn read_at_rejects_middle_of_record() {
    let test_dir = TestDir::new("middle");
    let mut wal = open_wal(&test_dir);

    wal.append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.sync().unwrap();

    let err = wal.read_at(Lsn::new(1)).unwrap_err();

    assert!(matches!(err, WalError::LsnOutOfRange { lsn } if lsn == Lsn::new(1)));
}

#[test]
fn iter_from_yields_records_in_order_within_one_segment() {
    let test_dir = TestDir::new("one-segment");
    let mut wal = open_wal(&test_dir);

    let first_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    let second_lsn = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"world!")
        .unwrap();
    wal.sync().unwrap();

    let mut iter = wal.iter_from(first_lsn).unwrap();

    let first = iter.next().unwrap().unwrap();
    let second = iter.next().unwrap().unwrap();
    let third = iter.next().unwrap();

    assert_eq!(first.lsn, first_lsn);
    assert_eq!(first.payload, b"hello");
    assert_eq!(second.lsn, second_lsn);
    assert_eq!(second.payload, b"world!");
    assert!(third.is_none());
}

#[test]
fn iter_from_crosses_segment_rollover_and_yields_segment_seal() {
    let test_dir = TestDir::new("cross-segment");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let mut wal = open_wal_with_config(&test_dir, config);

    let first_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    let second_lsn = wal
        .append(RecordType::new(record_types::USER_MIN + 1), &[2u8; 16])
        .unwrap();
    wal.sync().unwrap();

    let mut iter = wal.iter_from(Lsn::ZERO).unwrap();

    let first = iter.next().unwrap().unwrap();
    let seal = iter.next().unwrap().unwrap();
    let second = iter.next().unwrap().unwrap();
    let end = iter.next().unwrap();

    assert_eq!(first_lsn, Lsn::ZERO);
    assert_eq!(second_lsn, Lsn::new(104));

    assert_eq!(first.record_type, RecordType::new(record_types::USER_MIN));
    assert_eq!(first.payload, vec![1u8; 16]);

    assert_eq!(seal.record_type, record_types::SEGMENT_SEAL);
    assert_eq!(seal.lsn, Lsn::new(48));
    assert_eq!(
        seal.total_len,
        (RecordHeader::ENCODED_LEN + SegmentSealPayload::ENCODED_LEN) as u32
    );

    let seal_payload = decode_seal_payload(&seal.payload);
    assert_eq!(seal_payload.segment_id, 1);
    assert_eq!(seal_payload.record_count, 1);
    assert_eq!(seal_payload.logical_bytes, 48);

    assert_eq!(
        second.record_type,
        RecordType::new(record_types::USER_MIN + 1)
    );
    assert_eq!(second.payload, vec![2u8; 16]);
    assert_eq!(second.lsn, Lsn::new(104));

    assert!(end.is_none());
}

#[test]
fn read_at_can_target_segment_seal_record_after_rollover() {
    let test_dir = TestDir::new("read-seal");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let mut wal = open_wal_with_config(&test_dir, config);

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN + 1), &[2u8; 16])
        .unwrap();
    wal.sync().unwrap();

    let record = wal.read_at(Lsn::new(48)).unwrap();

    assert_eq!(record.record_type, record_types::SEGMENT_SEAL);
    assert_eq!(record.lsn, Lsn::new(48));

    let seal_payload = decode_seal_payload(&record.payload);
    assert_eq!(seal_payload.segment_id, 1);
    assert_eq!(seal_payload.record_count, 1);
    assert_eq!(seal_payload.logical_bytes, 48);
}

#[test]
fn iter_from_written_end_is_empty() {
    let test_dir = TestDir::new("end");
    let mut wal = open_wal(&test_dir);

    wal.append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.sync().unwrap();

    let mut iter = wal.iter_from(wal.next_lsn()).unwrap();
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn read_at_decompresses_lz4_payload_from_segment() {
    let test_dir = TestDir::new("lz4");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let original = b"lz4 payload lz4 payload lz4 payload".to_vec();
    let compressed = lz4_flex::compress_prepend_size(&original);

    create_manual_segment(
        &directory,
        1,
        Lsn::ZERO,
        compression_algorithms::LZ4,
        &[(
            RecordType::new(record_types::USER_MIN),
            record_flags::COMPRESSED,
            compressed.clone(),
            Lsn::ZERO,
        )],
        0,
    );

    let wal = open_wal(&test_dir);
    let record = wal.read_at(Lsn::ZERO).unwrap();

    assert_eq!(record.payload, original);
    assert_eq!(
        record.total_len,
        (RecordHeader::ENCODED_LEN + compressed.len()) as u32
    );
}

#[test]
fn read_at_decompresses_zstd_payload_from_segment() {
    let test_dir = TestDir::new("zstd");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let original = b"zstd payload zstd payload zstd payload".to_vec();
    let compressed = zstd::stream::encode_all(Cursor::new(original.as_slice()), 0).unwrap();

    create_manual_segment(
        &directory,
        1,
        Lsn::ZERO,
        compression_algorithms::ZSTD,
        &[(
            RecordType::new(record_types::USER_MIN),
            record_flags::COMPRESSED,
            compressed.clone(),
            Lsn::ZERO,
        )],
        0,
    );

    let wal = open_wal(&test_dir);
    let record = wal.read_at(Lsn::ZERO).unwrap();

    assert_eq!(record.payload, original);
    assert_eq!(
        record.total_len,
        (RecordHeader::ENCODED_LEN + compressed.len()) as u32
    );
}
