use std::{
    fs,
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
    wal::engine::Wal,
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
            "wal-recovery-tail-{prefix}-{}-{nanos}",
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

fn create_segment(
    directory: &FsSegmentDirectory,
    segment_id: u64,
    base_lsn: Lsn,
    compression_algorithm: u8,
    encoded_records: &[Vec<u8>],
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

    for encoded in encoded_records {
        file.append_all(encoded).unwrap();
    }

    file.sync().unwrap();
}

fn corrupt_file_byte(path: &Path, offset: usize) {
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0xFF;
    fs::write(path, bytes).unwrap();
}

#[test]
fn open_repairs_corrupt_tail_in_latest_segment_and_reports_truncation() {
    let test_dir = TestDir::new("truncate");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let first_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"hello",
        Lsn::ZERO,
        0,
    );
    let second_lsn = Lsn::new(first_record.len() as u64);
    let second_record = encode_record(
        RecordType::new(record_types::USER_MIN + 1),
        record_flags::NONE,
        b"world",
        second_lsn,
        0,
    );

    create_segment(
        &directory,
        1,
        Lsn::ZERO,
        compression_algorithms::NONE,
        &[first_record.clone(), second_record.clone()],
    );

    let segment_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 1)
        .unwrap()
        .path;

    let second_payload_offset =
        SEGMENT_HEADER_LEN as usize + first_record.len() + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment_path, second_payload_offset);

    let (wal, report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(wal.first_lsn(), Some(Lsn::ZERO));
    assert_eq!(wal.next_lsn(), second_lsn);
    assert_eq!(wal.durable_lsn(), second_lsn);
    assert_eq!(
        wal.current_wal_size(),
        SEGMENT_HEADER_LEN + first_record.len() as u64
    );
    assert_eq!(wal.active_segment_id(), Some(1));

    assert_eq!(report.segments_scanned, 1);
    assert_eq!(report.records_scanned, 1);
    assert_eq!(report.corrupt_records_found, 1);
    assert_eq!(report.first_lsn, Some(Lsn::ZERO));
    assert_eq!(report.last_valid_lsn, Some(Lsn::ZERO));
    assert_eq!(report.next_lsn, second_lsn);
    assert_eq!(report.truncated_bytes, second_record.len() as u64);
    assert_eq!(report.checkpoint_lsn, None);
    assert!(!report.clean_shutdown);
    assert!(!report.recovery_skipped);

    let repaired = directory.open_segment(1).unwrap();
    assert_eq!(
        repaired.len().unwrap(),
        SEGMENT_HEADER_LEN + first_record.len() as u64
    );
}

#[test]
fn reopening_after_tail_repair_is_idempotent() {
    let test_dir = TestDir::new("idempotent");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let first_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"hello",
        Lsn::ZERO,
        0,
    );
    let second_lsn = Lsn::new(first_record.len() as u64);
    let second_record = encode_record(
        RecordType::new(record_types::USER_MIN + 1),
        record_flags::NONE,
        b"world",
        second_lsn,
        0,
    );

    create_segment(
        &directory,
        1,
        Lsn::ZERO,
        compression_algorithms::NONE,
        &[first_record.clone(), second_record.clone()],
    );

    let segment_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 1)
        .unwrap()
        .path;

    let second_payload_offset =
        SEGMENT_HEADER_LEN as usize + first_record.len() + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment_path, second_payload_offset);

    {
        let (_wal, first_report) = Wal::open(
            FsSegmentDirectory::new(test_dir.path().to_path_buf()),
            test_dir.config(),
            (),
        )
        .unwrap();

        assert_eq!(first_report.corrupt_records_found, 1);
        assert_eq!(first_report.truncated_bytes, second_record.len() as u64);
        assert_eq!(first_report.next_lsn, second_lsn);
    }

    let repaired_len = directory.open_segment(1).unwrap().len().unwrap();
    assert_eq!(repaired_len, SEGMENT_HEADER_LEN + first_record.len() as u64);

    let (wal, second_report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(wal.next_lsn(), second_lsn);
    assert_eq!(wal.durable_lsn(), second_lsn);
    assert_eq!(second_report.corrupt_records_found, 0);
    assert_eq!(second_report.truncated_bytes, 0);
    assert_eq!(second_report.next_lsn, second_lsn);
    assert_eq!(
        directory.open_segment(1).unwrap().len().unwrap(),
        repaired_len
    );
}

#[test]
fn read_only_open_refuses_truncatable_latest_tail_corruption() {
    let test_dir = TestDir::new("read-only");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let first_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"hello",
        Lsn::ZERO,
        0,
    );
    let second_lsn = Lsn::new(first_record.len() as u64);
    let second_record = encode_record(
        RecordType::new(record_types::USER_MIN + 1),
        record_flags::NONE,
        b"world",
        second_lsn,
        0,
    );

    create_segment(
        &directory,
        1,
        Lsn::ZERO,
        compression_algorithms::NONE,
        &[first_record.clone(), second_record.clone()],
    );

    let segment_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 1)
        .unwrap()
        .path;

    let second_payload_offset =
        SEGMENT_HEADER_LEN as usize + first_record.len() + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment_path, second_payload_offset);

    let mut config = test_dir.config();
    config.read_only = true;

    let result = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    );

    assert!(matches!(result, Err(WalError::ReadOnlyTailCorruption)));

    let segment = directory.open_segment(1).unwrap();
    assert_eq!(
        segment.len().unwrap(),
        SEGMENT_HEADER_LEN + first_record.len() as u64 + second_record.len() as u64
    );
}
