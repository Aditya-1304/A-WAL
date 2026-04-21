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
            "wal-recovery-mid-{prefix}-{}-{nanos}",
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

fn build_three_segment_history(directory: &FsSegmentDirectory) -> (u64, u64, u64) {
    let first_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"first",
        Lsn::ZERO,
        0,
    );
    let seal1_payload = SegmentSealPayload {
        segment_id: 1,
        record_count: 1,
        logical_bytes: first_record.len() as u64,
    }
    .encode();
    let seal1_lsn = Lsn::new(first_record.len() as u64);
    let seal1_record = encode_record(
        record_types::SEGMENT_SEAL,
        record_flags::NONE,
        &seal1_payload,
        seal1_lsn,
        0,
    );

    let second_base_lsn = Lsn::new((first_record.len() + seal1_record.len()) as u64);
    let second_record = encode_record(
        RecordType::new(record_types::USER_MIN + 1),
        record_flags::NONE,
        b"middle",
        second_base_lsn,
        0,
    );
    let seal2_payload = SegmentSealPayload {
        segment_id: 2,
        record_count: 1,
        logical_bytes: second_record.len() as u64,
    }
    .encode();
    let seal2_lsn =
        Lsn::new((first_record.len() + seal1_record.len() + second_record.len()) as u64);
    let seal2_record = encode_record(
        record_types::SEGMENT_SEAL,
        record_flags::NONE,
        &seal2_payload,
        seal2_lsn,
        0,
    );

    let third_base_lsn = Lsn::new(
        (first_record.len() + seal1_record.len() + second_record.len() + seal2_record.len()) as u64,
    );
    let third_record = encode_record(
        RecordType::new(record_types::USER_MIN + 2),
        record_flags::NONE,
        b"latest",
        third_base_lsn,
        0,
    );

    create_segment(
        directory,
        1,
        Lsn::ZERO,
        compression_algorithms::NONE,
        &[first_record.clone(), seal1_record.clone()],
    );
    create_segment(
        directory,
        2,
        second_base_lsn,
        compression_algorithms::NONE,
        &[second_record.clone(), seal2_record.clone()],
    );
    create_segment(
        directory,
        3,
        third_base_lsn,
        compression_algorithms::NONE,
        &[third_record.clone()],
    );

    let segment1_len = SEGMENT_HEADER_LEN + first_record.len() as u64 + seal1_record.len() as u64;
    let segment2_len = SEGMENT_HEADER_LEN + second_record.len() as u64 + seal2_record.len() as u64;
    let segment3_len = SEGMENT_HEADER_LEN + third_record.len() as u64;

    (segment1_len, segment2_len, segment3_len)
}

#[test]
fn open_fails_hard_on_corruption_in_middle_sealed_segment() {
    let test_dir = TestDir::new("hard-fail");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    build_three_segment_history(&directory);

    let segment2_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 2)
        .unwrap()
        .path;

    let middle_payload_offset = SEGMENT_HEADER_LEN as usize + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment2_path, middle_payload_offset);

    let result = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    );

    assert!(matches!(result, Err(WalError::CorruptionInSealedSegment)));
}

#[test]
fn middle_segment_corruption_does_not_modify_any_segment_files() {
    let test_dir = TestDir::new("no-truncate");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let (segment1_len, segment2_len, segment3_len) = build_three_segment_history(&directory);

    let segment2_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 2)
        .unwrap()
        .path;

    let middle_payload_offset = SEGMENT_HEADER_LEN as usize + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment2_path, middle_payload_offset);

    let result = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    );

    assert!(matches!(result, Err(WalError::CorruptionInSealedSegment)));

    assert_eq!(
        directory.open_segment(1).unwrap().len().unwrap(),
        segment1_len
    );
    assert_eq!(
        directory.open_segment(2).unwrap().len().unwrap(),
        segment2_len
    );
    assert_eq!(
        directory.open_segment(3).unwrap().len().unwrap(),
        segment3_len
    );
}

#[test]
fn read_only_open_still_fails_hard_on_middle_segment_corruption() {
    let test_dir = TestDir::new("read-only");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    build_three_segment_history(&directory);

    let segment2_path = directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == 2)
        .unwrap()
        .path;

    let middle_payload_offset = SEGMENT_HEADER_LEN as usize + RecordHeader::ENCODED_LEN;
    corrupt_file_byte(&segment2_path, middle_payload_offset);

    let mut config = test_dir.config();
    config.read_only = true;

    let result = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    );

    assert!(matches!(result, Err(WalError::CorruptionInSealedSegment)));
}
