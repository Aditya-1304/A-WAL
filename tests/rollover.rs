use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, WalConfig},
    error::WalError,
    format::{record_header::RecordHeader, segment_header::SegmentHeader},
    io::{
        directory::{FsSegmentDirectory, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
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

        let path =
            std::env::temp_dir().join(format!("wal-rollover-{prefix}-{}-{nanos}", process::id()));

        fs::create_dir_all(&path).expect("failed to create test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: WalIdentity::new(11, 22, 1),
            ..WalConfig::default()
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn read_exact_at<F: SegmentFile>(file: &F, offset: u64, buf: &mut [u8]) {
    let mut filled = 0usize;

    while filled < buf.len() {
        let n = file
            .read_at(offset + filled as u64, &mut buf[filled..])
            .expect("read_at failed");
        assert!(n > 0, "unexpected EOF while reading exact bytes");
        filled += n;
    }
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
fn rollover_switches_to_next_segment_and_keeps_lsn_space_contiguous() {
    let test_dir = TestDir::new("switch");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let first_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    let second_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    assert_eq!(first_lsn, Lsn::ZERO);
    assert_eq!(second_lsn, Lsn::new(104));
    assert_eq!(wal.active_segment_id(), Some(2));
    assert_eq!(wal.next_lsn(), Lsn::new(152));
    assert_eq!(wal.buffered_bytes(), 48);
}

#[test]
fn rollover_creates_two_segments_sorted_by_base_lsn() {
    let test_dir = TestDir::new("sorted");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let segments = directory.list_segments().unwrap();

    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].segment_id, 1);
    assert_eq!(segments[0].base_lsn, Lsn::ZERO);
    assert_eq!(segments[1].segment_id, 2);
    assert_eq!(segments[1].base_lsn, Lsn::new(104));
}

#[test]
fn sealed_segment_ends_with_segment_seal_record() {
    let test_dir = TestDir::new("seal-record");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_segment = directory.open_segment(1).unwrap();
    let first_len = first_segment.len().unwrap();
    let seal_offset = first_len - 56;

    let mut header_bytes = [0u8; RecordHeader::ENCODED_LEN];
    read_exact_at(&first_segment, seal_offset, &mut header_bytes);

    let seal_header = RecordHeader::decode(&header_bytes).unwrap();

    assert_eq!(seal_header.record_type, record_types::SEGMENT_SEAL);
    assert_eq!(seal_header.lsn, Lsn::new(48));
    assert_eq!(
        seal_header.payload_len as usize,
        SegmentSealPayload::ENCODED_LEN
    );
}

#[test]
fn segment_seal_payload_describes_the_sealed_segment() {
    let test_dir = TestDir::new("seal-payload");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_segment = directory.open_segment(1).unwrap();
    let first_len = first_segment.len().unwrap();
    let payload_offset = first_len - SegmentSealPayload::ENCODED_LEN as u64;

    let mut payload_bytes = vec![0u8; SegmentSealPayload::ENCODED_LEN];
    read_exact_at(&first_segment, payload_offset, &mut payload_bytes);

    let payload = decode_seal_payload(&payload_bytes);

    assert_eq!(payload.segment_id, 1);
    assert_eq!(payload.record_count, 1);
    assert_eq!(payload.logical_bytes, 48);
}

#[test]
fn new_segment_header_base_lsn_matches_post_seal_tail() {
    let test_dir = TestDir::new("new-base-lsn");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let second_segment = directory.open_segment(2).unwrap();

    let mut header_bytes = [0u8; SegmentHeader::ENCODED_LEN];
    read_exact_at(&second_segment, 0, &mut header_bytes);

    let header = SegmentHeader::decode(&header_bytes).unwrap();

    assert_eq!(header.segment_id, 2);
    assert_eq!(header.base_lsn, Lsn::new(104));
}

#[test]
fn rollover_preserves_old_segment_file_length_after_switch() {
    let test_dir = TestDir::new("old-len-stable");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();
    wal.sync().unwrap();

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_segment = directory.open_segment(1).unwrap();
    let second_segment = directory.open_segment(2).unwrap();

    assert_eq!(first_segment.len().unwrap(), SEGMENT_HEADER_LEN + 48 + 56);
    assert_eq!(second_segment.len().unwrap(), SEGMENT_HEADER_LEN + 48);
}

#[test]
fn open_rejects_segment_size_that_cannot_fit_record_and_trailing_seal() {
    let test_dir = TestDir::new("bad-constraints");
    let mut config = test_dir.config();
    config.max_record_size = 16;
    config.write_buffer_size = 64;
    config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 55;

    let result = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    );

    assert!(matches!(result, Err(WalError::InvalidConfig { .. })));
}
