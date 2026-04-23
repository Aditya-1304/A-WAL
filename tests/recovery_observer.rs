use std::{
    fs,
    path::{Path, PathBuf},
    process,
    sync::Mutex,
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
        control_file::{ControlFile, FsControlFileStore},
        directory::{FsSegmentDirectory, NewSegment, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_flags, record_types},
    wal::engine::Wal,
    wal::recovery_observer::RecoveryObserver,
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
            "wal-recovery-observer-{prefix}-{}-{nanos}",
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
    encoded_records: &[Vec<u8>],
) {
    let mut header = SegmentHeader::new(
        sample_identity(),
        segment_id,
        base_lsn,
        compression_algorithms::NONE,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    SegmentStart {
        segment_id: u64,
        base_lsn: Lsn,
    },
    RecordsScanned {
        count: u64,
        current_lsn: Lsn,
    },
    Corruption {
        lsn: Lsn,
        kind: &'static str,
    },
    Truncation {
        at_lsn: Lsn,
        truncated_bytes: u64,
    },
    CheckpointFound {
        checkpoint_lsn: Lsn,
        checkpoint_no: u64,
    },
}

#[derive(Default)]
struct RecordingObserver {
    events: Mutex<Vec<Event>>,
}

impl RecordingObserver {
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

impl RecoveryObserver for RecordingObserver {
    fn on_segment_start(&self, segment_id: u64, base_lsn: Lsn) {
        self.events.lock().unwrap().push(Event::SegmentStart {
            segment_id,
            base_lsn,
        });
    }

    fn on_records_scanned(&self, count: u64, current_lsn: Lsn) {
        self.events
            .lock()
            .unwrap()
            .push(Event::RecordsScanned { count, current_lsn });
    }

    fn on_corruption_found(&self, lsn: Lsn, error: &WalError) {
        let kind = match error {
            WalError::ChecksumMismatch { .. } => "checksum_mismatch",
            WalError::ShortRead => "short_read",
            WalError::BadRecordHeader => "bad_record_header",
            WalError::BadSegmentHeader => "bad_segment_header",
            _ => "other",
        };

        self.events
            .lock()
            .unwrap()
            .push(Event::Corruption { lsn, kind });
    }

    fn on_truncation(&self, at_lsn: Lsn, truncated_bytes: u64) {
        self.events.lock().unwrap().push(Event::Truncation {
            at_lsn,
            truncated_bytes,
        });
    }

    fn on_checkpoint_found(&self, checkpoint_lsn: Lsn, checkpoint_no: u64) {
        self.events.lock().unwrap().push(Event::CheckpointFound {
            checkpoint_lsn,
            checkpoint_no,
        });
    }
}

struct PanickingObserver;

impl RecoveryObserver for PanickingObserver {
    fn on_segment_start(&self, _segment_id: u64, _base_lsn: Lsn) {
        panic!("boom");
    }

    fn on_records_scanned(&self, _count: u64, _current_lsn: Lsn) {
        panic!("boom");
    }

    fn on_corruption_found(&self, _lsn: Lsn, _error: &WalError) {
        panic!("boom");
    }

    fn on_truncation(&self, _at_lsn: Lsn, _truncated_bytes: u64) {
        panic!("boom");
    }

    fn on_checkpoint_found(&self, _checkpoint_lsn: Lsn, _checkpoint_no: u64) {
        panic!("boom");
    }
}

#[test]
fn open_with_observer_reports_segment_record_and_checkpoint_callbacks() {
    let test_dir = TestDir::new("checkpoint");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let user_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"hello",
        Lsn::ZERO,
        0,
    );
    let checkpoint_lsn = Lsn::new(user_record.len() as u64);
    let checkpoint_record = encode_record(
        record_types::END_CHECKPOINT,
        record_flags::NONE,
        &[],
        checkpoint_lsn,
        0,
    );

    create_segment(
        &directory,
        1,
        Lsn::ZERO,
        &[user_record.clone(), checkpoint_record],
    );

    let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());
    let control = ControlFile::new(sample_identity(), Some(checkpoint_lsn), 7, false);
    control_store.publish(&control).unwrap();

    let observer = RecordingObserver::default();
    let (_wal, report) = Wal::open_with_observer(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
        &observer,
    )
    .unwrap();

    let events = observer.events();

    assert!(events.contains(&Event::SegmentStart {
        segment_id: 1,
        base_lsn: Lsn::ZERO,
    }));
    assert!(events.contains(&Event::RecordsScanned {
        count: 1,
        current_lsn: Lsn::ZERO,
    }));
    assert!(events.contains(&Event::RecordsScanned {
        count: 2,
        current_lsn: checkpoint_lsn,
    }));
    assert!(events.contains(&Event::CheckpointFound {
        checkpoint_lsn,
        checkpoint_no: 7,
    }));

    assert_eq!(report.records_scanned, 2);
    assert_eq!(report.checkpoint_lsn, Some(checkpoint_lsn));
}

#[test]
fn open_with_observer_reports_tail_corruption_and_truncation() {
    let test_dir = TestDir::new("truncation");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let first_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"first",
        Lsn::ZERO,
        0,
    );
    let second_lsn = Lsn::new(first_record.len() as u64);
    let second_record = encode_record(
        RecordType::new(record_types::USER_MIN + 1),
        record_flags::NONE,
        b"second",
        second_lsn,
        0,
    );

    create_segment(
        &directory,
        1,
        Lsn::ZERO,
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

    let observer = RecordingObserver::default();
    let (_wal, report) = Wal::open_with_observer(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
        &observer,
    )
    .unwrap();

    let events = observer.events();

    assert!(events.contains(&Event::Corruption {
        lsn: second_lsn,
        kind: "checksum_mismatch",
    }));
    assert!(events.contains(&Event::Truncation {
        at_lsn: second_lsn,
        truncated_bytes: second_record.len() as u64,
    }));

    assert_eq!(report.corrupt_records_found, 1);
    assert_eq!(report.truncated_bytes, second_record.len() as u64);
}

#[test]
fn observer_panics_do_not_abort_recovery() {
    let test_dir = TestDir::new("panic");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let user_record = encode_record(
        RecordType::new(record_types::USER_MIN),
        record_flags::NONE,
        b"hello",
        Lsn::ZERO,
        0,
    );

    create_segment(&directory, 1, Lsn::ZERO, &[user_record]);

    let observer = PanickingObserver;
    let (_wal, report) = Wal::open_with_observer(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
        &observer,
    )
    .unwrap();

    assert_eq!(report.records_scanned, 1);
    assert_eq!(report.next_lsn, Lsn::new(37));
}
