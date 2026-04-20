use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, SyncPolicy, WalConfig},
    error::WalError,
    io::{
        directory::{FsSegmentDirectory, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
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

        let path =
            std::env::temp_dir().join(format!("wal-append-{prefix}-{}-{nanos}", process::id()));

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

fn open_wal(test_dir: &TestDir) -> Wal<FsSegmentDirectory, ()> {
    Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .expect("failed to open wal")
    .0
}

#[test]
fn open_empty_directory_starts_clean() {
    let test_dir = TestDir::new("open-empty");
    let wal = open_wal(&test_dir);

    assert_eq!(wal.next_lsn(), Lsn::ZERO);
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);
    assert_eq!(wal.first_lsn(), None);
    assert_eq!(wal.current_wal_size(), 0);
    assert_eq!(wal.active_segment_id(), None);
    assert_eq!(wal.buffered_bytes(), 0);
}

#[test]
fn append_assigns_contiguous_lsns_in_logical_byte_space() {
    let test_dir = TestDir::new("contiguous-lsns");
    let mut wal = open_wal(&test_dir);

    let first = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 8])
        .unwrap();
    let second = wal
        .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();

    assert_eq!(first, Lsn::ZERO);
    assert_eq!(second, Lsn::new(40));
    assert_eq!(wal.first_lsn(), Some(Lsn::ZERO));
    assert_eq!(wal.next_lsn(), Lsn::new(88));
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);
    assert_eq!(wal.active_segment_id(), Some(1));
}

#[test]
fn flush_drains_tail_bytes_without_advancing_durable_frontier() {
    let test_dir = TestDir::new("flush");
    let mut wal = open_wal(&test_dir);

    wal.append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN);
    assert_eq!(wal.buffered_bytes(), 37);

    wal.flush().unwrap();

    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 37);
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let segment = directory.open_segment(1).unwrap();
    assert_eq!(segment.len().unwrap(), SEGMENT_HEADER_LEN + 37);
}

#[test]
fn sync_forces_small_tail_to_disk_and_advances_durable_frontier() {
    let test_dir = TestDir::new("sync");
    let mut wal = open_wal(&test_dir);

    wal.append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.sync().unwrap();

    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.next_lsn(), Lsn::new(37));
    assert_eq!(wal.durable_lsn(), Lsn::new(37));
    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 37);
}

#[test]
fn steady_state_drain_prefers_storage_write_unit_multiples() {
    let test_dir = TestDir::new("steady-drain");
    let mut config = test_dir.config();
    config.storage_write_unit = 512;
    config.write_buffer_size = 2048;
    config.max_record_size = 600;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[7u8; 600])
        .unwrap();

    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 512);
    assert_eq!(wal.buffered_bytes(), 120);
    assert_eq!(wal.next_lsn(), Lsn::new(632));

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let segment = directory.open_segment(1).unwrap();
    assert_eq!(segment.len().unwrap(), SEGMENT_HEADER_LEN + 512);
}

#[test]
fn sync_policy_always_makes_append_durable_immediately() {
    let test_dir = TestDir::new("sync-always");
    let mut config = test_dir.config();
    config.sync_policy = SyncPolicy::Always;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(lsn, Lsn::ZERO);
    assert_eq!(wal.next_lsn(), Lsn::new(37));
    assert_eq!(wal.durable_lsn(), Lsn::new(37));
    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 37);
}

#[test]
fn read_only_mode_rejects_append_flush_and_sync() {
    let test_dir = TestDir::new("read-only");
    let mut config = test_dir.config();
    config.read_only = true;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let append_err = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();
    let flush_err = wal.flush().unwrap_err();
    let sync_err = wal.sync().unwrap_err();

    assert!(matches!(append_err, WalError::ReadOnlyViolation));
    assert!(matches!(flush_err, WalError::ReadOnlyViolation));
    assert!(matches!(sync_err, WalError::ReadOnlyViolation));
}

#[test]
fn append_rejects_payloads_larger_than_max_record_size() {
    let test_dir = TestDir::new("payload-too-large");
    let mut config = test_dir.config();
    config.max_record_size = 16;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let err = wal
        .append(RecordType::new(record_types::USER_MIN), &[0u8; 17])
        .unwrap_err();

    assert!(matches!(
        err,
        WalError::PayloadTooLarge { len: 17, max: 16 }
    ));
}

#[test]
fn reopen_restores_latest_next_lsn_from_segment_lengths() {
    let test_dir = TestDir::new("reopen");

    {
        let mut wal = open_wal(&test_dir);
        wal.append(RecordType::new(record_types::USER_MIN), b"hello")
            .unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.first_lsn(), Some(Lsn::ZERO));
        assert_eq!(wal.next_lsn(), Lsn::new(37));
        assert_eq!(wal.durable_lsn(), Lsn::new(37));
    }

    let (reopened, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(reopened.active_segment_id(), Some(1));
    assert_eq!(reopened.first_lsn(), Some(Lsn::ZERO));
    assert_eq!(reopened.next_lsn(), Lsn::new(37));
    assert_eq!(reopened.durable_lsn(), Lsn::new(37));
    assert_eq!(reopened.current_wal_size(), SEGMENT_HEADER_LEN + 37);
}
