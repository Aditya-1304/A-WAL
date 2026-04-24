use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, SyncPolicy, WalConfig},
    error::WalError,
    io::directory::{FsSegmentDirectory, SegmentDirectory},
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

        let path = std::env::temp_dir().join(format!(
            "wal-append-batch-{prefix}-{}-{nanos}",
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
            identity: WalIdentity::new(11, 22, 1),
            ..WalConfig::default()
        }
    }

    fn rollover_config(&self) -> WalConfig {
        let mut config = self.config();
        config.max_record_size = 16;
        config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;
        config
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
    .unwrap()
    .0
}

fn open_wal_with_config(test_dir: &TestDir, config: WalConfig) -> Wal<FsSegmentDirectory, ()> {
    Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap()
    .0
}

#[test]
fn append_batch_assigns_contiguous_lsns_and_reads_back_records() {
    let test_dir = TestDir::new("contiguous");
    let mut wal = open_wal(&test_dir);

    let batch = [
        (RecordType::new(record_types::USER_MIN), &b"alpha"[..]),
        (RecordType::new(record_types::USER_MIN + 1), &b"beta"[..]),
        (RecordType::new(record_types::USER_MIN + 2), &b"gamma"[..]),
    ];

    let lsns = wal.append_batch(&batch).unwrap();
    wal.sync().unwrap();

    let first = wal.read_at(lsns[0]).unwrap();
    let second = wal.read_at(lsns[1]).unwrap();
    let third = wal.read_at(lsns[2]).unwrap();

    assert_eq!(lsns.len(), 3);
    assert_eq!(lsns[0], Lsn::ZERO);
    assert_eq!(
        lsns[1],
        lsns[0].checked_add_bytes(first.total_len as u64).unwrap()
    );
    assert_eq!(
        lsns[2],
        lsns[1].checked_add_bytes(second.total_len as u64).unwrap()
    );
    assert_eq!(
        wal.next_lsn(),
        lsns[2].checked_add_bytes(third.total_len as u64).unwrap()
    );

    assert_eq!(first.payload, b"alpha");
    assert_eq!(second.payload, b"beta");
    assert_eq!(third.payload, b"gamma");
    assert_eq!(wal.first_lsn(), Some(lsns[0]));

    let metrics = wal.metrics();
    assert_eq!(metrics.batch_appends, 1);
    assert_eq!(metrics.records_appended, 3);
    assert_eq!(
        metrics.bytes_appended,
        u64::from(first.total_len) + u64::from(second.total_len) + u64::from(third.total_len)
    );
}

#[test]
fn append_batch_sync_policy_always_makes_whole_batch_durable_once() {
    let test_dir = TestDir::new("sync-always");
    let mut config = test_dir.config();
    config.sync_policy = SyncPolicy::Always;

    let mut wal = open_wal_with_config(&test_dir, config);

    let batch = [
        (RecordType::new(record_types::USER_MIN), &b"hello"[..]),
        (RecordType::new(record_types::USER_MIN + 1), &b"world"[..]),
    ];

    let lsns = wal.append_batch(&batch).unwrap();

    assert_eq!(wal.durable_lsn(), wal.next_lsn());
    assert_eq!(wal.metrics().sync_calls, 1);
    assert_eq!(wal.metrics().batch_appends, 1);
    assert_eq!(wal.read_at(lsns[0]).unwrap().payload, b"hello");
    assert_eq!(wal.read_at(lsns[1]).unwrap().payload, b"world");
}

#[test]
fn append_batch_rejects_wal_size_limit_before_creating_a_segment() {
    let test_dir = TestDir::new("first-segment-limit");
    let mut config = test_dir.config();

    let projected = SEGMENT_HEADER_LEN + 37 + 37;
    config.max_wal_size = Some(projected - 1);

    let mut wal = open_wal_with_config(&test_dir, config);

    let batch = [
        (RecordType::new(record_types::USER_MIN), &b"hello"[..]),
        (RecordType::new(record_types::USER_MIN + 1), &b"world"[..]),
    ];

    let err = wal.append_batch(&batch).unwrap_err();

    assert!(matches!(
        err,
        WalError::WalSizeLimitExceeded { current, limit }
            if current == projected && limit == projected - 1
    ));

    assert_eq!(wal.current_wal_size(), 0);
    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.active_segment_id(), None);

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    assert!(directory.list_segments().unwrap().is_empty());
}

#[test]
fn append_batch_accounts_for_rollover_overhead_during_preflight() {
    let test_dir = TestDir::new("rollover-limit");
    let mut config = test_dir.rollover_config();

    let projected = SEGMENT_HEADER_LEN + 48 + 56 + SEGMENT_HEADER_LEN + 48;
    config.max_wal_size = Some(projected - 1);

    let mut wal = open_wal_with_config(&test_dir, config);

    let batch = [
        (RecordType::new(record_types::USER_MIN), &[1u8; 16][..]),
        (RecordType::new(record_types::USER_MIN + 1), &[2u8; 16][..]),
    ];

    let err = wal.append_batch(&batch).unwrap_err();

    assert!(matches!(
        err,
        WalError::WalSizeLimitExceeded { current, limit }
            if current == projected && limit == projected - 1
    ));

    assert_eq!(wal.current_wal_size(), 0);
    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.active_segment_id(), None);

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    assert!(directory.list_segments().unwrap().is_empty());
}
