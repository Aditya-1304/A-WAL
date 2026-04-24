use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SyncPolicy, WalConfig},
    error::WalError,
    io::directory::FsSegmentDirectory,
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
            "wal-reservation-{prefix}-{}-{nanos}",
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
fn reservation_commit_publishes_records_at_provisional_lsns() {
    let test_dir = TestDir::new("commit");
    let mut wal = open_wal(&test_dir);

    let mut reservation = wal.reserve(2, 6).unwrap();
    let first = reservation
        .append(RecordType::new(record_types::USER_MIN), b"ab")
        .unwrap();
    let second = reservation
        .append(RecordType::new(record_types::USER_MIN + 1), b"cdef")
        .unwrap();

    assert_eq!(first, Lsn::ZERO);
    assert_eq!(second, Lsn::new(34));

    let committed = reservation.commit().unwrap();
    assert_eq!(committed, vec![first, second]);

    wal.flush().unwrap();

    let first_record = wal.read_at(first).unwrap();
    let second_record = wal.read_at(second).unwrap();

    assert_eq!(first_record.payload, b"ab");
    assert_eq!(second_record.payload, b"cdef");
    assert_eq!(wal.first_lsn(), Some(first));

    let metrics = wal.metrics();
    assert_eq!(metrics.batch_appends, 1);
    assert_eq!(metrics.records_appended, 2);
}

#[test]
fn reservation_abort_restores_pre_reservation_state_without_holes() {
    let test_dir = TestDir::new("abort");
    let mut wal = open_wal(&test_dir);

    let mut reservation = wal.reserve(2, 10).unwrap();
    reservation
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    reservation.abort().unwrap();

    assert_eq!(wal.next_lsn(), Lsn::ZERO);
    assert_eq!(wal.first_lsn(), None);
    assert_eq!(wal.current_wal_size(), 0);
    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.active_segment_id(), None);

    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"after")
        .unwrap();
    assert_eq!(lsn, Lsn::ZERO);
}

#[test]
fn dropping_reservation_aborts_implicitly() {
    let test_dir = TestDir::new("drop");
    let mut wal = open_wal(&test_dir);

    {
        let mut reservation = wal.reserve(1, 8).unwrap();
        reservation
            .append(RecordType::new(record_types::USER_MIN), b"hello")
            .unwrap();
    }

    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"later")
        .unwrap();
    assert_eq!(lsn, Lsn::ZERO);
}

#[test]
fn reservation_shrinks_unused_reserved_tail_before_publish() {
    let test_dir = TestDir::new("shrink");
    let mut wal = open_wal(&test_dir);

    let mut reservation = wal.reserve(2, 100).unwrap();
    let first = reservation
        .append(RecordType::new(record_types::USER_MIN), b"a")
        .unwrap();
    let second = reservation
        .append(RecordType::new(record_types::USER_MIN + 1), b"b")
        .unwrap();

    reservation.commit().unwrap();

    assert_eq!(first, Lsn::ZERO);
    assert_eq!(second, Lsn::new(33));

    let third = wal
        .append(RecordType::new(record_types::USER_MIN + 2), b"c")
        .unwrap();

    assert_eq!(third, Lsn::new(66));
}

#[test]
fn reservation_enforces_payload_and_record_count_quotas() {
    let test_dir = TestDir::new("quotas");
    let mut wal = open_wal(&test_dir);

    {
        let mut reservation = wal.reserve(2, 5).unwrap();
        reservation
            .append(RecordType::new(record_types::USER_MIN), b"four")
            .unwrap();

        let payload_err = reservation
            .append(RecordType::new(record_types::USER_MIN + 1), b"xy")
            .unwrap_err();
        assert!(matches!(payload_err, WalError::ReservationOverflow));

        reservation.abort().unwrap();
    }

    {
        let mut reservation = wal.reserve(1, 8).unwrap();
        reservation
            .append(RecordType::new(record_types::USER_MIN), b"one")
            .unwrap();

        let count_err = reservation
            .append(RecordType::new(record_types::USER_MIN + 1), b"two")
            .unwrap_err();
        assert!(matches!(count_err, WalError::ReservationOverflow));
    }
}

#[test]
fn reservation_commit_rejects_incomplete_batch_without_mutation() {
    let test_dir = TestDir::new("incomplete");
    let mut wal = open_wal(&test_dir);

    let mut reservation = wal.reserve(2, 10).unwrap();
    reservation
        .append(RecordType::new(record_types::USER_MIN), b"only-one")
        .unwrap();

    let err = reservation.commit().unwrap_err();
    assert!(matches!(err, WalError::ReservationOverflow));

    assert_eq!(wal.next_lsn(), Lsn::ZERO);
    assert_eq!(wal.first_lsn(), None);
    assert_eq!(wal.current_wal_size(), 0);
    assert_eq!(wal.buffered_bytes(), 0);
    assert_eq!(wal.active_segment_id(), None);
}

#[test]
fn reservation_commit_sync_policy_always_syncs_once() {
    let test_dir = TestDir::new("sync-always");
    let mut config = test_dir.config();
    config.sync_policy = SyncPolicy::Always;

    let mut wal = open_wal_with_config(&test_dir, config);

    let mut reservation = wal.reserve(2, 10).unwrap();
    reservation
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    reservation
        .append(RecordType::new(record_types::USER_MIN + 1), b"world")
        .unwrap();

    reservation.commit().unwrap();

    assert_eq!(wal.durable_lsn(), wal.next_lsn());
    assert_eq!(wal.metrics().sync_calls, 1);
    assert_eq!(wal.metrics().batch_appends, 1);
}
