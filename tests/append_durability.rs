use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SyncPolicy, WalConfig},
    error::{AppendFailure, WalError},
    io::{directory::FsSegmentDirectory, fault::FaultDirectory},
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
    wal::{WalHandle, engine::Wal},
};

/// creates temporary directory scoped to one durable append test
///
/// every test receives an isolated WAL identity and storage directory so fault
/// injection and sticky fatal state cannot leak between test cases
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow the Unix epoch")
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "wal-durable-append-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create durable-append test directory");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: WalIdentity::new(81, 32, 1),
            ..WalConfig::default()
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// ensures deterministic admission rejection is classified as not staged
///
/// Realistic bug caught:
///
/// A payload rejected before acquiring an extent could be mislabeled as
/// outcome-unknown, forcing unnecessary recovery and preventing a safe caller
/// retry even though no user-record bytes entered the WAL
#[test]
fn admission_rejection_is_definitely_not_staged() {
    let test_dir = TestDir::new("not-staged");
    let mut config = test_dir.config();
    config.max_record_size = 4;

    let (mut wal, _) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let error = wal
        .append_and_sync(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();

    assert!(matches!(
        error,
        AppendFailure::NotStaged(WalError::PayloadTooLarge { len: 5, max: 4 })
    ));

    assert_eq!(wal.next_lsn(), Lsn::ZERO);
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);
    assert_eq!(wal.active_segment_id(), None);

    // deterministic admission failure does not poison an otherwise healthy
    // writer
    let extent = wal
        .append_and_sync(RecordType::new(record_types::USER_MIN), b"okay")
        .unwrap();

    assert_eq!(extent.start_lsn, Lsn::ZERO);
    assert_eq!(extent.end_lsn, Lsn::new(36));
    assert_eq!(wal.durable_lsn(), extent.end_lsn);
}

/// ensures a synchronization failure after staging preserves the exact extent
///
/// Realistic bug caught:
///
/// Reporting this failure as an ordinary abort could cause the database to
/// retry a commit even though crash recovery may retain the first record,
/// producing duplicate logical execution without durable request deduplication.
#[test]
fn synchronization_failure_after_staging_is_outcome_unknown() {
    let test_dir = TestDir::new("outcome-unknown");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    directory.inject_sync_error(1).unwrap();

    let (mut wal, _) = Wal::open(directory, test_dir.config(), ()).unwrap();

    let error = wal
        .append_and_sync(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();

    let extent = match error {
        AppendFailure::OutcomeUnknown { extent, source } => {
            assert!(matches!(
                source,
                WalError::FatalIo {
                    operation: "sync wal segment",
                    ..
                }
            ));

            extent
        }

        other => panic!("expected outcome-unknown synchronization failure, got {other:?}"),
    };

    assert_eq!(extent.start_lsn, Lsn::ZERO);
    assert_eq!(extent.end_lsn, Lsn::new(37));

    // The record acquired a complete logical extent, but the failed fsync means
    // the process cannot prove whether recovery will retain it.
    assert_eq!(wal.next_lsn(), extent.end_lsn);
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);
}

/// Ensures uncertain mutating I/O fail-stops subsequent writes.
///
/// Realistic bug caught:
///
/// Allowing another append after an uncertain synchronization failure could
/// extend an in-memory history whose durable prefix is no longer known,
/// preventing recovery from establishing an unambiguous commit boundary.
#[test]
fn outcome_unknown_puts_writer_in_sticky_fatal_state() {
    let test_dir = TestDir::new("sticky-fatal");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    directory.inject_sync_error(1).unwrap();

    let (mut wal, _) = Wal::open(directory, test_dir.config(), ()).unwrap();

    let first_error = wal
        .append_and_sync(RecordType::new(record_types::USER_MIN), b"first")
        .unwrap_err();

    let first_extent = match first_error {
        AppendFailure::OutcomeUnknown { extent, .. } => extent,
        other => panic!("expected outcome unknown, got {other:?}"),
    };

    let retry_error = wal
        .append_and_sync(RecordType::new(record_types::USER_MIN + 1), b"second")
        .unwrap_err();

    assert!(matches!(
        retry_error,
        AppendFailure::NotStaged(WalError::FatalIo {
            operation: "sync wal segment",
            ..
        })
    ));

    assert_eq!(wal.next_lsn(), first_extent.end_lsn);
    assert_eq!(wal.durable_lsn(), Lsn::ZERO);
}

/// Ensures `append` preserves outcome-unknown under `SyncPolicy::Always`.
///
/// Realistic bug caught:
///
/// The implicit synchronization performed by `append` could otherwise discard
/// the successfully staged extent when fsync fails, incorrectly presenting the
/// operation as definitely aborted.
#[test]
fn sync_policy_always_append_preserves_outcome_unknown() {
    let test_dir = TestDir::new("always");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    directory.inject_sync_error(1).unwrap();

    let mut config = test_dir.config();
    config.sync_policy = SyncPolicy::Always;

    let (mut wal, _) = Wal::open(directory, config, ()).unwrap();

    let error = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();

    match error {
        AppendFailure::OutcomeUnknown { extent, source } => {
            assert_eq!(extent.start_lsn, Lsn::ZERO);
            assert_eq!(extent.end_lsn, Lsn::new(37));

            assert!(matches!(
                source,
                WalError::FatalIo {
                    operation: "sync wal segment",
                    ..
                }
            ));
        }

        other => panic!("expected SyncPolicy::Always outcome unknown, got {other:?}"),
    }
}

/// Ensures the shared WAL handle exposes the same exact durable boundary.
///
/// Realistic bug caught:
///
/// A correct engine implementation is insufficient if the concurrent handle
/// still exposes append and sync as separate operations or returns the record's
/// starting LSN as the durability target.
#[test]
fn wal_handle_append_and_sync_returns_durable_extent() {
    let test_dir = TestDir::new("handle-success");

    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let extent = handle
        .append_and_sync(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(extent.start_lsn, Lsn::ZERO);
    assert_eq!(extent.end_lsn, Lsn::new(37));
    assert_eq!(handle.durable_lsn(), extent.end_lsn);
    assert!(extent.is_durable_at(handle.durable_lsn()));

    let record = handle.read_at(extent.start_lsn).unwrap();
    assert_eq!(record.payload, b"hello");
}
