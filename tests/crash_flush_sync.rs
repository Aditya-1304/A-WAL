use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, SyncPolicy, WalConfig},
    error::WalError,
    format::record_header::RecordHeader,
    io::fault::{CrashPlan, FaultDirectory},
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
            std::env::temp_dir().join(format!("wal-crash-{prefix}-{}-{nanos}", process::id()));

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

#[test]
fn flush_only_record_is_lost_after_crash_reset() {
    let test_dir = TestDir::new("flush-only");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.flush().unwrap();

    drop(wal);
    directory.crash_reset(CrashPlan::drop_all_unsynced());

    let (wal, report) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();

    assert_eq!(report.records_scanned, 0);
    assert_eq!(wal.next_lsn(), Lsn::ZERO);
    assert!(matches!(
        wal.read_at(lsn),
        Err(WalError::LsnOutOfRange { lsn: found }) if found == lsn
    ));
}

#[test]
fn synced_record_survives_crash_reset() {
    let test_dir = TestDir::new("sync-survives");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    let lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.sync().unwrap();

    drop(wal);
    directory.crash_reset(CrashPlan::drop_all_unsynced());

    let (wal, report) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    let record = wal.read_at(lsn).unwrap();

    assert_eq!(report.records_scanned, 1);
    assert_eq!(record.payload, b"hello");
    assert_eq!(wal.first_lsn(), Some(lsn));
}

#[test]
fn torn_flushed_suffix_recovers_maximal_valid_prefix() {
    let test_dir = TestDir::new("torn-tail");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());

    let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();

    let first_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"first")
        .unwrap();
    wal.sync().unwrap();

    let second_lsn = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"second")
        .unwrap();
    wal.flush().unwrap();

    drop(wal);

    let kept_prefix = RecordHeader::ENCODED_LEN + 2;
    directory.crash_reset(CrashPlan::keep_flushed_prefix(kept_prefix));

    let (wal, report) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();

    assert_eq!(report.records_scanned, 1);
    assert_eq!(report.corrupt_records_found, 1);
    assert_eq!(report.truncated_bytes, kept_prefix as u64);
    assert_eq!(wal.next_lsn(), second_lsn);
    assert_eq!(wal.read_at(first_lsn).unwrap().payload, b"first");
    assert!(matches!(
        wal.read_at(second_lsn),
        Err(WalError::LsnOutOfRange { lsn }) if lsn == second_lsn
    ));
}

#[test]
fn injected_partial_append_poison_wal_and_crash_drops_partial_bytes() {
    let test_dir = TestDir::new("partial-append");
    let directory = FaultDirectory::new(test_dir.path().to_path_buf());
    directory.inject_partial_append(1, 5).unwrap();

    let mut config = test_dir.config();
    config.sync_policy = SyncPolicy::Always;

    let (mut wal, _) = Wal::open(directory.clone(), config, ()).unwrap();

    let err = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();
    assert!(matches!(
        err,
        WalError::FatalIo {
            operation: "append wal bytes",
            ..
        }
    ));

    assert_eq!(
        directory.segment_bytes(1).unwrap().len(),
        SEGMENT_HEADER_LEN as usize + 5
    );

    let retry = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"later")
        .unwrap_err();
    assert!(matches!(
        retry,
        WalError::FatalIo {
            operation: "append wal bytes",
            ..
        }
    ));

    drop(wal);
    directory.crash_reset(CrashPlan::drop_all_unsynced());

    assert_eq!(
        directory.segment_bytes(1).unwrap().len(),
        SEGMENT_HEADER_LEN as usize
    );

    let (wal, report) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    assert_eq!(report.records_scanned, 0);
    assert_eq!(wal.next_lsn(), Lsn::ZERO);
}
