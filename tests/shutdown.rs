use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::WalConfig,
    error::WalError,
    io::{control_file::FsControlFileStore, directory::FsSegmentDirectory},
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
            "wal-shutdown-it-{prefix}-{}-{nanos}",
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

#[test]
fn shutdown_sets_clean_witness_rejects_future_appends_and_fast_restarts() {
    let test_dir = TestDir::new("fast-path");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

    let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    let user_lsn = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    let next_before_shutdown = wal.next_lsn();

    wal.shutdown().unwrap();

    let published = control_store.read().unwrap().unwrap();
    assert!(published.clean_shutdown);

    let err = wal
        .append(RecordType::new(record_types::USER_MIN + 1), b"later")
        .unwrap_err();
    assert!(matches!(err, WalError::ShutdownInProgress));

    let (reopened, report) = Wal::open(directory, test_dir.config(), ()).unwrap();

    assert!(report.clean_shutdown);
    assert!(report.recovery_skipped);
    assert!(reopened.next_lsn() > next_before_shutdown);
    assert_eq!(reopened.read_at(user_lsn).unwrap().payload, b"hello");

    let cleared = control_store.read().unwrap().unwrap();
    assert!(!cleared.clean_shutdown);
}

#[test]
fn read_only_fast_restart_preserves_clean_shutdown_flag() {
    let test_dir = TestDir::new("read-only");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

    let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    wal.append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();
    wal.shutdown().unwrap();

    let mut config = test_dir.config();
    config.read_only = true;

    let (_wal, report) = Wal::open(directory, config, ()).unwrap();

    assert!(report.clean_shutdown);
    assert!(report.recovery_skipped);
    assert!(control_store.read().unwrap().unwrap().clean_shutdown);
}
