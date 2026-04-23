use std::{
    fs,
    path::{Path, PathBuf},
    process, thread,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
    wal::WalHandle,
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
            std::env::temp_dir().join(format!("wal-handle-{prefix}-{}-{nanos}", process::id()));

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
fn handle_round_trips_append_sync_and_read_paths() {
    let test_dir = TestDir::new("round-trip");
    let (handle, report) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(report.next_lsn, Lsn::ZERO);

    let lsn = handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(handle.durable_lsn(), Lsn::ZERO);

    handle.sync_through(Lsn::new(37)).unwrap();

    let record = handle.read_at(lsn).unwrap();
    assert_eq!(record.payload, b"hello");

    let mut iter = handle.iter_from(lsn).unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.lsn, lsn);
    assert_eq!(first.payload, b"hello");
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn concurrent_appends_through_handle_produce_unique_ordered_lsns() {
    let test_dir = TestDir::new("concurrent");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let mut joins = Vec::new();

    for i in 0..8u16 {
        let handle = handle.clone();

        joins.push(thread::spawn(move || {
            let payload = format!("record-{i}").into_bytes();
            let lsn = handle
                .append(RecordType::new(record_types::USER_MIN + i), &payload)
                .unwrap();

            (lsn, payload)
        }));
    }

    let mut results = joins
        .into_iter()
        .map(|join| join.join().unwrap())
        .collect::<Vec<_>>();

    handle.sync().unwrap();

    results.sort_by_key(|(lsn, _)| lsn.as_u64());

    for window in results.windows(2) {
        assert!(window[0].0 < window[1].0);
    }

    for (lsn, payload) in results {
        let record = handle.read_at(lsn).unwrap();
        assert_eq!(record.payload, payload);
    }
}

#[test]
fn handle_retention_pin_updates_metrics() {
    let test_dir = TestDir::new("pin-metrics");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(handle.metrics().retention_pins_active, 0);

    let pin = handle.acquire_retention_pin("tail", Lsn::ZERO).unwrap();
    assert_eq!(handle.metrics().retention_pins_active, 1);

    drop(pin);
    assert_eq!(handle.metrics().retention_pins_active, 0);
}
