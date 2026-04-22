use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, WalConfig},
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
            "wal-retention-pins-{prefix}-{}-{nanos}",
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

#[test]
fn retention_pin_blocks_pruning_until_guard_is_dropped() {
    let test_dir = TestDir::new("blocks-prune");
    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.rollover_config(),
        (),
    )
    .unwrap();

    let first = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    let second = wal
        .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();
    let third = wal
        .append(RecordType::new(record_types::USER_MIN), &[3u8; 16])
        .unwrap();

    wal.sync().unwrap();

    let pin = wal.acquire_retention_pin("tail", second).unwrap();

    let removed = wal.truncate_segments_before(third).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(wal.first_lsn(), Some(second));

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let metas = directory.list_segments().unwrap();
    assert_eq!(metas.len(), 2);
    assert_eq!(metas[0].segment_id, 2);
    assert_eq!(metas[1].segment_id, 3);

    assert!(matches!(
        wal.read_at(first),
        Err(WalError::LsnPruned { lsn }) if lsn == first
    ));

    let second_record = wal.read_at(second).unwrap();
    assert_eq!(second_record.payload, vec![2u8; 16]);

    drop(pin);

    let removed_again = wal.truncate_segments_before(third).unwrap();
    assert_eq!(removed_again, 1);
    assert_eq!(wal.first_lsn(), Some(third));

    let metas = directory.list_segments().unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].segment_id, 3);
}

#[test]
fn metrics_report_active_retention_pin_count() {
    let test_dir = TestDir::new("metrics");
    let (wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(wal.metrics().retention_pins_active, 0);

    let pin1 = wal.acquire_retention_pin("tail", Lsn::ZERO).unwrap();
    assert_eq!(wal.metrics().retention_pins_active, 1);

    let pin2 = wal.acquire_retention_pin("backup", Lsn::new(10)).unwrap();
    assert_eq!(wal.metrics().retention_pins_active, 2);

    drop(pin1);
    assert_eq!(wal.metrics().retention_pins_active, 1);

    drop(pin2);
    assert_eq!(wal.metrics().retention_pins_active, 0);
}

#[test]
fn read_only_mode_rejects_retention_pin_acquisition() {
    let test_dir = TestDir::new("read-only");
    let mut config = test_dir.config();
    config.read_only = true;

    let (wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let err = wal.acquire_retention_pin("tail", Lsn::ZERO).unwrap_err();

    assert!(matches!(err, WalError::ReadOnlyViolation));
}
