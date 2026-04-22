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

        let path =
            std::env::temp_dir().join(format!("wal-retention-{prefix}-{}-{nanos}", process::id()));

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
fn truncate_segments_before_removes_whole_sealed_prefix_and_updates_first_lsn() {
    let test_dir = TestDir::new("whole-prefix");
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

    let removed = wal.truncate_segments_before(third).unwrap();

    assert_eq!(removed, 2);
    assert_eq!(wal.first_lsn(), Some(third));
    assert_eq!(wal.active_segment_id(), Some(3));
    assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 48);

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let metas = directory.list_segments().unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].segment_id, 3);

    assert!(matches!(
        wal.read_at(first),
        Err(WalError::LsnPruned { lsn }) if lsn == first
    ));
    assert!(matches!(
        wal.read_at(second),
        Err(WalError::LsnPruned { lsn }) if lsn == second
    ));

    let third_record = wal.read_at(third).unwrap();
    assert_eq!(third_record.payload, vec![3u8; 16]);
}

#[test]
fn truncate_segments_before_does_not_remove_the_only_active_segment() {
    let test_dir = TestDir::new("keep-active");
    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.rollover_config(),
        (),
    )
    .unwrap();

    let first = wal
        .append(RecordType::new(record_types::USER_MIN), &[9u8; 16])
        .unwrap();

    let size_before = wal.current_wal_size();
    let removed = wal.truncate_segments_before(Lsn::new(10_000)).unwrap();

    assert_eq!(removed, 0);
    assert_eq!(wal.first_lsn(), Some(first));
    assert_eq!(wal.active_segment_id(), Some(1));
    assert_eq!(wal.current_wal_size(), size_before);
}

#[test]
fn read_only_mode_rejects_retention_mutations() {
    let test_dir = TestDir::new("read-only");
    let mut config = test_dir.config();
    config.read_only = true;

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let set_err = wal.set_min_retention_lsn(Lsn::new(100)).unwrap_err();
    let truncate_err = wal.truncate_segments_before(Lsn::new(100)).unwrap_err();

    assert!(matches!(set_err, WalError::ReadOnlyViolation));
    assert!(matches!(truncate_err, WalError::ReadOnlyViolation));
}
