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
            std::env::temp_dir().join(format!("wal-max-size-{prefix}-{}-{nanos}", process::id()));

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
fn first_append_is_rejected_before_creating_a_segment_when_limit_is_too_small() {
    let test_dir = TestDir::new("first-append");
    let mut config = test_dir.config();

    let projected = SEGMENT_HEADER_LEN + 37;
    config.max_wal_size = Some(projected - 1);

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let err = wal
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap_err();

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
fn append_is_rejected_when_rollover_overhead_would_push_wal_over_limit() {
    let test_dir = TestDir::new("rollover-overhead");
    let mut config = test_dir.rollover_config();

    let first_record_len = 48u64;
    let seal_record_len = 56u64;
    let second_record_len = 48u64;
    let projected = SEGMENT_HEADER_LEN
        + first_record_len
        + seal_record_len
        + SEGMENT_HEADER_LEN
        + second_record_len;

    config.max_wal_size = Some(projected - 1);

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    wal.append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    wal.sync().unwrap();

    let err = wal
        .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap_err();

    assert!(matches!(
        err,
        WalError::WalSizeLimitExceeded { current, limit }
            if current == projected && limit == projected - 1
    ));

    assert_eq!(wal.active_segment_id(), Some(1));
    assert_eq!(
        wal.current_wal_size(),
        SEGMENT_HEADER_LEN + first_record_len
    );

    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let metas = directory.list_segments().unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].segment_id, 1);
}

#[test]
fn pruning_frees_space_for_a_later_append_under_the_same_limit() {
    let test_dir = TestDir::new("prune-then-append");
    let mut config = test_dir.rollover_config();

    let sealed_segment_len = SEGMENT_HEADER_LEN + 48 + 56;
    let active_segment_len = SEGMENT_HEADER_LEN + 48;

    config.max_wal_size = Some(sealed_segment_len + active_segment_len);

    let (mut wal, _report) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        config,
        (),
    )
    .unwrap();

    let _first = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();
    let second = wal
        .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
        .unwrap();
    wal.sync().unwrap();

    let err = wal
        .append(RecordType::new(record_types::USER_MIN), &[3u8; 16])
        .unwrap_err();
    assert!(matches!(err, WalError::WalSizeLimitExceeded { .. }));

    let removed = wal.truncate_segments_before(second).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(wal.current_wal_size(), active_segment_len);

    let third = wal
        .append(RecordType::new(record_types::USER_MIN), &[3u8; 16])
        .unwrap();

    assert_eq!(third, Lsn::new(208));
}
