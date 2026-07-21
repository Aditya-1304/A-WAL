use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::WalConfig,
    io::directory::FsSegmentDirectory,
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
    wal::{engine::Wal, handle::WalHandle},
};

/// temp directory whose lifetime is scoped to one WAL test
///
/// each test receives an isolated WAL identity and filesystem location so
/// rollover and durability assertions cannot observe state from another test.
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
            "wal-append-extent-{prefix}-{}-{nanos}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("failed to create WAL test directory");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: WalIdentity::new(71, 32, 1),
            ..WalConfig::default()
        }
    }

    /// Return a configuration that stores every record in one 512-byte logical
    /// allocation and rolls over before admitting the second user record.
    fn aligned_rollover_config(&self) -> WalConfig {
        let mut config = self.config();

        config.max_record_size = 16;
        config.record_alignment = 512;

        // Physical segment layout:
        //
        // 68-byte segment header + 512-byte user record + 512-byte seal.
        //
        // The configured size must itself be alignment-compatible, so 1536 is
        // the smallest convenient boundary that admits the first record and
        // its future seal while forcing the second record into a new segment.
        config.target_segment_size = 1536;

        config
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Ensures append reports framing and alignment padding, not only payload size.
///
/// Realistic bug caught:
///
/// A caller that derives the durability target from the payload length could
/// acknowledge a record while its checksum, framing, or alignment padding
/// still lies beyond the durable frontier.
#[test]
fn aligned_append_returns_complete_logical_extent() {
    let test_dir = TestDir::new("aligned");

    let (mut wal, _) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.aligned_rollover_config(),
        (),
    )
    .unwrap();

    let extent = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();

    assert_eq!(extent.start_lsn, Lsn::ZERO);
    assert_eq!(extent.end_lsn, Lsn::new(512));
    assert_eq!(
        extent.end_lsn.checked_distance_from(extent.start_lsn),
        Some(512)
    );
    assert_eq!(wal.next_lsn(), extent.end_lsn);
}

/// Ensures rollover metadata is outside the returned user-record interval.
///
/// Realistic bug caught:
///
/// Returning the pre-rollover LSN as the user record's start would make the
/// interval include the preceding segment seal and misidentify where the
/// actual user record can be read.
#[test]
fn rollover_returns_only_the_new_user_record_extent() {
    let test_dir = TestDir::new("rollover");

    let (mut wal, _) = Wal::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.aligned_rollover_config(),
        (),
    )
    .unwrap();

    let first = wal
        .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
        .unwrap();

    let second = wal
        .append(RecordType::new(record_types::USER_MIN + 1), &[2u8; 16])
        .unwrap();

    assert_eq!(first.start_lsn, Lsn::ZERO);
    assert_eq!(first.end_lsn, Lsn::new(512));

    // The segment seal occupies [512, 1024). The new segment header consumes
    // physical file bytes but no logical LSN space.
    assert_eq!(second.start_lsn, Lsn::new(1024));
    assert_eq!(second.end_lsn, Lsn::new(1536));

    // The returned extent covers only the second user record. It excludes both
    // the preceding seal and the new physical segment header.
    assert_eq!(
        second.end_lsn.checked_distance_from(second.start_lsn),
        Some(512)
    );

    assert_eq!(wal.active_segment_id(), Some(2));
    assert_eq!(wal.next_lsn(), second.end_lsn);
}

/// Ensures the record is considered durable only when its complete half-open
/// interval lies behind the durable frontier.
///
/// Realistic bug caught:
///
/// Synchronizing through `start_lsn` can return successfully even though bytes
/// belonging to the appended record have not reached stable storage.
#[test]
fn complete_record_requires_durable_frontier_at_end_lsn() {
    let test_dir = TestDir::new("durability");

    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let extent = handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(extent.start_lsn, Lsn::ZERO);
    assert_eq!(extent.end_lsn, Lsn::new(37));

    assert!(!extent.is_durable_at(handle.durable_lsn()));
    assert!(!extent.is_durable_at(Lsn::new(36)));
    assert!(extent.is_durable_at(extent.end_lsn));

    handle.sync_through(extent.end_lsn).unwrap();

    assert_eq!(handle.durable_lsn(), extent.end_lsn);
    assert!(extent.is_durable_at(handle.durable_lsn()));
}
