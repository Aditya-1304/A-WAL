use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{SEGMENT_HEADER_LEN, WalConfig},
    error::WalError,
    format::{
        record_header::RecordHeader,
        segment_header::{SegmentHeader, compression_algorithms},
    },
    io::{
        control_file::{ControlFile, FsControlFileStore},
        directory::{FsSegmentDirectory, NewSegment, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_flags, record_types},
    wal::engine::{SegmentSealPayload, Wal},
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must follow the Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "wal-recovery-structure-{name}-{}-{nonce}",
            process::id()
        ));

        fs::create_dir_all(&path).expect("test WAL directory must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: identity(),
            ..WalConfig::default()
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn identity() -> WalIdentity {
    WalIdentity::new(71, 81, 1)
}

fn encode_record(record_type: RecordType, payload: &[u8], lsn: Lsn) -> Vec<u8> {
    let mut header = RecordHeader::new(
        record_type,
        record_flags::NONE,
        payload.len() as u32,
        lsn,
        RecordHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum(payload).unwrap();

    let mut bytes = header.encode();
    bytes.extend_from_slice(payload);
    bytes
}

fn user_record(discriminator: u16, payload: &[u8], lsn: Lsn) -> Vec<u8> {
    encode_record(
        RecordType::new(record_types::USER_MIN + discriminator),
        payload,
        lsn,
    )
}

fn seal_record(payload: SegmentSealPayload, lsn: Lsn) -> Vec<u8> {
    encode_record(record_types::SEGMENT_SEAL, &payload.encode(), lsn)
}

fn create_segment(
    directory: &FsSegmentDirectory,
    segment_id: u64,
    base_lsn: Lsn,
    records: &[Vec<u8>],
) {
    let mut header = SegmentHeader::new(
        identity(),
        segment_id,
        base_lsn,
        compression_algorithms::NONE,
        SegmentHeader::SUPPORTED_VERSION,
    );
    header.finalize_checksum();

    let mut file = directory
        .create_segment(NewSegment {
            segment_id,
            base_lsn,
            header,
        })
        .unwrap();

    for record in records {
        file.append_all(record).unwrap();
    }

    file.sync().unwrap();
}

fn segment_path(directory: &FsSegmentDirectory, segment_id: u64) -> PathBuf {
    directory
        .list_segments()
        .unwrap()
        .into_iter()
        .find(|meta| meta.segment_id == segment_id)
        .expect("requested test segment must exist")
        .path
}

fn build_three_segment_history(directory: &FsSegmentDirectory) {
    let first = user_record(0, b"first", Lsn::ZERO);
    let first_seal_lsn = Lsn::new(first.len() as u64);
    let first_seal = seal_record(
        SegmentSealPayload {
            segment_id: 1,
            record_count: 1,
            logical_bytes: first.len() as u64,
        },
        first_seal_lsn,
    );
    let second_base = Lsn::new((first.len() + first_seal.len()) as u64);
    let second = user_record(1, b"second", second_base);
    let second_seal_lsn = Lsn::new(second_base.as_u64() + second.len() as u64);
    let second_seal = seal_record(
        SegmentSealPayload {
            segment_id: 2,
            record_count: 1,
            logical_bytes: second.len() as u64,
        },
        second_seal_lsn,
    );
    let third_base = Lsn::new(second_seal_lsn.as_u64() + second_seal.len() as u64);
    let third = user_record(2, b"third", third_base);

    create_segment(directory, 1, Lsn::ZERO, &[first, first_seal]);
    create_segment(directory, 2, second_base, &[second, second_seal]);
    create_segment(directory, 3, third_base, &[third]);
}

fn build_history_with_first_seal(
    directory: &FsSegmentDirectory,
    seal_payload: SegmentSealPayload,
    record_after_seal: bool,
) {
    let first = user_record(0, b"first", Lsn::ZERO);
    let seal_lsn = Lsn::new(first.len() as u64);
    let seal = seal_record(seal_payload, seal_lsn);
    let mut first_records = vec![first, seal];
    let mut second_base = first_records.iter().map(Vec::len).sum::<usize>() as u64;

    if record_after_seal {
        let trailing = user_record(9, b"after-seal", Lsn::new(second_base));
        second_base += trailing.len() as u64;
        first_records.push(trailing);
    }

    let second = user_record(1, b"second", Lsn::new(second_base));

    create_segment(directory, 1, Lsn::ZERO, &first_records);
    create_segment(directory, 2, Lsn::new(second_base), &[second]);
}

fn publish_clean_witness(path: &Path) {
    let control = ControlFile::new(identity(), None, 0, true);
    FsControlFileStore::new(path.to_path_buf())
        .publish(&control)
        .expect("clean-shutdown witness must publish");
}

/// Realistic bug caught:
///
/// Directory enumeration can skip a deleted middle segment while every
/// remaining segment is individually well formed. Recovery must reject the
/// resulting discontinuity instead of silently accepting incomplete history.
#[test]
fn recovery_rejects_missing_middle_segment() {
    let test_dir = TestDir::new("missing-middle");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    build_three_segment_history(&directory);
    fs::remove_file(segment_path(&directory, 2)).unwrap();

    let result = Wal::open(directory, test_dir.config(), ());

    assert!(matches!(
        result,
        Err(WalError::SegmentContinuityViolation {
            expected_segment_id: 2,
            found_segment_id: 3,
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// With no successor segment, directory continuity checks have no evidence that
/// the only acknowledged segment ever existed. The durable MANIFEST must keep
/// that deletion from being mistaken for a brand-new empty WAL.
#[test]
fn recovery_rejects_deletion_of_the_only_durable_segment() {
    let test_dir = TestDir::new("missing-only-segment");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let segment_path = {
        let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
        let _ = wal
            .append(RecordType::new(record_types::USER_MIN), b"durable")
            .unwrap();
        wal.sync().unwrap();
        segment_path(&directory, 1)
    };

    fs::remove_file(segment_path).unwrap();

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::MissingDurableWalHistory {
            expected_segment_id: 1,
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// A WAL created before MANIFEST support may already contain acknowledged
/// history. A writable upgrade must publish a witness before serving traffic;
/// otherwise deletion before the first post-upgrade write would remain silent.
#[test]
fn writable_open_migrates_legacy_history_before_returning() {
    let test_dir = TestDir::new("legacy-manifest-migration");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let record = user_record(0, b"legacy-durable", Lsn::ZERO);
    create_segment(&directory, 1, Lsn::ZERO, &[record]);

    let segment_path = segment_path(&directory, 1);
    {
        let (_wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
    }
    fs::remove_file(segment_path).unwrap();

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::MissingDurableWalHistory {
            expected_segment_id: 1,
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// Removing the newest acknowledged segment leaves a valid sealed prefix, so
/// adjacent-segment validation alone would silently recover stale state. The
/// MANIFEST must prove that the later durable tail is required.
#[test]
fn recovery_rejects_deletion_of_the_newest_durable_segment() {
    let test_dir = TestDir::new("missing-newest-segment");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let mut config = test_dir.config();
    config.max_record_size = 64;
    config.target_segment_size = SEGMENT_HEADER_LEN + (32 + 64) + (32 + 24);

    let newest_path = {
        let (mut wal, _) = Wal::open(directory.clone(), config.clone(), ()).unwrap();
        let _ = wal
            .append(RecordType::new(record_types::USER_MIN), &[1; 64])
            .unwrap();
        let _ = wal
            .append(RecordType::new(record_types::USER_MIN + 1), &[2; 64])
            .unwrap();
        wal.sync().unwrap();
        assert_eq!(wal.active_segment_id(), Some(2));
        segment_path(&directory, 2)
    };

    fs::remove_file(newest_path).unwrap();

    assert!(matches!(
        Wal::open(directory, config, ()),
        Err(WalError::MissingDurableWalHistory {
            expected_segment_id: 2,
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// Maximal-prefix repair is correct only for an uncertain crash suffix. If a
/// checksum failure begins before the MANIFEST's acknowledged durable LSN,
/// truncating it would silently discard a record whose sync already succeeded.
#[test]
fn recovery_never_repairs_below_the_manifest_durable_tail() {
    let test_dir = TestDir::new("corrupt-witnessed-tail");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

    let path = {
        let (mut wal, _) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();
        let _ = wal
            .append(RecordType::new(record_types::USER_MIN), b"durable")
            .unwrap();
        wal.sync().unwrap();
        segment_path(&directory, 1)
    };

    let original_len = fs::metadata(&path).unwrap().len();
    let mut bytes = fs::read(&path).unwrap();
    bytes[SEGMENT_HEADER_LEN as usize + RecordHeader::ENCODED_LEN] ^= 0xff;
    fs::write(&path, bytes).unwrap();

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::MissingDurableWalHistory {
            expected_segment_id: 1,
            ..
        })
    ));
    assert_eq!(fs::metadata(path).unwrap().len(), original_len);
}

/// Realistic bug caught:
///
/// Adjacent segment IDs alone do not prove a contiguous logical WAL. A segment
/// whose base LSN jumps forward would otherwise hide durable bytes.
#[test]
fn recovery_rejects_lsn_gap_between_segments() {
    let test_dir = TestDir::new("lsn-gap");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first = user_record(0, b"first", Lsn::ZERO);
    let seal_lsn = Lsn::new(first.len() as u64);
    let seal = seal_record(
        SegmentSealPayload {
            segment_id: 1,
            record_count: 1,
            logical_bytes: first.len() as u64,
        },
        seal_lsn,
    );
    let expected_second_base = (first.len() + seal.len()) as u64;
    let gapped_second_base = Lsn::new(expected_second_base + 64);
    let second = user_record(1, b"second", gapped_second_base);

    create_segment(&directory, 1, Lsn::ZERO, &[first, seal]);
    create_segment(&directory, 2, gapped_second_base, &[second]);

    let result = Wal::open(directory, test_dir.config(), ());

    assert!(matches!(
        result,
        Err(WalError::SegmentContinuityViolation {
            expected_segment_id: 2,
            found_segment_id: 2,
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// A complete-record boundary is not proof that a historical segment finished
/// publication. Without a terminal seal, complete records may still be missing.
#[test]
fn recovery_rejects_non_latest_segment_without_seal() {
    let test_dir = TestDir::new("missing-seal");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first = user_record(0, b"first", Lsn::ZERO);
    let second_base = Lsn::new(first.len() as u64);
    let second = user_record(1, b"second", second_base);

    create_segment(&directory, 1, Lsn::ZERO, &[first]);
    create_segment(&directory, 2, second_base, &[second]);

    let result = Wal::open(directory, test_dir.config(), ());

    assert!(matches!(
        result,
        Err(WalError::MissingSegmentSeal { segment_id: 1 })
    ));
}

/// Realistic bug caught:
///
/// Truncation immediately before a seal leaves only complete user records, so
/// ordinary torn-record detection cannot distinguish it from a valid segment.
#[test]
fn recovery_rejects_old_segment_truncated_exactly_before_seal() {
    let test_dir = TestDir::new("truncated-before-seal");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    build_three_segment_history(&directory);
    let first_record_len = user_record(0, b"first", Lsn::ZERO).len() as u64;
    fs::OpenOptions::new()
        .write(true)
        .open(segment_path(&directory, 1))
        .unwrap()
        .set_len(SEGMENT_HEADER_LEN + first_record_len)
        .unwrap();

    let result = Wal::open(directory, test_dir.config(), ());

    assert!(matches!(
        result,
        Err(WalError::MissingSegmentSeal { segment_id: 1 })
    ));
}

#[test]
fn recovery_rejects_seal_with_wrong_segment_id() {
    let test_dir = TestDir::new("wrong-seal-segment");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_len = user_record(0, b"first", Lsn::ZERO).len() as u64;
    build_history_with_first_seal(
        &directory,
        SegmentSealPayload {
            segment_id: 99,
            record_count: 1,
            logical_bytes: first_len,
        },
        false,
    );

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::InvalidSegmentSeal { segment_id: 1, .. })
    ));
}

#[test]
fn recovery_rejects_seal_with_wrong_record_count() {
    let test_dir = TestDir::new("wrong-seal-count");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_len = user_record(0, b"first", Lsn::ZERO).len() as u64;
    build_history_with_first_seal(
        &directory,
        SegmentSealPayload {
            segment_id: 1,
            record_count: 2,
            logical_bytes: first_len,
        },
        false,
    );

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::InvalidSegmentSeal { segment_id: 1, .. })
    ));
}

#[test]
fn recovery_rejects_seal_with_wrong_logical_bytes() {
    let test_dir = TestDir::new("wrong-seal-bytes");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_len = user_record(0, b"first", Lsn::ZERO).len() as u64;
    build_history_with_first_seal(
        &directory,
        SegmentSealPayload {
            segment_id: 1,
            record_count: 1,
            logical_bytes: first_len + 1,
        },
        false,
    );

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::InvalidSegmentSeal { segment_id: 1, .. })
    ));
}

/// Realistic bug caught:
///
/// Treating the first seal as advisory would let later bytes masquerade as part
/// of the same completed segment even though the seal declared its exact end.
#[test]
fn recovery_rejects_records_after_segment_seal() {
    let test_dir = TestDir::new("record-after-seal");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first_len = user_record(0, b"first", Lsn::ZERO).len() as u64;
    build_history_with_first_seal(
        &directory,
        SegmentSealPayload {
            segment_id: 1,
            record_count: 1,
            logical_bytes: first_len,
        },
        true,
    );

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::RecordAfterSegmentSeal { segment_id: 1, .. })
    ));
}

/// Realistic bug caught:
///
/// Canonical segment creation synchronizes its header before rename. Deleting a
/// canonical file with a corrupt header could therefore discard durable data.
#[test]
fn recovery_rejects_invalid_latest_canonical_header() {
    let test_dir = TestDir::new("invalid-latest-header");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let first = user_record(0, b"first", Lsn::ZERO);
    create_segment(&directory, 1, Lsn::ZERO, &[first]);
    let path = segment_path(&directory, 1);
    let original_length = fs::metadata(&path).unwrap().len();
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    fs::write(&path, bytes).unwrap();

    let result = Wal::open(directory, test_dir.config(), ());

    assert!(matches!(result, Err(WalError::BadMagic { .. })));
    assert_eq!(fs::metadata(path).unwrap().len(), original_length);
}

/// Realistic bug caught:
///
/// A clean-shutdown witness describes process shutdown, not the continued
/// existence of every older segment. It must never bypass full history checks.
#[test]
fn clean_witness_rejects_missing_middle_segment() {
    let test_dir = TestDir::new("clean-missing-middle");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    build_three_segment_history(&directory);
    publish_clean_witness(test_dir.path());
    fs::remove_file(segment_path(&directory, 2)).unwrap();

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::SegmentContinuityViolation { .. })
    ));
}

#[test]
fn clean_witness_rejects_corrupt_old_segment() {
    let test_dir = TestDir::new("clean-corrupt-old");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    build_three_segment_history(&directory);
    publish_clean_witness(test_dir.path());
    let path = segment_path(&directory, 1);
    let mut bytes = fs::read(&path).unwrap();
    let payload_offset = SEGMENT_HEADER_LEN as usize + RecordHeader::ENCODED_LEN;
    bytes[payload_offset] ^= 0xff;
    fs::write(path, bytes).unwrap();

    assert!(matches!(
        Wal::open(directory, test_dir.config(), ()),
        Err(WalError::CorruptionInSealedSegment)
    ));
}
