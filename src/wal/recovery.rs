use std::{collections::BTreeSet, time::Instant};

use crate::{
    config::WalConfig,
    error::WalError,
    format::{
        record_header::RecordHeader,
        segment_header::{SegmentHeader, compression_algorithms},
    },
    io::{
        control_file::{ControlFile, FsControlFileStore},
        directory::{SegmentDirectory, SegmentMeta},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, SegmentId, record_types},
    wal::{
        report::RecoveryReport,
        segment::{ActiveSegment, SegmentDescriptor},
    },
};

pub struct RecoveredWal<F> {
    pub active_segment: Option<ActiveSegment<F>>,
    pub first_lsn: Option<Lsn>,
    pub next_lsn: Lsn,
    pub durable_lsn: Lsn,
    pub current_wal_size: u64,
    pub next_segment_id: SegmentId,
    pub active_segment_record_count: u64,
    pub report: RecoveryReport,
}

struct RecoveredSegment<F> {
    file: F,
    header: SegmentHeader,
    descriptor: SegmentDescriptor,
    record_count: u64,
    last_valid_record_type: Option<RecordType>,
}

struct SegmentScanResult {
    valid_end_offset: u64,
    record_count: u64,
    last_valid_record_type: Option<RecordType>,
    tail_error: Option<WalError>,
}

struct ValidatedRecord {
    header: RecordHeader,
    next_file_offset: u64,
    next_lsn: Lsn,
}

pub fn recover<D: SegmentDirectory>(
    directory: &D,
    control_store: &FsControlFileStore,
    config: &WalConfig,
) -> Result<RecoveredWal<D::File>, WalError> {
    let started = Instant::now();
    let mut report = RecoveryReport::empty();

    let control = control_store.load_for_recovery(config.identity)?;
    report.mark_clean_shutdown(
        control
            .as_ref()
            .is_some_and(|control| control.clean_shutdown),
    );

    let metas = directory.list_segments()?;
    if metas.is_empty() {
        report.set_next_lsn(Lsn::ZERO);
        report.set_checkpoint_lsn(None);
        report.set_recovery_duration(started.elapsed());

        return Ok(RecoveredWal {
            active_segment: None,
            first_lsn: None,
            next_lsn: Lsn::ZERO,
            durable_lsn: Lsn::ZERO,
            current_wal_size: 0,
            next_segment_id: 1,
            active_segment_record_count: 0,
            report,
        });
    }

    let mut current_wal_size = 0u64;
    let mut last_kept_segment: Option<RecoveredSegment<D::File>> = None;
    let mut checkpoint_lsns = BTreeSet::new();
    let mut previous_end_lsn = None;

    for (index, meta) in metas.iter().enumerate() {
        let is_latest = index + 1 == metas.len();

        let recovered = recover_segment(
            directory,
            meta,
            is_latest,
            config,
            &mut report,
            &mut checkpoint_lsns,
            previous_end_lsn,
        )?;

        let Some(recovered) = recovered else {
            continue;
        };

        previous_end_lsn = Some(recovered.descriptor.written_end_lsn()?);
        current_wal_size = current_wal_size
            .checked_add(recovered.descriptor.file_len)
            .ok_or(WalError::ReservationOverflow)?;

        last_kept_segment = Some(recovered);
    }

    let checkpoint_lsn = select_checkpoint_lsn(control.as_ref(), &checkpoint_lsns);
    report.set_checkpoint_lsn(checkpoint_lsn);

    let next_lsn = match last_kept_segment.as_ref() {
        Some(segment) => segment.descriptor.written_end_lsn()?,
        None => Lsn::ZERO,
    };

    let next_segment_id = match last_kept_segment.as_ref() {
        Some(segment) => segment
            .descriptor
            .segment_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?,
        None => 1,
    };

    let active_segment_record_count = match last_kept_segment.as_ref() {
        Some(segment) if !segment_is_sealed(segment) => segment.record_count,
        _ => 0,
    };

    report.set_next_lsn(next_lsn);
    report.set_segments_prunable(0);
    report.set_recovery_duration(started.elapsed());

    let first_lsn = report.first_lsn;
    let active_segment = build_active_segment(last_kept_segment)?;

    Ok(RecoveredWal {
        active_segment,
        first_lsn,
        next_lsn,
        durable_lsn: next_lsn,
        current_wal_size,
        next_segment_id,
        active_segment_record_count,
        report,
    })
}

fn recover_segment<D: SegmentDirectory>(
    directory: &D,
    meta: &SegmentMeta,
    is_latest: bool,
    config: &WalConfig,
    report: &mut RecoveryReport,
    checkpoint_lsns: &mut BTreeSet<Lsn>,
    previous_end_lsn: Option<Lsn>,
) -> Result<Option<RecoveredSegment<D::File>>, WalError> {
    let mut file = directory.open_segment(meta.segment_id)?;
    let original_file_len = file.len()?;

    report.note_segment_scanned(!is_latest);

    let header = match read_segment_header(&file) {
        Ok(header) => header,
        Err(err) => {
            return handle_invalid_newest_header(
                directory,
                meta,
                is_latest,
                config,
                report,
                original_file_len,
                err,
            );
        }
    };

    if header.segment_id != meta.segment_id || header.base_lsn != meta.base_lsn {
        return Err(WalError::FilenameHeaderMismatch);
    }

    if header.identity() != config.identity {
        return Err(WalError::IdentityMismatch {
            expected: config.identity,
            found: header.identity(),
        });
    }

    let mut descriptor =
        SegmentDescriptor::from_header_with_sealed(&header, original_file_len, !is_latest)?;

    if let Some(previous_end_lsn) = previous_end_lsn {
        if descriptor.base_lsn < previous_end_lsn {
            return Err(WalError::SegmentOrderingViolation);
        }
    }

    let scan = scan_segment(
        &file,
        &descriptor,
        &header,
        config.max_record_size,
        config.record_alignment,
        checkpoint_lsns,
        report,
    )?;

    if let Some(err) = scan.tail_error {
        report.note_corruption();

        if !is_latest {
            return Err(WalError::CorruptionInSealedSegment);
        }

        if config.read_only {
            return Err(WalError::ReadOnlyTailCorruption);
        }

        if !config.truncate_tail {
            return Err(err);
        }

        let truncated_bytes = descriptor
            .file_len
            .checked_sub(scan.valid_end_offset)
            .ok_or(WalError::ReservationOverflow)?;

        file.truncate(scan.valid_end_offset)?;
        file.sync()?;
        descriptor.set_file_len(scan.valid_end_offset)?;
        report.note_truncation(truncated_bytes);
    }

    Ok(Some(RecoveredSegment {
        file,
        header,
        descriptor,
        record_count: scan.record_count,
        last_valid_record_type: scan.last_valid_record_type,
    }))
}

fn handle_invalid_newest_header<D: SegmentDirectory>(
    directory: &D,
    meta: &SegmentMeta,
    is_latest: bool,
    config: &WalConfig,
    report: &mut RecoveryReport,
    original_file_len: u64,
    err: WalError,
) -> Result<Option<RecoveredSegment<D::File>>, WalError> {
    if !is_latest {
        return Err(WalError::CorruptionInSealedSegment);
    }

    report.note_corruption();

    if config.read_only {
        return Err(WalError::ReadOnlyTailCorruption);
    }

    if !config.truncate_tail {
        return Err(err);
    }

    directory.remove_segment(meta.segment_id)?;
    report.note_truncation(original_file_len);

    Ok(None)
}

fn scan_segment<F: SegmentFile>(
    file: &F,
    descriptor: &SegmentDescriptor,
    header: &SegmentHeader,
    max_record_size: u32,
    record_alignment: u32,
    checkpoint_lsns: &mut BTreeSet<Lsn>,
    report: &mut RecoveryReport,
) -> Result<SegmentScanResult, WalError> {
    let mut file_offset = descriptor.header_len;
    let mut expected_lsn = descriptor.base_lsn;
    let mut record_count = 0u64;
    let mut last_valid_record_type = None;

    while file_offset < descriptor.file_len {
        match validate_record_at(
            file,
            descriptor.file_len,
            header,
            file_offset,
            expected_lsn,
            max_record_size,
            record_alignment,
        ) {
            Ok(record) => {
                report.note_record_scanned(record.header.lsn);

                if record.header.record_type == record_types::END_CHECKPOINT {
                    checkpoint_lsns.insert(record.header.lsn);
                }

                record_count = record_count
                    .checked_add(1)
                    .ok_or(WalError::ReservationOverflow)?;
                last_valid_record_type = Some(record.header.record_type);
                file_offset = record.next_file_offset;
                expected_lsn = record.next_lsn;
            }
            Err(err) => {
                return Ok(SegmentScanResult {
                    valid_end_offset: file_offset,
                    record_count,
                    last_valid_record_type,
                    tail_error: Some(err),
                });
            }
        }
    }

    Ok(SegmentScanResult {
        valid_end_offset: file_offset,
        record_count,
        last_valid_record_type,
        tail_error: None,
    })
}

fn validate_record_at<F: SegmentFile>(
    file: &F,
    segment_file_len: u64,
    segment_header: &SegmentHeader,
    file_offset: u64,
    expected_lsn: Lsn,
    max_record_size: u32,
    record_alignment: u32,
) -> Result<ValidatedRecord, WalError> {
    let mut header_bytes = [0u8; RecordHeader::ENCODED_LEN];
    read_exact_at(file, file_offset, &mut header_bytes)?;

    let header = RecordHeader::decode(&header_bytes)?;

    if header.payload_len > max_record_size {
        return Err(WalError::PayloadTooLarge {
            len: header.payload_len,
            max: max_record_size,
        });
    }

    if header.lsn != expected_lsn {
        return Err(WalError::NonMonotonicLsn {
            expected: expected_lsn,
            found: header.lsn,
        });
    }

    if header.is_compressed()
        && segment_header.compression_algorithm == compression_algorithms::NONE
    {
        return Err(WalError::BadRecordHeader);
    }

    let mut payload = vec![0u8; header.payload_len as usize];
    read_exact_at(
        file,
        file_offset + RecordHeader::ENCODED_LEN as u64,
        &mut payload,
    )?;
    header.verify_checksum(&header_bytes, &payload)?;

    let physical_len = physical_record_len(record_alignment, header.total_len() as usize)?;
    let next_file_offset = file_offset
        .checked_add(physical_len as u64)
        .ok_or(WalError::ReservationOverflow)?;

    if next_file_offset > segment_file_len {
        return Err(WalError::ShortRead);
    }

    let next_lsn = expected_lsn
        .checked_add_bytes(physical_len as u64)
        .ok_or(WalError::ReservationOverflow)?;

    Ok(ValidatedRecord {
        header,
        next_file_offset,
        next_lsn,
    })
}

fn segment_is_sealed<F>(segment: &RecoveredSegment<F>) -> bool {
    matches!(
        segment.last_valid_record_type,
        Some(record_type) if record_type == record_types::SEGMENT_SEAL
    )
}

fn build_active_segment<F: SegmentFile>(
    last_kept_segment: Option<RecoveredSegment<F>>,
) -> Result<Option<ActiveSegment<F>>, WalError> {
    let Some(segment) = last_kept_segment else {
        return Ok(None);
    };

    if segment_is_sealed(&segment) {
        return Ok(None);
    }

    Ok(Some(ActiveSegment::open(segment.file, segment.header)?))
}

fn select_checkpoint_lsn(
    control: Option<&ControlFile>,
    checkpoint_lsns: &BTreeSet<Lsn>,
) -> Option<Lsn> {
    if let Some(control) = control {
        if let Some(checkpoint_lsn) = control.last_checkpoint_lsn {
            if checkpoint_lsns.contains(&checkpoint_lsn) {
                return Some(checkpoint_lsn);
            }
        }
    }

    checkpoint_lsns.last().copied()
}

fn physical_record_len(record_alignment: u32, total_len: usize) -> Result<usize, WalError> {
    if record_alignment == 0 {
        return Ok(total_len);
    }

    let alignment = record_alignment as usize;
    let remainder = total_len % alignment;
    let padding = if remainder == 0 {
        0
    } else {
        alignment - remainder
    };

    total_len
        .checked_add(padding)
        .ok_or(WalError::ReservationOverflow)
}

fn read_segment_header<F: SegmentFile>(file: &F) -> Result<SegmentHeader, WalError> {
    let mut bytes = [0u8; SegmentHeader::ENCODED_LEN];
    read_exact_at(file, 0, &mut bytes)?;
    SegmentHeader::decode(&bytes)
}

fn read_exact_at<F: SegmentFile>(file: &F, offset: u64, buf: &mut [u8]) -> Result<(), WalError> {
    let mut filled = 0usize;

    while filled < buf.len() {
        let read = file.read_at(offset + filled as u64, &mut buf[filled..])?;
        if read == 0 {
            return Err(WalError::ShortRead);
        }
        filled += read;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        io::{
            control_file::FsControlFileStore,
            directory::{FsSegmentDirectory, NewSegment},
        },
        types::{WalIdentity, record_flags},
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

            let path = std::env::temp_dir()
                .join(format!("wal-recovery-{prefix}-{}-{nanos}", process::id()));

            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn config(&self) -> WalConfig {
            WalConfig {
                dir: self.path.clone(),
                identity: sample_identity(),
                ..WalConfig::default()
            }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sample_identity() -> WalIdentity {
        WalIdentity::new(11, 22, 1)
    }

    fn encode_record(
        record_type: RecordType,
        flags: u16,
        payload: &[u8],
        lsn: Lsn,
        alignment: u32,
    ) -> Vec<u8> {
        let mut header = RecordHeader::new(
            record_type,
            flags,
            payload.len() as u32,
            lsn,
            RecordHeader::SUPPORTED_VERSION,
        );
        header.finalize_checksum(payload).unwrap();

        let mut encoded = header.encode();
        encoded.extend_from_slice(payload);

        let physical_len = physical_record_len(alignment, header.total_len() as usize).unwrap();
        encoded.resize(physical_len, 0);

        encoded
    }

    fn create_manual_segment(
        directory: &FsSegmentDirectory,
        segment_id: u64,
        base_lsn: Lsn,
        compression_algorithm: u8,
        records: &[(RecordType, u16, Vec<u8>, Lsn)],
        alignment: u32,
    ) {
        let mut header = SegmentHeader::new(
            sample_identity(),
            segment_id,
            base_lsn,
            compression_algorithm,
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

        for (record_type, flags, payload, lsn) in records {
            let encoded = encode_record(*record_type, *flags, payload, *lsn, alignment);
            file.append_all(&encoded).unwrap();
        }

        file.sync().unwrap();
    }

    fn corrupt_file_byte(path: &Path, offset: usize) {
        let mut bytes = fs::read(path).unwrap();
        bytes[offset] ^= 0xFF;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn recover_empty_directory_returns_zero_state() {
        let test_dir = TestDir::new("empty");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
        let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let recovered = recover(&directory, &control_store, &test_dir.config()).unwrap();

        assert!(recovered.active_segment.is_none());
        assert_eq!(recovered.first_lsn, None);
        assert_eq!(recovered.next_lsn, Lsn::ZERO);
        assert_eq!(recovered.durable_lsn, Lsn::ZERO);
        assert_eq!(recovered.current_wal_size, 0);
        assert_eq!(recovered.report.records_scanned, 0);
    }

    #[test]
    fn recover_truncates_corrupt_tail_in_newest_segment() {
        let test_dir = TestDir::new("truncate-tail");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
        let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

        create_manual_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[
                (
                    RecordType::new(record_types::USER_MIN),
                    record_flags::NONE,
                    b"hello".to_vec(),
                    Lsn::ZERO,
                ),
                (
                    RecordType::new(record_types::USER_MIN + 1),
                    record_flags::NONE,
                    b"world".to_vec(),
                    Lsn::new(37),
                ),
            ],
            0,
        );

        let segment_path = directory
            .list_segments()
            .unwrap()
            .into_iter()
            .find(|meta| meta.segment_id == 1)
            .unwrap()
            .path;

        let second_record_payload_offset =
            SegmentHeader::ENCODED_LEN + 37 + RecordHeader::ENCODED_LEN;
        corrupt_file_byte(&segment_path, second_record_payload_offset);

        let recovered = recover(&directory, &control_store, &test_dir.config()).unwrap();

        assert_eq!(recovered.first_lsn, Some(Lsn::ZERO));
        assert_eq!(recovered.next_lsn, Lsn::new(37));
        assert_eq!(recovered.current_wal_size, 68 + 37);
        assert_eq!(recovered.report.corrupt_records_found, 1);
        assert_eq!(recovered.report.truncated_bytes, 37);
        assert!(recovered.active_segment.is_some());

        let repaired_len = directory.open_segment(1).unwrap().len().unwrap();
        assert_eq!(repaired_len, 68 + 37);
    }

    #[test]
    fn recover_read_only_refuses_truncatable_tail_corruption() {
        let test_dir = TestDir::new("read-only-tail");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
        let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

        create_manual_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[
                (
                    RecordType::new(record_types::USER_MIN),
                    record_flags::NONE,
                    b"hello".to_vec(),
                    Lsn::ZERO,
                ),
                (
                    RecordType::new(record_types::USER_MIN + 1),
                    record_flags::NONE,
                    b"world".to_vec(),
                    Lsn::new(37),
                ),
            ],
            0,
        );

        let segment_path = directory
            .list_segments()
            .unwrap()
            .into_iter()
            .find(|meta| meta.segment_id == 1)
            .unwrap()
            .path;

        let second_record_payload_offset =
            SegmentHeader::ENCODED_LEN + 37 + RecordHeader::ENCODED_LEN;
        corrupt_file_byte(&segment_path, second_record_payload_offset);

        let mut config = test_dir.config();
        config.read_only = true;

        let result = recover(&directory, &control_store, &config);
        assert!(matches!(result, Err(WalError::ReadOnlyTailCorruption)));
    }

    #[test]
    fn recover_fails_hard_on_corruption_in_older_segment() {
        let test_dir = TestDir::new("sealed-history");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
        let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

        create_manual_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[
                (
                    RecordType::new(record_types::USER_MIN),
                    record_flags::NONE,
                    b"hello".to_vec(),
                    Lsn::ZERO,
                ),
                (
                    record_types::SEGMENT_SEAL,
                    record_flags::NONE,
                    vec![0u8; 24],
                    Lsn::new(37),
                ),
            ],
            0,
        );

        create_manual_segment(
            &directory,
            2,
            Lsn::new(93),
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN + 1),
                record_flags::NONE,
                b"world".to_vec(),
                Lsn::new(93),
            )],
            0,
        );

        let segment_path = directory
            .list_segments()
            .unwrap()
            .into_iter()
            .find(|meta| meta.segment_id == 1)
            .unwrap()
            .path;

        let first_record_payload_offset = SegmentHeader::ENCODED_LEN + RecordHeader::ENCODED_LEN;
        corrupt_file_byte(&segment_path, first_record_payload_offset);

        let result = recover(&directory, &control_store, &test_dir.config());
        assert!(matches!(result, Err(WalError::CorruptionInSealedSegment)));
    }

    #[test]
    fn recover_falls_back_to_scanned_checkpoint_when_control_pointer_is_invalid() {
        let test_dir = TestDir::new("checkpoint-fallback");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
        let control_store = FsControlFileStore::new(test_dir.path().to_path_buf());

        create_manual_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[
                (
                    RecordType::new(record_types::USER_MIN),
                    record_flags::NONE,
                    b"hello".to_vec(),
                    Lsn::ZERO,
                ),
                (
                    record_types::END_CHECKPOINT,
                    record_flags::NONE,
                    Vec::new(),
                    Lsn::new(37),
                ),
            ],
            0,
        );

        let control = ControlFile::new(sample_identity(), Some(Lsn::new(9999)), 1, false);
        control_store.publish(&control).unwrap();

        let recovered = recover(&directory, &control_store, &test_dir.config()).unwrap();

        assert_eq!(recovered.report.checkpoint_lsn, Some(Lsn::new(37)));
    }
}
