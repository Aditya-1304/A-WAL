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
        engine::SegmentSealPayload,
        recovery_observer::{RecoveryCallbacks, RecoveryObserver},
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
    seal_seen: bool,
    tail_error_lsn: Option<Lsn>,
    tail_error: Option<WalError>,
}

struct ValidatedRecord {
    header: RecordHeader,
    payload: Vec<u8>,
    next_file_offset: u64,
    next_lsn: Lsn,
}

pub fn recover<D: SegmentDirectory>(
    directory: &D,
    control_store: &FsControlFileStore,
    config: &WalConfig,
) -> Result<RecoveredWal<D::File>, WalError> {
    recover_with_observer(directory, control_store, config, None)
}

pub(crate) fn recover_with_observer<D: SegmentDirectory>(
    directory: &D,
    control_store: &FsControlFileStore,
    config: &WalConfig,
    observer: Option<&dyn RecoveryObserver>,
) -> Result<RecoveredWal<D::File>, WalError> {
    let observer = RecoveryCallbacks::new(observer);
    let started = Instant::now();
    let mut report = RecoveryReport::empty();

    let control = control_store.load_for_recovery(config.identity)?;
    let control_checkpoint = control.as_ref().and_then(|control| {
        control
            .last_checkpoint_lsn
            .map(|lsn| (lsn, control.checkpoint_no))
    });

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
    // retention may remove an arbitrary sealed prefix, so the first retained
    // segment need not have ID one or base LSN zero. once that first segment is
    // established, every later segment must form one exact adjacent history
    let mut previous_segment_boundary: Option<(SegmentId, Lsn)> = None;

    for (index, meta) in metas.iter().enumerate() {
        let is_latest = index + 1 == metas.len();

        let recovered = recover_segment(
            directory,
            meta,
            is_latest,
            config,
            &mut report,
            &mut checkpoint_lsns,
            previous_segment_boundary,
            control_checkpoint,
            observer,
        )?;

        let Some(recovered) = recovered else {
            continue;
        };

        previous_segment_boundary = Some((
            recovered.descriptor.segment_id,
            recovered.descriptor.written_end_lsn()?,
        ));
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
// Recovery necessarily carries physical, logical, reporting, and observer
// state together. Keeping the boundary explicit documents that these inputs
// form one validation context rather than an arbitrary parameter list.
#[allow(clippy::too_many_arguments)]
fn recover_segment<D: SegmentDirectory>(
    directory: &D,
    meta: &SegmentMeta,
    is_latest: bool,
    config: &WalConfig,
    report: &mut RecoveryReport,
    checkpoint_lsns: &mut BTreeSet<Lsn>,
    previous_segment_boundary: Option<(SegmentId, Lsn)>,
    control_checkpoint: Option<(Lsn, u64)>,
    observer: RecoveryCallbacks<'_>,
) -> Result<Option<RecoveredSegment<D::File>>, WalError> {
    observer.on_segment_start(meta.segment_id, meta.base_lsn);

    let mut file = directory.open_segment(meta.segment_id)?;
    let original_file_len = file.len()?;

    report.note_segment_scanned(!is_latest);

    let header = match read_segment_header(&file) {
        Ok(header) => header,

        Err(source) => {
            observer.on_corruption_found(meta.base_lsn, &source);
            report.note_corruption();

            // canonical segment creation publishes a fully written and synced
            // header through atomic rename. Therefore, an invalid canonical
            // header is not a repairable torn record tail. deleting it could
            // silently discard an entire durable segment
            if is_latest {
                return Err(source);
            }

            return Err(WalError::CorruptionInSealedSegment);
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

    // Sealed state is established only after the physical seal record and its
    // payload have been validated. File position alone must never imply that a
    // historical segment was completed.
    let mut descriptor =
        SegmentDescriptor::from_header_with_sealed(&header, original_file_len, false)?;

    if let Some((previous_segment_id, previous_end_lsn)) = previous_segment_boundary {
        let expected_segment_id = previous_segment_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        if descriptor.segment_id != expected_segment_id || descriptor.base_lsn != previous_end_lsn {
            return Err(WalError::SegmentContinuityViolation {
                expected_segment_id,
                found_segment_id: descriptor.segment_id,
                expected_base_lsn: previous_end_lsn,
                found_base_lsn: descriptor.base_lsn,
            });
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
        control_checkpoint,
        observer,
    )?;

    if let Some(err) = scan.tail_error {
        let corruption_lsn = scan.tail_error_lsn.unwrap_or(descriptor.base_lsn);
        observer.on_corruption_found(corruption_lsn, &err);
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
        observer.on_truncation(corruption_lsn, truncated_bytes);
        report.note_truncation(truncated_bytes);
    }

    if !is_latest && !scan.seal_seen {
        return Err(WalError::MissingSegmentSeal {
            segment_id: descriptor.segment_id,
        });
    }

    if scan.seal_seen {
        descriptor.mark_sealed();
    }

    Ok(Some(RecoveredSegment {
        file,
        header,
        descriptor,
        record_count: scan.record_count,
        last_valid_record_type: scan.last_valid_record_type,
    }))
}

// Segment scanning validates a complete durable-history context. The explicit
// arguments make each independent boundary visible at the corruption site.
#[allow(clippy::too_many_arguments)]
fn scan_segment<F: SegmentFile>(
    file: &F,
    descriptor: &SegmentDescriptor,
    header: &SegmentHeader,
    max_record_size: u32,
    record_alignment: u32,
    checkpoint_lsns: &mut BTreeSet<Lsn>,
    report: &mut RecoveryReport,
    control_checkpoint: Option<(Lsn, u64)>,
    observer: RecoveryCallbacks<'_>,
) -> Result<SegmentScanResult, WalError> {
    let mut file_offset = descriptor.header_len;
    let mut expected_lsn = descriptor.base_lsn;
    let mut record_count = 0u64;
    let mut last_valid_record_type = None;
    let mut seal_seen = false;

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
                if seal_seen {
                    return Err(WalError::RecordAfterSegmentSeal {
                        segment_id: descriptor.segment_id,
                        lsn: record.header.lsn,
                    });
                }

                if record.header.record_type == record_types::SEGMENT_SEAL {
                    validate_segment_seal(descriptor, &record, record_count)?;
                    seal_seen = true;
                }

                report.note_record_scanned(record.header.lsn);
                observer.on_records_scanned(report.records_scanned, record.header.lsn);

                if record.header.record_type == record_types::END_CHECKPOINT {
                    checkpoint_lsns.insert(record.header.lsn);
                    observer.on_checkpoint_found(
                        record.header.lsn,
                        observed_checkpoint_no(control_checkpoint, record.header.lsn),
                    );
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
                    seal_seen,
                    tail_error_lsn: Some(expected_lsn),
                    tail_error: Some(err),
                });
            }
        }
    }

    Ok(SegmentScanResult {
        valid_end_offset: file_offset,
        record_count,
        last_valid_record_type,
        seal_seen,
        tail_error_lsn: None,
        tail_error: None,
    })
}

fn validate_segment_seal(
    descriptor: &SegmentDescriptor,
    record: &ValidatedRecord,
    preceding_record_count: u64,
) -> Result<(), WalError> {
    let seal = decode_segment_seal(descriptor.segment_id, &record.payload)?;

    let preceding_logical_bytes = record
        .header
        .lsn
        .checked_distance_from(descriptor.base_lsn)
        .ok_or_else(|| WalError::InvalidSegmentSeal {
            segment_id: descriptor.segment_id,
            reason: format!(
                "seal LSN {} precedes segment base LSN {}",
                record.header.lsn.as_u64(),
                descriptor.base_lsn.as_u64(),
            ),
        })?;

    if seal.segment_id != descriptor.segment_id {
        return Err(WalError::InvalidSegmentSeal {
            segment_id: descriptor.segment_id,
            reason: format!(
                "payload names segment {}, expected {}",
                seal.segment_id, descriptor.segment_id,
            ),
        });
    }

    if seal.record_count != preceding_record_count {
        return Err(WalError::InvalidSegmentSeal {
            segment_id: descriptor.segment_id,
            reason: format!(
                "payload records {}, but recovery counted {} records before the seal",
                seal.record_count, preceding_record_count,
            ),
        });
    }

    if seal.logical_bytes != preceding_logical_bytes {
        return Err(WalError::InvalidSegmentSeal {
            segment_id: descriptor.segment_id,
            reason: format!(
                "payload covers {} logical bytes, but the seal begins after {} bytes",
                seal.logical_bytes, preceding_logical_bytes,
            ),
        });
    }

    Ok(())
}

fn decode_segment_seal(
    segment_id: SegmentId,
    payload: &[u8],
) -> Result<SegmentSealPayload, WalError> {
    if payload.len() != SegmentSealPayload::ENCODED_LEN {
        return Err(WalError::InvalidSegmentSeal {
            segment_id,
            reason: format!(
                "payload length is {}, expected {}",
                payload.len(),
                SegmentSealPayload::ENCODED_LEN,
            ),
        });
    }

    let stored_segment_id = u64::from_le_bytes(
        payload[0..8]
            .try_into()
            .expect("segment-seal segment ID has a fixed validated width"),
    );
    let record_count = u64::from_le_bytes(
        payload[8..16]
            .try_into()
            .expect("segment-seal record count has a fixed validated width"),
    );
    let logical_bytes = u64::from_le_bytes(
        payload[16..24]
            .try_into()
            .expect("segment-seal logical length has a fixed validated width"),
    );

    Ok(SegmentSealPayload {
        segment_id: stored_segment_id,
        record_count,
        logical_bytes,
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
        payload,
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
    if let Some(control) = control
        && let Some(checkpoint_lsn) = control.last_checkpoint_lsn
        && checkpoint_lsns.contains(&checkpoint_lsn)
    {
        return Some(checkpoint_lsn);
    }

    checkpoint_lsns.last().copied()
}

fn observed_checkpoint_no(control_checkpoint: Option<(Lsn, u64)>, checkpoint_lsn: Lsn) -> u64 {
    match control_checkpoint {
        Some((control_lsn, checkpoint_no)) if control_lsn == checkpoint_lsn => checkpoint_no,
        _ => 0,
    }
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
