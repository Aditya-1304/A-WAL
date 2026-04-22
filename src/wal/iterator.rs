use crate::{
    error::WalError,
    format::{
        compression::decompress_payload, record_header::RecordHeader, segment_header::SegmentHeader,
    },
    io::{directory::SegmentDirectory, segment_file::SegmentFile},
    lsn::Lsn,
    types::{RecordType, WalIdentity},
    wal::segment::SegmentDescriptor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
    pub total_len: u32,
}

#[derive(Debug)]
pub struct WalIterator<F> {
    segments: Vec<SnapshotSegment<F>>,
    current_segment_index: usize,
    next_lsn: Lsn,
    record_alignment: u32,
}

#[derive(Debug)]
pub(crate) struct SnapshotSegment<F> {
    file: F,
    header: SegmentHeader,
    descriptor: SegmentDescriptor,
}

#[derive(Debug)]
struct DecodedRecord {
    record: WalRecord,
    physical_len: u64,
}

impl<F: SegmentFile> SnapshotSegment<F> {
    fn open(file: F, header: SegmentHeader) -> Result<Self, WalError> {
        let descriptor = SegmentDescriptor::from_header(&header, file.len()?)?;
        Ok(Self {
            file,
            header,
            descriptor,
        })
    }
}

impl<F: SegmentFile> WalIterator<F> {
    pub(crate) fn new(
        segments: Vec<SnapshotSegment<F>>,
        record_alignment: u32,
        from: Lsn,
    ) -> Result<Self, WalError> {
        let end_lsn = snapshot_end_lsn(&segments)?;

        if segments.is_empty() {
            if from == Lsn::ZERO {
                return Ok(Self {
                    segments,
                    current_segment_index: 0,
                    next_lsn: from,
                    record_alignment,
                });
            }

            return Err(WalError::LsnOutOfRange { lsn: from });
        }

        if from == end_lsn {
            return Ok(Self {
                current_segment_index: segments.len(),
                segments,
                next_lsn: from,
                record_alignment,
            });
        }

        if from < segments[0].descriptor.base_lsn {
            return Err(WalError::LsnPruned { lsn: from });
        }

        let segment_index =
            find_segment_index(&segments, from).ok_or(WalError::LsnOutOfRange { lsn: from })?;
        let segment = &segments[segment_index];

        if from >= segment.descriptor.written_end_lsn()? {
            return Err(WalError::LsnOutOfRange { lsn: from });
        }

        let _ = decode_record_at_lsn(segment, record_alignment, from, true)?;

        Ok(Self {
            segments,
            current_segment_index: segment_index,
            next_lsn: from,
            record_alignment,
        })
    }

    pub fn next(&mut self) -> Result<Option<WalRecord>, WalError> {
        loop {
            let Some(segment) = self.segments.get(self.current_segment_index) else {
                return Ok(None);
            };

            let segment_end_lsn = segment.descriptor.written_end_lsn()?;
            if self.next_lsn == segment_end_lsn {
                self.current_segment_index += 1;

                if let Some(next_segment) = self.segments.get(self.current_segment_index) {
                    if next_segment.descriptor.base_lsn != self.next_lsn {
                        return Err(WalError::SegmentOrderingViolation);
                    }
                }

                continue;
            }

            let decoded =
                decode_record_at_lsn(segment, self.record_alignment, self.next_lsn, false)?;

            self.next_lsn = self
                .next_lsn
                .checked_add_bytes(decoded.physical_len)
                .ok_or(WalError::ReservationOverflow)?;

            return Ok(Some(decoded.record));
        }
    }

    pub fn current_lsn(&self) -> Lsn {
        self.next_lsn
    }
}

pub(crate) fn snapshot_segments<D: SegmentDirectory>(
    directory: &D,
    expected_identity: WalIdentity,
) -> Result<Vec<SnapshotSegment<D::File>>, WalError> {
    let metas = directory.list_segments()?;
    let mut segments = Vec::with_capacity(metas.len());
    let mut previous_end_lsn = None;

    for meta in metas {
        let file = directory.open_segment(meta.segment_id)?;
        let header = read_segment_header(&file)?;

        if header.segment_id != meta.segment_id || header.base_lsn != meta.base_lsn {
            return Err(WalError::FilenameHeaderMismatch);
        }

        if header.identity() != expected_identity {
            return Err(WalError::IdentityMismatch {
                expected: expected_identity,
                found: header.identity(),
            });
        }

        let segment = SnapshotSegment::open(file, header)?;

        if let Some(previous_end_lsn) = previous_end_lsn {
            if segment.descriptor.base_lsn < previous_end_lsn {
                return Err(WalError::SegmentOrderingViolation);
            }
        }

        previous_end_lsn = Some(segment.descriptor.written_end_lsn()?);
        segments.push(segment);
    }

    Ok(segments)
}

pub(crate) fn snapshot_segments_through<D: SegmentDirectory>(
    directory: &D,
    expected_identity: WalIdentity,
    end_lsn: Lsn,
) -> Result<Vec<SnapshotSegment<D::File>>, WalError> {
    let metas = directory.list_segments()?;
    let mut segments = Vec::with_capacity(metas.len());
    let mut previous_end_lsn = None;

    for meta in metas {
        if meta.base_lsn >= end_lsn {
            break;
        }

        let file = directory.open_segment(meta.segment_id)?;
        let header = read_segment_header(&file)?;

        if header.segment_id != meta.segment_id || header.base_lsn != meta.base_lsn {
            return Err(WalError::FilenameHeaderMismatch);
        }

        if header.identity() != expected_identity {
            return Err(WalError::IdentityMismatch {
                expected: expected_identity,
                found: header.identity(),
            });
        }

        let mut segment = SnapshotSegment::open(file, header)?;

        if let Some(previous_end_lsn) = previous_end_lsn {
            if segment.descriptor.base_lsn < previous_end_lsn {
                return Err(WalError::SegmentOrderingViolation);
            }
        }

        let segment_end_lsn = segment.descriptor.written_end_lsn()?;
        if end_lsn < segment_end_lsn {
            let retained_logical_len = end_lsn
                .checked_distance_from(segment.descriptor.base_lsn)
                .ok_or(WalError::BrokenDurabilityContract)?;
            let retained_file_len = segment
                .descriptor
                .header_len
                .checked_add(retained_logical_len)
                .ok_or(WalError::ReservationOverflow)?;

            segment.descriptor.set_file_len(retained_file_len)?;
            segments.push(segment);
            break;
        }

        previous_end_lsn = Some(segment_end_lsn);
        segments.push(segment);
    }

    Ok(segments)
}

pub(crate) fn read_record_at_snapshot<F: SegmentFile>(
    segments: &[SnapshotSegment<F>],
    record_alignment: u32,
    lsn: Lsn,
) -> Result<WalRecord, WalError> {
    if segments.is_empty() {
        return Err(WalError::LsnOutOfRange { lsn });
    }

    if lsn < segments[0].descriptor.base_lsn {
        return Err(WalError::LsnPruned { lsn });
    }

    let segment_index = find_segment_index(segments, lsn).ok_or(WalError::LsnOutOfRange { lsn })?;
    let segment = &segments[segment_index];

    if lsn >= segment.descriptor.written_end_lsn()? {
        return Err(WalError::LsnOutOfRange { lsn });
    }

    let decoded = decode_record_at_lsn(segment, record_alignment, lsn, true)?;
    Ok(decoded.record)
}

fn decode_record_at_lsn<F: SegmentFile>(
    segment: &SnapshotSegment<F>,
    record_alignment: u32,
    expected_lsn: Lsn,
    exact_lookup: bool,
) -> Result<DecodedRecord, WalError> {
    let file_offset = segment.descriptor.lsn_to_file_offset(expected_lsn)?;

    let mut header_bytes = [0u8; RecordHeader::ENCODED_LEN];
    if let Err(err) = read_exact_at(&segment.file, file_offset, &mut header_bytes) {
        if exact_lookup {
            return Err(WalError::LsnOutOfRange { lsn: expected_lsn });
        }
        return Err(err);
    }

    let header = match RecordHeader::decode(&header_bytes) {
        Ok(header) => header,
        Err(err) if exact_lookup => {
            let _ = err;
            return Err(WalError::LsnOutOfRange { lsn: expected_lsn });
        }
        Err(err) => return Err(err),
    };

    if header.lsn != expected_lsn {
        if exact_lookup {
            return Err(WalError::LsnOutOfRange { lsn: expected_lsn });
        }

        return Err(WalError::NonMonotonicLsn {
            expected: expected_lsn,
            found: header.lsn,
        });
    }

    let mut payload = vec![0u8; header.payload_len as usize];
    read_exact_at(
        &segment.file,
        file_offset + RecordHeader::ENCODED_LEN as u64,
        &mut payload,
    )?;
    header.verify_checksum(&header_bytes, &payload)?;

    let physical_len = physical_record_len(record_alignment, header.total_len() as usize)?;
    let record_end = file_offset
        .checked_add(physical_len as u64)
        .ok_or(WalError::ReservationOverflow)?;

    if record_end > segment.descriptor.file_len {
        return Err(WalError::ShortRead);
    }

    let decoded_payload = decompress_payload(
        segment.header.compression_algorithm,
        header.is_compressed(),
        &payload,
    )?;

    Ok(DecodedRecord {
        record: WalRecord {
            lsn: header.lsn,
            record_type: header.record_type,
            payload: decoded_payload,
            total_len: header.total_len(),
        },
        physical_len: physical_len as u64,
    })
}

fn find_segment_index<F>(segments: &[SnapshotSegment<F>], lsn: Lsn) -> Option<usize> {
    let upper_bound = segments.partition_point(|segment| segment.descriptor.base_lsn <= lsn);
    upper_bound.checked_sub(1)
}

fn snapshot_end_lsn<F>(segments: &[SnapshotSegment<F>]) -> Result<Lsn, WalError> {
    match segments.last() {
        Some(segment) => segment.descriptor.written_end_lsn(),
        None => Ok(Lsn::ZERO),
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
        format::segment_header::{SegmentHeader, compression_algorithms},
        io::{
            directory::{FsSegmentDirectory, NewSegment, SegmentDirectory},
            segment_file::SegmentFile,
        },
        types::{WalIdentity, record_flags, record_types},
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
                .join(format!("wal-iterator-{prefix}-{}-{nanos}", process::id()));

            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
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

    fn create_segment(
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

    #[test]
    fn read_record_at_returns_exact_uncompressed_record() {
        let test_dir = TestDir::new("read-exact");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN),
                record_flags::NONE,
                b"hello".to_vec(),
                Lsn::ZERO,
            )],
            0,
        );

        let segments = snapshot_segments(&directory, sample_identity()).unwrap();
        let record = read_record_at_snapshot(&segments, 0, Lsn::ZERO).unwrap();

        assert_eq!(record.lsn, Lsn::ZERO);
        assert_eq!(record.record_type, RecordType::new(record_types::USER_MIN));
        assert_eq!(record.payload, b"hello");
        assert_eq!(record.total_len, 37);
    }

    #[test]
    fn read_record_at_rejects_middle_of_record() {
        let test_dir = TestDir::new("middle");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN),
                record_flags::NONE,
                b"hello".to_vec(),
                Lsn::ZERO,
            )],
            0,
        );

        let segments = snapshot_segments(&directory, sample_identity()).unwrap();
        let err = read_record_at_snapshot(&segments, 0, Lsn::new(1)).unwrap_err();

        assert!(matches!(err, WalError::LsnOutOfRange { lsn } if lsn == Lsn::new(1)));
    }

    #[test]
    fn iterator_walks_records_across_segments() {
        let test_dir = TestDir::new("cross-segment");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN),
                record_flags::NONE,
                b"hello".to_vec(),
                Lsn::ZERO,
            )],
            0,
        );

        create_segment(
            &directory,
            2,
            Lsn::new(37),
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN + 1),
                record_flags::NONE,
                b"world!".to_vec(),
                Lsn::new(37),
            )],
            0,
        );

        let segments = snapshot_segments(&directory, sample_identity()).unwrap();
        let mut iter = WalIterator::new(segments, 0, Lsn::ZERO).unwrap();

        let first = iter.next().unwrap().unwrap();
        let second = iter.next().unwrap().unwrap();
        let third = iter.next().unwrap();

        assert_eq!(first.lsn, Lsn::ZERO);
        assert_eq!(first.payload, b"hello");
        assert_eq!(second.lsn, Lsn::new(37));
        assert_eq!(second.payload, b"world!");
        assert!(third.is_none());
    }

    #[test]
    fn read_record_at_decompresses_lz4_payload() {
        let test_dir = TestDir::new("lz4");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let original = b"lz4 payload lz4 payload lz4 payload".to_vec();
        let compressed = lz4_flex::compress_prepend_size(&original);

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::LZ4,
            &[(
                RecordType::new(record_types::USER_MIN),
                record_flags::COMPRESSED,
                compressed.clone(),
                Lsn::ZERO,
            )],
            0,
        );

        let segments = snapshot_segments(&directory, sample_identity()).unwrap();
        let record = read_record_at_snapshot(&segments, 0, Lsn::ZERO).unwrap();

        assert_eq!(record.payload, original);
        assert_eq!(
            record.total_len,
            (RecordHeader::ENCODED_LEN + compressed.len()) as u32
        );
    }

    #[test]
    fn iter_from_written_end_is_empty() {
        let test_dir = TestDir::new("end-empty");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            compression_algorithms::NONE,
            &[(
                RecordType::new(record_types::USER_MIN),
                record_flags::NONE,
                b"hello".to_vec(),
                Lsn::ZERO,
            )],
            0,
        );

        let segments = snapshot_segments(&directory, sample_identity()).unwrap();
        let mut iter = WalIterator::new(segments, 0, Lsn::new(37)).unwrap();

        assert!(iter.next().unwrap().is_none());
    }
}
