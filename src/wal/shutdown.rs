use crate::{
    error::WalError,
    format::{record_header::RecordHeader, segment_header::SegmentHeader},
    io::{
        control_file::{ControlFile, FsControlFileStore},
        directory::SegmentDirectory,
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, SegmentId, WalIdentity, record_types},
    wal::segment::SegmentDescriptor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CheckpointState {
    pub last_checkpoint_lsn: Option<Lsn>,
    pub checkpoint_no: u64,
}

impl CheckpointState {
    pub fn from_control(control: Option<&ControlFile>) -> Self {
        match control {
            Some(control) => Self {
                last_checkpoint_lsn: control.last_checkpoint_lsn,
                checkpoint_no: control.checkpoint_no,
            },
            None => Self::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownTail {
    pub segment_id: SegmentId,
    pub shutdown_lsn: Lsn,
    pub next_lsn: Lsn,
    pub record_count: u64,
}

#[derive(Debug, Clone)]
struct TailScanResult {
    last_record: Option<TailRecord>,
    record_count: u64,
}

#[derive(Debug, Clone)]
struct TailRecord {
    lsn: Lsn,
    record_type: RecordType,
    payload_len: u32,
    next_lsn: Lsn,
    next_file_offset: u64,
}

pub(crate) fn publish_clean_shutdown(
    control_store: &FsControlFileStore,
    identity: WalIdentity,
    checkpoint: CheckpointState,
) -> Result<(), WalError> {
    let control = ControlFile::new(
        identity,
        checkpoint.last_checkpoint_lsn,
        checkpoint.checkpoint_no,
        true,
    );
    control_store.publish(&control)
}

pub(crate) fn clear_clean_shutdown(
    control_store: &FsControlFileStore,
    identity: WalIdentity,
    checkpoint: CheckpointState,
) -> Result<(), WalError> {
    let control = ControlFile::new(
        identity,
        checkpoint.last_checkpoint_lsn,
        checkpoint.checkpoint_no,
        false,
    );
    control_store.publish(&control)
}

pub(crate) fn find_shutdown_tail<D: SegmentDirectory>(
    directory: &D,
    expected_identity: WalIdentity,
    max_record_size: u32,
    record_alignment: u32,
) -> Result<Option<ShutdownTail>, WalError> {
    let metas = directory.list_segments()?;
    let Some(latest) = metas.last() else {
        return Ok(None);
    };

    let file = directory.open_segment(latest.segment_id)?;
    let header = match read_segment_header_or_none(&file)? {
        Some(header) => header,
        None => return Ok(None),
    };

    if header.segment_id != latest.segment_id || header.base_lsn != latest.base_lsn {
        return Ok(None);
    }

    if header.identity() != expected_identity {
        return Ok(None);
    }

    let descriptor = match SegmentDescriptor::from_header(&header, file.len()?) {
        Ok(descriptor) => descriptor,
        Err(err) if is_io_like(&err) => return Err(err),
        Err(_) => return Ok(None),
    };

    let scan = scan_last_record_in_segment(
        &file,
        &descriptor,
        &header,
        max_record_size,
        record_alignment,
    )?;

    let Some(last_record) = scan.last_record else {
        return Ok(None);
    };

    if last_record.record_type != record_types::SHUTDOWN || last_record.payload_len != 0 {
        return Ok(None);
    }

    Ok(Some(ShutdownTail {
        segment_id: descriptor.segment_id,
        shutdown_lsn: last_record.lsn,
        next_lsn: last_record.next_lsn,
        record_count: scan.record_count,
    }))
}

fn scan_last_record_in_segment<F: SegmentFile>(
    file: &F,
    descriptor: &SegmentDescriptor,
    segment_header: &SegmentHeader,
    max_record_size: u32,
    record_alignment: u32,
) -> Result<TailScanResult, WalError> {
    let mut file_offset = descriptor.header_len;
    let mut expected_lsn = descriptor.base_lsn;
    let mut last_record = None;
    let mut record_count = 0u64;

    while file_offset < descriptor.file_len {
        let record = match validate_record_at(
            file,
            descriptor.file_len,
            segment_header,
            file_offset,
            expected_lsn,
            max_record_size,
            record_alignment,
        ) {
            Ok(record) => record,
            Err(err) if is_io_like(&err) => return Err(err),
            Err(_) => {
                return Ok(TailScanResult {
                    last_record: None,
                    record_count: 0,
                });
            }
        };

        record_count = record_count
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        file_offset = record.next_file_offset;
        expected_lsn = record.next_lsn;
        last_record = Some(record);
    }

    Ok(TailScanResult {
        last_record,
        record_count,
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
) -> Result<TailRecord, WalError> {
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

    if header.is_compressed() && segment_header.compression_algorithm == 0 {
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

    Ok(TailRecord {
        lsn: header.lsn,
        record_type: header.record_type,
        payload_len: header.payload_len,
        next_lsn,
        next_file_offset,
    })
}

fn read_segment_header_or_none<F: SegmentFile>(
    file: &F,
) -> Result<Option<SegmentHeader>, WalError> {
    let mut bytes = [0u8; SegmentHeader::ENCODED_LEN];
    match read_exact_at(file, 0, &mut bytes) {
        Ok(()) => {}
        Err(err) if is_io_like(&err) => return Err(err),
        Err(_) => return Ok(None),
    }

    match SegmentHeader::decode(&bytes) {
        Ok(header) => Ok(Some(header)),
        Err(err) if is_io_like(&err) => Err(err),
        Err(_) => Ok(None),
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

fn is_io_like(err: &WalError) -> bool {
    matches!(err, WalError::Io(_) | WalError::FatalIo { .. })
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
        config::SEGMENT_HEADER_LEN,
        format::segment_header::compression_algorithms,
        io::{
            directory::{FsSegmentDirectory, NewSegment, SegmentDirectory},
            segment_file::SegmentFile,
        },
        types::{record_flags, record_types},
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
                .join(format!("wal-shutdown-{prefix}-{}-{nanos}", process::id()));

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
        encoded_records: &[Vec<u8>],
    ) {
        let mut header = SegmentHeader::new(
            sample_identity(),
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

        for record in encoded_records {
            file.append_all(record).unwrap();
        }

        file.sync().unwrap();
    }

    fn corrupt_file_byte(path: &Path, offset: usize) {
        let mut bytes = fs::read(path).unwrap();
        bytes[offset] ^= 0xFF;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn publish_and_clear_clean_shutdown_round_trip_control_flag() {
        let test_dir = TestDir::new("control");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let checkpoint = CheckpointState {
            last_checkpoint_lsn: Some(Lsn::new(4096)),
            checkpoint_no: 7,
        };

        publish_clean_shutdown(&store, sample_identity(), checkpoint).unwrap();

        let control = store.read().unwrap().unwrap();
        assert!(control.clean_shutdown);
        assert_eq!(control.last_checkpoint_lsn, checkpoint.last_checkpoint_lsn);
        assert_eq!(control.checkpoint_no, checkpoint.checkpoint_no);

        clear_clean_shutdown(&store, sample_identity(), checkpoint).unwrap();

        let control = store.read().unwrap().unwrap();
        assert!(!control.clean_shutdown);
        assert_eq!(control.last_checkpoint_lsn, checkpoint.last_checkpoint_lsn);
        assert_eq!(control.checkpoint_no, checkpoint.checkpoint_no);
    }

    #[test]
    fn checkpoint_state_can_be_extracted_from_control_file() {
        let checkpoint = CheckpointState {
            last_checkpoint_lsn: Some(Lsn::new(1234)),
            checkpoint_no: 9,
        };
        let control = ControlFile::new(
            sample_identity(),
            checkpoint.last_checkpoint_lsn,
            checkpoint.checkpoint_no,
            true,
        );

        assert_eq!(CheckpointState::from_control(Some(&control)), checkpoint);
        assert_eq!(
            CheckpointState::from_control(None),
            CheckpointState::default()
        );
    }

    #[test]
    fn find_shutdown_tail_returns_none_for_empty_directory() {
        let test_dir = TestDir::new("empty");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let tail = find_shutdown_tail(&directory, sample_identity(), 1024, 0).unwrap();

        assert_eq!(tail, None);
    }

    #[test]
    fn find_shutdown_tail_detects_valid_shutdown_record_at_tail() {
        let test_dir = TestDir::new("valid-tail");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let user_record = encode_record(
            RecordType::new(record_types::USER_MIN),
            record_flags::NONE,
            b"hello",
            Lsn::ZERO,
            0,
        );
        let shutdown_lsn = Lsn::new(user_record.len() as u64);
        let shutdown_record = encode_record(
            record_types::SHUTDOWN,
            record_flags::NONE,
            &[],
            shutdown_lsn,
            0,
        );

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            &[user_record.clone(), shutdown_record],
        );

        let tail = find_shutdown_tail(&directory, sample_identity(), 1024, 0)
            .unwrap()
            .unwrap();

        assert_eq!(tail.segment_id, 1);
        assert_eq!(tail.shutdown_lsn, shutdown_lsn);
        assert_eq!(tail.next_lsn, Lsn::new((user_record.len() + 32) as u64));
        assert_eq!(tail.record_count, 2);
    }

    #[test]
    fn find_shutdown_tail_returns_none_when_last_record_is_not_shutdown() {
        let test_dir = TestDir::new("not-shutdown");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let user_record = encode_record(
            RecordType::new(record_types::USER_MIN),
            record_flags::NONE,
            b"hello",
            Lsn::ZERO,
            0,
        );

        create_segment(&directory, 1, Lsn::ZERO, &[user_record]);

        let tail = find_shutdown_tail(&directory, sample_identity(), 1024, 0).unwrap();

        assert_eq!(tail, None);
    }

    #[test]
    fn find_shutdown_tail_returns_none_for_corrupt_tail_record() {
        let test_dir = TestDir::new("corrupt-tail");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let user_record = encode_record(
            RecordType::new(record_types::USER_MIN),
            record_flags::NONE,
            b"hello",
            Lsn::ZERO,
            0,
        );
        let shutdown_lsn = Lsn::new(user_record.len() as u64);
        let shutdown_record = encode_record(
            record_types::SHUTDOWN,
            record_flags::NONE,
            &[],
            shutdown_lsn,
            0,
        );

        create_segment(
            &directory,
            1,
            Lsn::ZERO,
            &[user_record.clone(), shutdown_record],
        );

        let segment_path = directory
            .list_segments()
            .unwrap()
            .into_iter()
            .find(|meta| meta.segment_id == 1)
            .unwrap()
            .path;

        let shutdown_header_offset = SEGMENT_HEADER_LEN as usize + user_record.len();
        corrupt_file_byte(&segment_path, shutdown_header_offset + 1);

        let tail = find_shutdown_tail(&directory, sample_identity(), 1024, 0).unwrap();
        assert_eq!(tail, None);
    }
}
