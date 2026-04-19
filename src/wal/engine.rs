use crate::{
    config::{CompressionPolicy, RECORD_HEADER_LEN, SEGMENT_HEADER_LEN, SyncPolicy, WalConfig},
    error::WalError,
    format::{
        codec::put_u64_le,
        record_header::RecordHeader,
        segment_header::{SegmentHeader, compression_algorithms},
    },
    io::{
        buffer::{AppendBuffer, AppendBufferError},
        directory::{NewSegment, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, SegmentId, record_flags, record_types},
    wal::segment::ActiveSegment,
};

pub struct Wal<D, C>
where
    D: SegmentDirectory,
{
    directory: D,
    config: WalConfig,
    active_segment: Option<ActiveSegment<D::File>>,
    write_buffer: AppendBuffer,
    next_lsn: Lsn,
    durable_lsn: Lsn,
    first_lsn: Option<Lsn>,
    current_wal_size: u64,
    active_segment_record_count: u64,
    _checksummer: C,
}

pub struct SegmentSealPayload {
    pub segment_id: SegmentId,
    pub record_count: u64,
    pub logical_bytes: u64,
}

impl SegmentSealPayload {
    pub const ENCODED_LEN: usize = 24;

    pub fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);
        put_u64_le(&mut buf, self.segment_id);
        put_u64_le(&mut buf, self.record_count);
        put_u64_le(&mut buf, self.logical_bytes);
        buf
    }
}

impl<D, C> Wal<D, C>
where
    D: SegmentDirectory,
{
    pub fn open(directory: D, config: WalConfig, checksummer: C) -> Result<Self, WalError> {
        config.validate()?;
        Self::validate_constraints(&config)?;

        let mut wal = Self {
            directory,
            config: config.clone(),
            active_segment: None,
            write_buffer: AppendBuffer::new(config.write_buffer_size),
            next_lsn: Lsn::ZERO,
            durable_lsn: Lsn::ZERO,
            first_lsn: None,
            current_wal_size: 0,
            active_segment_record_count: 0,
            _checksummer: checksummer,
        };

        wal.initialize_from_directory()?;
        Ok(wal)
    }

    pub fn append(&mut self, record_type: RecordType, payload: &[u8]) -> Result<Lsn, WalError> {
        self.ensure_mutable()?;

        if payload.len() > self.config.max_record_size as usize {
            return Err(WalError::PayloadTooLarge {
                len: payload.len() as u32,
                max: self.config.max_record_size,
            });
        }

        self.ensure_writable_segment()?;

        let (on_disk_payload, flags) = self.prepare_payload_for_append(payload)?;
        let lsn = self.stage_record(record_type, flags, &on_disk_payload, true)?;

        self.active_segment_record_count = self
            .active_segment_record_count
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn);
        }

        if matches!(self.config.sync_policy, SyncPolicy::Always) {
            self.sync()?;
        }

        Ok(lsn)
    }

    pub fn flush(&mut self) -> Result<(), WalError> {
        self.ensure_mutable()?;

        if self.active_segment.is_none() {
            return Ok(());
        }

        self.drain_buffer(false)?;

        if let Some(segment) = self.active_segment.as_mut() {
            segment.file_mut().flush()?;
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.ensure_mutable()?;
        self.flush()?;

        if let Some(segment) = self.active_segment.as_mut() {
            segment.file_mut().sync()?;
        }

        self.durable_lsn = self.next_lsn;
        Ok(())
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.durable_lsn
    }

    pub fn first_lsn(&self) -> Option<Lsn> {
        self.first_lsn
    }

    pub fn current_wal_size(&self) -> u64 {
        self.current_wal_size
    }

    pub fn buffered_bytes(&self) -> usize {
        self.write_buffer.len()
    }

    pub fn active_segment_id(&self) -> Option<SegmentId> {
        self.active_segment
            .as_ref()
            .map(|segment| segment.segment_id())
    }

    fn initialize_from_directory(&mut self) -> Result<(), WalError> {
        let segments = self.directory.list_segments()?;
        if segments.is_empty() {
            return Ok(());
        }

        let mut previous_base_lsn = None;
        let mut latest_open: Option<(D::File, SegmentHeader)> = None;

        for meta in segments {
            let file = self.directory.open_segment(meta.segment_id)?;
            let header = read_segment_header(&file)?;

            if header.segment_id != meta.segment_id || header.base_lsn != meta.base_lsn {
                return Err(WalError::FilenameHeaderMismatch);
            }

            if header.identity() != self.config.identity {
                return Err(WalError::IdentityMismatch {
                    expected: self.config.identity,
                    found: header.identity(),
                });
            }

            if let Some(previous) = previous_base_lsn {
                if header.base_lsn <= previous {
                    return Err(WalError::SegmentOrderingViolation);
                }
            }

            let file_len = file.len()?;
            self.current_wal_size = self
                .current_wal_size
                .checked_add(file_len)
                .ok_or(WalError::ReservationOverflow)?;

            if self.first_lsn.is_none() && file_len > u64::from(header.header_len) {
                self.first_lsn = Some(header.base_lsn);
            }

            previous_base_lsn = Some(header.base_lsn);
            latest_open = Some((file, header));
        }

        if let Some((file, header)) = latest_open {
            let active_segment = ActiveSegment::open(file, header)?;
            self.next_lsn = active_segment.written_end_lsn()?;
            self.durable_lsn = self.next_lsn;
            self.active_segment = Some(active_segment);
        }

        Ok(())
    }

    fn ensure_writable_segment(&mut self) -> Result<(), WalError> {
        if self.active_segment.is_some() {
            return Ok(());
        }

        let segment = self.create_segment(1, self.next_lsn)?;
        self.active_segment = Some(segment);
        self.active_segment_record_count = 0;
        Ok(())
    }

    fn create_segment(
        &mut self,
        segment_id: SegmentId,
        base_lsn: Lsn,
    ) -> Result<ActiveSegment<D::File>, WalError> {
        let mut header = SegmentHeader::new(
            self.config.identity,
            segment_id,
            base_lsn,
            self.segment_compression_algorithm(),
            self.config.format_version,
        );
        header.finalize_checksum();

        let file = self.directory.create_segment(NewSegment {
            segment_id,
            base_lsn,
            header: header.clone(),
        })?;

        let active_segment = ActiveSegment::open(file, header)?;
        self.current_wal_size = self
            .current_wal_size
            .checked_add(active_segment.file_len())
            .ok_or(WalError::ReservationOverflow)?;

        Ok(active_segment)
    }

    fn stage_record(
        &mut self,
        record_type: RecordType,
        flags: u16,
        payload: &[u8],
        reserve_future_seal_space: bool,
    ) -> Result<Lsn, WalError> {
        self.ensure_writable_segment()?;

        let record_len = self.record_physical_len_for_payload_len(payload.len())?;

        if self.write_buffer.len() + record_len > self.config.write_buffer_size {
            self.flush()?;
        }

        if reserve_future_seal_space && !self.active_segment_can_fit(record_len as u64, true)? {
            if self.active_segment_is_empty() {
                return Err(WalError::invalid_config(
                    "target_segment_size must leave room for one maximum sized record plus a trailing SEGMENT_SEAL record",
                ));
            }

            self.rollover()?;
        }

        if !self.active_segment_can_fit(record_len as u64, reserve_future_seal_space)? {
            return Err(WalError::invalid_config(
                "active segment cannot fit staged record",
            ));
        }

        let lsn = self.next_lsn;
        let encoded_record = self.encode_record_bytes(record_type, flags, payload, lsn)?;

        self.write_buffer
            .append(&encoded_record)
            .map_err(buffer_error_to_wal)?;

        self.next_lsn = self
            .next_lsn
            .checked_add_bytes(encoded_record.len() as u64)
            .ok_or(WalError::ReservationOverflow)?;

        self.drain_buffer(true)?;
        Ok(lsn)
    }

    fn rollover(&mut self) -> Result<(), WalError> {
        let seal_payload = self.build_segment_seal_payload()?.encode();
        let current_segment_id = self
            .active_segment
            .as_ref()
            .ok_or(WalError::BrokenDurabilityContract)?
            .segment_id();

        self.stage_record(
            record_types::SEGMENT_SEAL,
            record_flags::NONE,
            &seal_payload,
            false,
        )?;

        self.flush()?;
        self.sync()?;

        if let Some(segment) = self.active_segment.as_mut() {
            segment.mark_sealed();
        }

        let next_segment_id = current_segment_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;
        let next_base_lsn = self.next_lsn;

        let new_segment = self.create_segment(next_segment_id, next_base_lsn)?;
        self.active_segment = Some(new_segment);
        self.active_segment_record_count = 0;

        Ok(())
    }

    fn build_segment_seal_payload(&self) -> Result<SegmentSealPayload, WalError> {
        let active_segment = self
            .active_segment
            .as_ref()
            .ok_or(WalError::BrokenDurabilityContract)?;

        let buffered_logical_bytes = self.write_buffer.len() as u64;
        let logical_bytes = active_segment
            .written_logical_len()
            .checked_add(buffered_logical_bytes)
            .ok_or(WalError::ReservationOverflow)?;

        Ok(SegmentSealPayload {
            segment_id: active_segment.segment_id(),
            record_count: self.active_segment_record_count,
            logical_bytes,
        })
    }

    fn encode_record_bytes(
        &self,
        record_type: RecordType,
        flags: u16,
        payload: &[u8],
        lsn: Lsn,
    ) -> Result<Vec<u8>, WalError> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| WalError::PayloadTooLarge {
            len: u32::MAX,
            max: self.config.max_record_size,
        })?;

        let mut header = RecordHeader::new(
            record_type,
            flags,
            payload_len,
            lsn,
            self.config.format_version,
        );
        header.finalize_checksum(payload)?;

        let mut encoded =
            Vec::with_capacity(self.record_physical_len_for_payload_len(payload.len())?);
        encoded.extend_from_slice(&header.encode());
        encoded.extend_from_slice(payload);

        let padding_len = self.padding_len(encoded.len())?;
        if padding_len > 0 {
            encoded.resize(encoded.len() + padding_len, 0);
        }

        Ok(encoded)
    }

    fn drain_buffer(&mut self, steady_state: bool) -> Result<(), WalError> {
        if self.active_segment.is_none() {
            return Ok(());
        }

        loop {
            let chunk = if steady_state {
                self.write_buffer
                    .drain_preferred_chunk(self.config.storage_write_unit as usize)
            } else {
                self.write_buffer.drain_all()
            };

            if chunk.is_empty() {
                break;
            }

            let chunk_len = chunk.len() as u64;

            {
                let active_segment = self
                    .active_segment
                    .as_mut()
                    .ok_or(WalError::BrokenDurabilityContract)?;
                active_segment.file_mut().append_all(&chunk)?;
                active_segment.note_bytes_written(chunk_len)?;
            }

            self.current_wal_size = self
                .current_wal_size
                .checked_add(chunk_len)
                .ok_or(WalError::ReservationOverflow)?;

            if !steady_state {
                break;
            }
        }

        Ok(())
    }

    fn active_segment_can_fit(
        &self,
        additional_bytes: u64,
        reserve_future_seal_space: bool,
    ) -> Result<bool, WalError> {
        let active_segment = self
            .active_segment
            .as_ref()
            .ok_or(WalError::BrokenDurabilityContract)?;

        let mut required = additional_bytes;
        if reserve_future_seal_space {
            required = required
                .checked_add(self.seal_record_physical_len()? as u64)
                .ok_or(WalError::ReservationOverflow)?;
        }

        let buffered_len = self.write_buffer.len() as u64;
        let used = active_segment
            .file_len()
            .checked_add(buffered_len)
            .ok_or(WalError::ReservationOverflow)?;

        let final_len = used
            .checked_add(required)
            .ok_or(WalError::ReservationOverflow)?;

        Ok(final_len <= self.config.target_segment_size)
    }

    fn active_segment_is_empty(&self) -> bool {
        match self.active_segment.as_ref() {
            Some(segment) => {
                segment.file_len() == segment.header_len() && self.write_buffer.is_empty()
            }
            None => true,
        }
    }

    fn prepare_payload_for_append(&self, payload: &[u8]) -> Result<(Vec<u8>, u16), WalError> {
        Ok((payload.to_vec(), record_flags::NONE))
    }

    fn record_physical_len_for_payload_len(&self, payload_len: usize) -> Result<usize, WalError> {
        let logical_len = RECORD_HEADER_LEN
            .checked_add(payload_len)
            .ok_or(WalError::ReservationOverflow)?;
        let padding_len = self.padding_len(logical_len)?;

        logical_len
            .checked_add(padding_len)
            .ok_or(WalError::ReservationOverflow)
    }

    fn seal_record_physical_len(&self) -> Result<usize, WalError> {
        self.record_physical_len_for_payload_len(SegmentSealPayload::ENCODED_LEN)
    }

    fn padding_len(&self, total_len: usize) -> Result<usize, WalError> {
        let alignment = self.config.record_alignment as usize;
        if alignment == 0 {
            return Ok(0);
        }

        let remainder = total_len % alignment;
        if remainder == 0 {
            Ok(0)
        } else {
            alignment
                .checked_sub(remainder)
                .ok_or(WalError::ReservationOverflow)
        }
    }

    fn segment_compression_algorithm(&self) -> u8 {
        match self.config.compression_policy {
            CompressionPolicy::None => compression_algorithms::NONE,
            CompressionPolicy::Lz4 => compression_algorithms::LZ4,
            CompressionPolicy::Zstd { .. } => compression_algorithms::ZSTD,
        }
    }

    fn ensure_mutable(&self) -> Result<(), WalError> {
        if self.config.read_only {
            return Err(WalError::ReadOnlyViolation);
        }

        Ok(())
    }

    fn validate_constraints(config: &WalConfig) -> Result<(), WalError> {
        let max_record_physical_len = physical_record_len_for_alignment(
            config.record_alignment,
            config.max_record_size as usize,
        )?;
        let seal_record_physical_len = physical_record_len_for_alignment(
            config.record_alignment,
            SegmentSealPayload::ENCODED_LEN,
        )?;

        if config.write_buffer_size < max_record_physical_len {
            return Err(WalError::invalid_config(
                "write_buffer_size must be large enough for one maximum sized physical record including padding",
            ));
        }

        let required_segment_size = SEGMENT_HEADER_LEN
            .checked_add(max_record_physical_len as u64)
            .and_then(|value| value.checked_add(seal_record_physical_len as u64))
            .ok_or_else(|| WalError::invalid_config("segment size calculation overflowed"))?;

        if required_segment_size > config.target_segment_size {
            return Err(WalError::invalid_config(
                "target_segment_size must be large enough for one maximum-sized record plus one SEGMENT_SEAL record",
            ));
        }

        Ok(())
    }
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

fn physical_record_len_for_alignment(
    record_alignment: u32,
    payload_len: usize,
) -> Result<usize, WalError> {
    let logical_len = RECORD_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(WalError::ReservationOverflow)?;

    if record_alignment == 0 {
        return Ok(logical_len);
    }

    let alignment = record_alignment as usize;
    let remainder = logical_len % alignment;
    let padding = if remainder == 0 {
        0
    } else {
        alignment - remainder
    };

    logical_len
        .checked_add(padding)
        .ok_or(WalError::ReservationOverflow)
}

fn buffer_error_to_wal(_: AppendBufferError) -> WalError {
    WalError::ReservationOverflow
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
        format::record_header::RecordHeader,
        io::{
            directory::{FsSegmentDirectory, SegmentDirectory},
            segment_file::SegmentFile,
        },
        types::{WalIdentity, record_types},
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
                std::env::temp_dir().join(format!("wal-engine-{prefix}-{}-{nanos}", process::id()));

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

    fn open_test_wal(test_dir: &TestDir) -> Wal<FsSegmentDirectory, ()> {
        Wal::open(
            FsSegmentDirectory::new(test_dir.path().to_path_buf()),
            test_dir.config(),
            (),
        )
        .expect("failed to open test wal")
    }

    #[test]
    fn open_empty_directory_starts_with_zero_lsn_and_no_active_segment() {
        let test_dir = TestDir::new("open-empty");
        let wal = open_test_wal(&test_dir);

        assert_eq!(wal.next_lsn(), Lsn::ZERO);
        assert_eq!(wal.durable_lsn(), Lsn::ZERO);
        assert_eq!(wal.first_lsn(), None);
        assert_eq!(wal.current_wal_size(), 0);
        assert_eq!(wal.active_segment_id(), None);
        assert_eq!(wal.buffered_bytes(), 0);
    }

    #[test]
    fn append_creates_first_segment_and_assigns_lsn_zero() {
        let test_dir = TestDir::new("append-first");
        let mut wal = open_test_wal(&test_dir);

        let lsn = wal
            .append(RecordType::new(record_types::USER_MIN), b"hello")
            .unwrap();

        assert_eq!(lsn, Lsn::ZERO);
        assert_eq!(wal.first_lsn(), Some(Lsn::ZERO));
        assert_eq!(wal.next_lsn(), Lsn::new(37));
        assert_eq!(wal.durable_lsn(), Lsn::ZERO);
        assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN);
        assert_eq!(wal.active_segment_id(), Some(1));
        assert_eq!(wal.buffered_bytes(), 37);
    }

    #[test]
    fn flush_drains_tail_bytes_to_file_without_advancing_durable_frontier() {
        let test_dir = TestDir::new("flush");
        let mut wal = open_test_wal(&test_dir);

        wal.append(RecordType::new(record_types::USER_MIN), b"hello")
            .unwrap();
        wal.flush().unwrap();

        assert_eq!(wal.buffered_bytes(), 0);
        assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 37);
        assert_eq!(wal.durable_lsn(), Lsn::ZERO);

        let active_segment = wal.active_segment.as_ref().unwrap();
        assert_eq!(active_segment.file_len(), SEGMENT_HEADER_LEN + 37);
    }

    #[test]
    fn sync_advances_durable_frontier_to_next_lsn() {
        let test_dir = TestDir::new("sync");
        let mut wal = open_test_wal(&test_dir);

        wal.append(RecordType::new(record_types::USER_MIN), b"hello")
            .unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.buffered_bytes(), 0);
        assert_eq!(wal.next_lsn(), Lsn::new(37));
        assert_eq!(wal.durable_lsn(), Lsn::new(37));
    }

    #[test]
    fn steady_state_draining_prefers_storage_write_unit_multiples() {
        let test_dir = TestDir::new("steady-drain");
        let mut config = test_dir.config();
        config.storage_write_unit = 512;
        config.write_buffer_size = 2048;

        let mut wal = Wal::open(
            FsSegmentDirectory::new(test_dir.path().to_path_buf()),
            config,
            (),
        )
        .unwrap();

        wal.append(RecordType::new(record_types::USER_MIN), &[7u8; 600])
            .unwrap();

        assert_eq!(wal.current_wal_size(), SEGMENT_HEADER_LEN + 512);
        assert_eq!(wal.buffered_bytes(), 120);

        let active_segment = wal.active_segment.as_ref().unwrap();
        assert_eq!(active_segment.file_len(), SEGMENT_HEADER_LEN + 512);
        assert_eq!(wal.next_lsn(), Lsn::new(632));
    }

    #[test]
    fn rollover_appends_segment_seal_and_switches_to_next_segment() {
        let test_dir = TestDir::new("rollover");
        let mut config = test_dir.config();
        config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

        let mut wal = Wal::open(
            FsSegmentDirectory::new(test_dir.path().to_path_buf()),
            config,
            (),
        )
        .unwrap();

        let first_lsn = wal
            .append(RecordType::new(record_types::USER_MIN), &[1u8; 16])
            .unwrap();
        let second_lsn = wal
            .append(RecordType::new(record_types::USER_MIN), &[2u8; 16])
            .unwrap();

        assert_eq!(first_lsn, Lsn::ZERO);
        assert_eq!(second_lsn, Lsn::new(104));
        assert_eq!(wal.active_segment_id(), Some(2));

        let segments = wal.directory.list_segments().unwrap();
        assert_eq!(segments.len(), 2);

        let first_segment = wal.directory.open_segment(1).unwrap();
        let first_len = first_segment.len().unwrap();
        let seal_offset = first_len - 56;

        let mut header_bytes = [0u8; RecordHeader::ENCODED_LEN];
        read_exact_at(&first_segment, seal_offset, &mut header_bytes).unwrap();

        let seal_header = RecordHeader::decode(&header_bytes).unwrap();
        assert_eq!(seal_header.record_type, record_types::SEGMENT_SEAL);
        assert_eq!(seal_header.lsn, Lsn::new(48));
    }

    #[test]
    fn reopen_restores_latest_next_lsn_from_existing_segment_lengths() {
        let test_dir = TestDir::new("reopen");

        {
            let mut wal = open_test_wal(&test_dir);
            wal.append(RecordType::new(record_types::USER_MIN), b"hello")
                .unwrap();
            wal.sync().unwrap();
            assert_eq!(wal.next_lsn(), Lsn::new(37));
        }

        let reopened = Wal::open(
            FsSegmentDirectory::new(test_dir.path().to_path_buf()),
            test_dir.config(),
            (),
        )
        .unwrap();

        assert_eq!(reopened.active_segment_id(), Some(1));
        assert_eq!(reopened.first_lsn(), Some(Lsn::ZERO));
        assert_eq!(reopened.next_lsn(), Lsn::new(37));
        assert_eq!(reopened.durable_lsn(), Lsn::new(37));
    }
}
