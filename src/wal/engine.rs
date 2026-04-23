use std::{io, time::Instant};

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
        control_file::{ControlFile, FsControlFileStore},
        directory::{NewSegment, SegmentDirectory},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::{RecordType, SegmentId, WalIdentity, record_flags, record_types},
    wal::{
        iterator::{
            SnapshotSegment, WalIterator, WalRecord, read_record_at_snapshot, snapshot_segments,
            snapshot_segments_through,
        },
        metrics::WalMetrics,
        recovery::{RecoveredWal, recover_with_observer},
        recovery_observer::RecoveryObserver,
        report::RecoveryReport,
        retention::{RetentionState, execute_truncate_plan, plan_truncate_segments_before},
        retention_pin::RetentionPinGuard,
        segment::ActiveSegment,
        shutdown::{
            CheckpointState, clear_clean_shutdown, find_shutdown_tail, publish_clean_shutdown,
        },
    },
};

pub struct Wal<D, C>
where
    D: SegmentDirectory,
{
    directory: D,
    control_store: FsControlFileStore,
    config: WalConfig,
    active_segment: Option<ActiveSegment<D::File>>,
    write_buffer: AppendBuffer,
    next_lsn: Lsn,
    durable_lsn: Lsn,
    first_lsn: Option<Lsn>,
    current_wal_size: u64,
    next_segment_id: SegmentId,
    active_segment_record_count: u64,
    checkpoint_state: CheckpointState,
    retention_state: RetentionState,
    metrics: WalMetrics,
    shutdown_in_progress: bool,
    fatal_state: Option<StickyFatalState>,
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

#[derive(Debug, Clone, Copy)]
struct StickyFatalState {
    operation: &'static str,
    safe_lsn: Lsn,
}

impl<D, C> Wal<D, C>
where
    D: SegmentDirectory,
{
    pub fn open(
        directory: D,
        config: WalConfig,
        checksummer: C,
    ) -> Result<(Self, RecoveryReport), WalError> {
        Self::open_internal(directory, config, checksummer, None)
    }

    pub fn open_with_observer(
        directory: D,
        config: WalConfig,
        checksummer: C,
        observer: &dyn RecoveryObserver,
    ) -> Result<(Self, RecoveryReport), WalError> {
        Self::open_internal(directory, config, checksummer, Some(observer))
    }

    fn open_internal(
        directory: D,
        config: WalConfig,
        checksummer: C,
        observer: Option<&dyn RecoveryObserver>,
    ) -> Result<(Self, RecoveryReport), WalError> {
        config.validate()?;
        Self::validate_constraints(&config)?;

        let control_store = FsControlFileStore::new(config.dir.clone());
        let prior_control = control_store.load_for_recovery(config.identity)?;
        let write_buffer = AppendBuffer::new(config.write_buffer_size);

        let recovered = match prior_control
            .as_ref()
            .filter(|control| control.clean_shutdown)
        {
            Some(control) => {
                match Self::try_open_clean_shutdown_fast_path(&directory, &config, control)? {
                    Some(recovered) => recovered,
                    None => recover_with_observer(&directory, &control_store, &config, observer)?,
                }
            }
            None => recover_with_observer(&directory, &control_store, &config, observer)?,
        };

        let RecoveredWal {
            active_segment,
            first_lsn,
            next_lsn,
            durable_lsn,
            current_wal_size,
            next_segment_id,
            active_segment_record_count,
            report,
        } = recovered;

        let checkpoint_state = CheckpointState {
            last_checkpoint_lsn: report.checkpoint_lsn,
            checkpoint_no: prior_control
                .as_ref()
                .map(|control| control.checkpoint_no)
                .unwrap_or(0),
        };

        if !config.read_only
            && prior_control
                .as_ref()
                .is_some_and(|control| control.clean_shutdown)
        {
            clear_clean_shutdown(&control_store, config.identity, checkpoint_state)?;
        }

        let mut metrics = WalMetrics::new(current_wal_size);
        metrics.note_recovery(&report);

        let wal = Self {
            directory,
            control_store,
            config,
            active_segment,
            write_buffer,
            next_lsn,
            durable_lsn,
            first_lsn,
            current_wal_size,
            next_segment_id,
            active_segment_record_count,
            checkpoint_state,
            retention_state: RetentionState::default(),
            metrics,
            shutdown_in_progress: false,
            fatal_state: None,
            _checksummer: checksummer,
        };

        Ok((wal, report))
    }

    fn try_open_clean_shutdown_fast_path(
        directory: &D,
        config: &WalConfig,
        control: &ControlFile,
    ) -> Result<Option<RecoveredWal<D::File>>, WalError> {
        let started = Instant::now();

        let Some(shutdown_tail) = find_shutdown_tail(
            directory,
            config.identity,
            config.max_record_size,
            config.record_alignment,
        )?
        else {
            return Ok(None);
        };

        let metas = directory.list_segments()?;
        let Some(latest_meta) = metas.last() else {
            return Ok(None);
        };

        if latest_meta.segment_id != shutdown_tail.segment_id {
            return Ok(None);
        }

        let mut current_wal_size = 0u64;
        let mut first_lsn = None;

        for meta in &metas {
            let file = directory.open_segment(meta.segment_id)?;
            let file_len = file.len()?;

            current_wal_size = current_wal_size
                .checked_add(file_len)
                .ok_or(WalError::ReservationOverflow)?;

            if first_lsn.is_none() && file_len > SEGMENT_HEADER_LEN {
                first_lsn = Some(meta.base_lsn);
            }
        }

        let latest_file = directory.open_segment(latest_meta.segment_id)?;
        let latest_header = match read_segment_header(&latest_file) {
            Ok(header) => header,
            Err(err) if is_io_like(&err) => return Err(err),
            Err(_) => return Ok(None),
        };

        if latest_header.segment_id != latest_meta.segment_id
            || latest_header.base_lsn != latest_meta.base_lsn
        {
            return Ok(None);
        }

        if latest_header.identity() != config.identity {
            return Ok(None);
        }

        let active_segment = match ActiveSegment::open(latest_file, latest_header) {
            Ok(segment) => segment,
            Err(err) if is_io_like(&err) => return Err(err),
            Err(_) => return Ok(None),
        };

        let next_segment_id = latest_meta
            .segment_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;
        let next_lsn = shutdown_tail.next_lsn;

        let mut report = RecoveryReport::empty();
        report.segments_scanned = 1;
        report.records_scanned = shutdown_tail.record_count;
        report.first_lsn = first_lsn;
        report.last_valid_lsn = Some(shutdown_tail.shutdown_lsn);
        report.set_next_lsn(next_lsn);
        report.set_checkpoint_lsn(control.last_checkpoint_lsn);
        report.set_segments_prunable(0);
        report.set_recovery_duration(started.elapsed());
        report.mark_clean_shutdown(true);
        report.mark_recovery_skipped(true);

        Ok(Some(RecoveredWal {
            active_segment: Some(active_segment),
            first_lsn,
            next_lsn,
            durable_lsn: next_lsn,
            current_wal_size,
            next_segment_id,
            active_segment_record_count: shutdown_tail.record_count,
            report,
        }))
    }

    pub fn append(&mut self, record_type: RecordType, payload: &[u8]) -> Result<Lsn, WalError> {
        self.ensure_operational()?;

        if payload.len() > self.config.max_record_size as usize {
            return Err(WalError::PayloadTooLarge {
                len: payload.len() as u32,
                max: self.config.max_record_size,
            });
        }

        let original_payload_len = payload.len() as u64;
        let (on_disk_payload, flags) = self.prepare_payload_for_append(payload)?;
        let record_bytes = self.record_physical_len_for_payload_len(on_disk_payload.len())? as u64;

        self.ensure_record_admission_within_wal_size_limit(record_bytes, true)?;

        let lsn = self.stage_record(record_type, flags, &on_disk_payload, true)?;

        self.active_segment_record_count = self
            .active_segment_record_count
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn);
        }

        self.metrics.note_record_append(record_bytes);

        if flags & record_flags::COMPRESSED != 0 {
            self.metrics
                .note_compression(original_payload_len, on_disk_payload.len() as u64);
        }

        if matches!(self.config.sync_policy, SyncPolicy::Always) {
            self.sync()?;
        }

        Ok(lsn)
    }

    pub fn flush(&mut self) -> Result<(), WalError> {
        self.ensure_operational()?;
        self.flush_inner()
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.ensure_operational()?;
        self.sync_inner()
    }

    pub fn shutdown(&mut self) -> Result<(), WalError> {
        self.ensure_mutable()?;
        self.ensure_not_fatal()?;

        if self.shutdown_in_progress {
            return Err(WalError::ShutdownInProgress);
        }

        self.shutdown_in_progress = true;

        self.flush_inner()?;
        self.sync_inner()?;
        self.ensure_writable_segment()?;

        let shutdown_bytes = self.record_physical_len_for_payload_len(0)? as u64;
        if !self.active_segment_can_fit(shutdown_bytes, false)? {
            if self.active_segment_is_empty() {
                return Err(WalError::invalid_config(
                    "active segment cannot fit SHUTDOWN record",
                ));
            }

            self.rollover()?;
        }

        let shutdown_lsn =
            self.stage_record(record_types::SHUTDOWN, record_flags::NONE, &[], false)?;

        self.active_segment_record_count = self
            .active_segment_record_count
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        if self.first_lsn.is_none() {
            self.first_lsn = Some(shutdown_lsn);
        }

        self.metrics.note_record_append(shutdown_bytes);

        self.flush_inner()?;
        self.sync_inner()?;

        if let Err(err) = publish_clean_shutdown(
            &self.control_store,
            self.config.identity,
            self.checkpoint_state,
        ) {
            return Err(self.promote_mutating_error("publish clean shutdown control file", err));
        }

        self.metrics.note_clean_shutdown();

        Ok(())
    }

    pub fn metrics(&self) -> WalMetrics {
        let mut metrics = self.metrics.clone();
        metrics.set_retention_pins_active(self.retention_state.active_pin_count());
        metrics
    }

    pub fn set_min_retention_lsn(&mut self, lsn: Lsn) -> Result<(), WalError> {
        self.ensure_operational()?;
        self.retention_state.set_min_retention_lsn(lsn);
        Ok(())
    }

    pub fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<RetentionPinGuard, WalError> {
        self.ensure_operational()?;
        self.retention_state.acquire_pin(holder_name, min_lsn)
    }

    pub fn truncate_segments_before(&mut self, lsn: Lsn) -> Result<usize, WalError> {
        self.ensure_operational()?;

        let floor_lsn = self.retention_state.requested_prune_floor(lsn);
        let plan = match plan_truncate_segments_before(
            &self.directory,
            self.config.identity,
            self.active_segment_id(),
            floor_lsn,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                return Err(self.promote_mutating_error("plan wal retention truncation", err));
            }
        };

        if plan.removable_segment_ids.is_empty() {
            return Ok(0);
        }

        let removed = match execute_truncate_plan(&self.directory, &plan) {
            Ok(removed) => removed,
            Err(err) => return Err(self.promote_mutating_error("truncate wal segments", err)),
        };

        self.current_wal_size = self
            .current_wal_size
            .checked_sub(plan.removed_bytes)
            .ok_or(WalError::BrokenDurabilityContract)?;
        self.metrics.set_current_wal_size(self.current_wal_size);
        self.first_lsn = self.recompute_first_lsn_after_prune(plan.first_remaining_lsn);

        Ok(removed)
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

    pub(crate) fn cloned_directory(&self) -> D
    where
        D: Clone,
    {
        self.directory.clone()
    }

    pub(crate) fn identity(&self) -> WalIdentity {
        self.config.identity
    }

    pub(crate) fn record_alignment(&self) -> u32 {
        self.config.record_alignment
    }

    pub(crate) fn readable_cap_lsn(&self) -> Option<Lsn> {
        self.fatal_state.map(|state| state.safe_lsn)
    }

    fn ensure_record_admission_within_wal_size_limit(
        &self,
        record_len: u64,
        reserve_future_seal_space: bool,
    ) -> Result<(), WalError> {
        let Some(limit) = self.config.max_wal_size else {
            return Ok(());
        };

        let current_admitted_size = self
            .current_wal_size
            .checked_add(self.write_buffer.len() as u64)
            .ok_or(WalError::ReservationOverflow)?;

        let projected_growth =
            self.predicted_record_admission_growth(record_len, reserve_future_seal_space)?;
        let projected_size = current_admitted_size
            .checked_add(projected_growth)
            .ok_or(WalError::ReservationOverflow)?;

        if projected_size > limit {
            return Err(WalError::WalSizeLimitExceeded {
                current: projected_size,
                limit,
            });
        }

        Ok(())
    }

    fn predicted_record_admission_growth(
        &self,
        record_len: u64,
        reserve_future_seal_space: bool,
    ) -> Result<u64, WalError> {
        let mut growth = record_len;

        match self.active_segment.as_ref() {
            None => {
                growth = growth
                    .checked_add(SEGMENT_HEADER_LEN)
                    .ok_or(WalError::ReservationOverflow)?;
            }
            Some(_)
                if reserve_future_seal_space
                    && !self.active_segment_can_fit(record_len, true)? =>
            {
                if !self.active_segment_is_empty() {
                    growth = growth
                        .checked_add(self.seal_record_physical_len()? as u64)
                        .and_then(|value| value.checked_add(SEGMENT_HEADER_LEN))
                        .ok_or(WalError::ReservationOverflow)?;
                }
            }
            Some(_) => {}
        }

        Ok(growth)
    }

    fn ensure_writable_segment(&mut self) -> Result<(), WalError> {
        if self.active_segment.is_some() {
            return Ok(());
        }

        let segment = self.create_segment(self.next_segment_id, self.next_lsn)?;
        self.active_segment = Some(segment);
        self.active_segment_record_count = 0;
        Ok(())
    }

    fn create_segment(
        &mut self,
        segment_id: SegmentId,
        base_lsn: Lsn,
    ) -> Result<ActiveSegment<D::File>, WalError> {
        let following_segment_id = segment_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        let mut header = SegmentHeader::new(
            self.config.identity,
            segment_id,
            base_lsn,
            self.segment_compression_algorithm(),
            self.config.format_version,
        );
        header.finalize_checksum();

        let file = match self.directory.create_segment(NewSegment {
            segment_id,
            base_lsn,
            header: header.clone(),
        }) {
            Ok(file) => file,
            Err(source) => return Err(self.mark_sticky_fatal("create segment", source)),
        };

        let active_segment = match ActiveSegment::open(file, header) {
            Ok(segment) => segment,
            Err(err) => return Err(self.promote_mutating_error("open new active segment", err)),
        };

        self.current_wal_size = self
            .current_wal_size
            .checked_add(active_segment.file_len())
            .ok_or(WalError::ReservationOverflow)?;

        self.next_segment_id = following_segment_id;
        self.metrics.set_current_wal_size(self.current_wal_size);
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

        self.stage_record(
            record_types::SEGMENT_SEAL,
            record_flags::NONE,
            &seal_payload,
            false,
        )?;

        self.flush_inner()?;
        self.sync_inner()?;

        if let Some(segment) = self.active_segment.as_mut() {
            segment.mark_sealed();
        }

        let next_base_lsn = self.next_lsn;
        let new_segment = self.create_segment(self.next_segment_id, next_base_lsn)?;
        self.active_segment = Some(new_segment);
        self.active_segment_record_count = 0;
        self.metrics.note_segment_rollover();

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

            let append_err = {
                let active_segment = self
                    .active_segment
                    .as_mut()
                    .ok_or(WalError::BrokenDurabilityContract)?;

                match active_segment.file_mut().append_all(&chunk) {
                    Ok(()) => {
                        active_segment.note_bytes_written(chunk_len)?;
                        None
                    }
                    Err(source) => Some(source),
                }
            };

            if let Some(source) = append_err {
                return Err(self.mark_sticky_fatal("append wal bytes", source));
            }

            self.current_wal_size = self
                .current_wal_size
                .checked_add(chunk_len)
                .ok_or(WalError::ReservationOverflow)?;
            self.metrics.set_current_wal_size(self.current_wal_size);

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

    fn ensure_not_fatal(&self) -> Result<(), WalError> {
        if let Some(state) = self.fatal_state {
            return Err(WalError::fatal_io(
                state.operation,
                io::Error::other("wal is in sticky fatal state; reopen required"),
            ));
        }

        Ok(())
    }

    fn ensure_operational(&self) -> Result<(), WalError> {
        self.ensure_mutable()?;
        self.ensure_not_fatal()?;

        if self.shutdown_in_progress {
            return Err(WalError::ShutdownInProgress);
        }

        Ok(())
    }

    fn mark_sticky_fatal(&mut self, operation: &'static str, source: io::Error) -> WalError {
        if self.fatal_state.is_none() {
            self.fatal_state = Some(StickyFatalState {
                operation,
                safe_lsn: self.durable_lsn,
            });
        }

        WalError::fatal_io(operation, source)
    }

    fn promote_mutating_error(&mut self, operation: &'static str, err: WalError) -> WalError {
        match err {
            WalError::Io(source) => self.mark_sticky_fatal(operation, source),
            WalError::FatalIo { source, .. } => self.mark_sticky_fatal(operation, source),
            other => other,
        }
    }

    fn snapshot_readable_segments(&self) -> Result<Vec<SnapshotSegment<D::File>>, WalError> {
        match self.fatal_state {
            Some(state) => {
                snapshot_segments_through(&self.directory, self.config.identity, state.safe_lsn)
            }
            None => snapshot_segments(&self.directory, self.config.identity),
        }
    }

    fn recompute_first_lsn_after_prune(
        &self,
        planned_first_remaining_lsn: Option<Lsn>,
    ) -> Option<Lsn> {
        planned_first_remaining_lsn.or_else(|| {
            let active_segment = self.active_segment.as_ref()?;

            if active_segment.written_logical_len() > 0 || !self.write_buffer.is_empty() {
                Some(active_segment.base_lsn())
            } else {
                None
            }
        })
    }

    fn flush_inner(&mut self) -> Result<(), WalError> {
        if self.active_segment.is_none() {
            return Ok(());
        }

        self.drain_buffer(false)?;

        if let Some(segment) = self.active_segment.as_mut() {
            if let Err(source) = segment.file_mut().flush() {
                return Err(self.mark_sticky_fatal("flush wal bytes", source));
            }
        }

        Ok(())
    }

    fn sync_inner(&mut self) -> Result<(), WalError> {
        let bytes_synced = self
            .next_lsn
            .checked_distance_from(self.durable_lsn)
            .ok_or(WalError::BrokenDurabilityContract)?;

        let started = Instant::now();
        self.flush_inner()?;

        if let Some(segment) = self.active_segment.as_mut() {
            if let Err(source) = segment.file_mut().sync() {
                return Err(self.mark_sticky_fatal("sync wal segment", source));
            }
        }

        self.durable_lsn = self.next_lsn;
        self.metrics.note_sync(bytes_synced, started.elapsed());

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

    pub fn read_at(&self, lsn: Lsn) -> Result<WalRecord, WalError> {
        let segments = self.snapshot_readable_segments()?;
        read_record_at_snapshot(&segments, self.config.record_alignment, lsn)
    }

    pub fn iter_from(&self, from: Lsn) -> Result<WalIterator<D::File>, WalError> {
        let segments = self.snapshot_readable_segments()?;
        WalIterator::new(segments, self.config.record_alignment, from)
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

fn is_io_like(err: &WalError) -> bool {
    matches!(err, WalError::Io(_) | WalError::FatalIo { .. })
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
        collections::BTreeMap,
        fs, io,
        path::{Path, PathBuf},
        process,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        config::SEGMENT_HEADER_LEN,
        format::record_header::RecordHeader,
        io::{
            directory::{FsSegmentDirectory, NewSegment, SegmentDirectory, SegmentMeta},
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
        .0
    }

    #[derive(Debug, Default)]
    struct InjectedSegmentState {
        bytes: Vec<u8>,
        sync_calls: u64,
        fail_sync_on_call: Option<u64>,
    }

    #[derive(Debug, Clone)]
    struct InjectedSegmentFile {
        state: Arc<Mutex<InjectedSegmentState>>,
    }

    impl SegmentFile for InjectedSegmentFile {
        fn len(&self) -> io::Result<u64> {
            Ok(self.state.lock().unwrap().bytes.len() as u64)
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let state = self.state.lock().unwrap();
            let start = offset as usize;

            if start >= state.bytes.len() {
                return Ok(0);
            }

            let end = (start + buf.len()).min(state.bytes.len());
            let len = end - start;
            buf[..len].copy_from_slice(&state.bytes[start..end]);
            Ok(len)
        }

        fn append_all(&mut self, buf: &[u8]) -> io::Result<()> {
            self.state.lock().unwrap().bytes.extend_from_slice(buf);
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn sync(&mut self) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.sync_calls += 1;

            if state.fail_sync_on_call == Some(state.sync_calls) {
                return Err(io::Error::other("injected sync failure"));
            }

            Ok(())
        }

        fn truncate(&mut self, len: u64) -> io::Result<()> {
            self.state.lock().unwrap().bytes.truncate(len as usize);
            Ok(())
        }

        fn advise_sequential(&self) -> io::Result<()> {
            Ok(())
        }

        fn prefetch(&self, _offset: u64, _len: u64) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct InjectedSegmentMeta {
        base_lsn: Lsn,
        state: Arc<Mutex<InjectedSegmentState>>,
    }

    #[derive(Debug, Clone, Default)]
    struct InjectedDirectory {
        segments: Arc<Mutex<BTreeMap<SegmentId, InjectedSegmentMeta>>>,
        fail_sync_on_segment: Option<SegmentId>,
        fail_sync_on_call: u64,
    }

    impl InjectedDirectory {
        fn with_sync_failure(segment_id: SegmentId, sync_call: u64) -> Self {
            Self {
                segments: Arc::new(Mutex::new(BTreeMap::new())),
                fail_sync_on_segment: Some(segment_id),
                fail_sync_on_call: sync_call,
            }
        }
    }

    impl SegmentDirectory for InjectedDirectory {
        type File = InjectedSegmentFile;

        fn list_segments(&self) -> io::Result<Vec<SegmentMeta>> {
            let segments = self.segments.lock().unwrap();
            let mut metas = segments
                .iter()
                .map(|(&segment_id, meta)| SegmentMeta {
                    segment_id,
                    base_lsn: meta.base_lsn,
                    path: PathBuf::from(format!("segment-{segment_id}.wal")),
                })
                .collect::<Vec<_>>();

            metas.sort_by_key(|meta| (meta.base_lsn, meta.segment_id));
            Ok(metas)
        }

        fn create_segment(&self, spec: NewSegment) -> io::Result<Self::File> {
            let mut state = InjectedSegmentState::default();
            state.bytes.extend_from_slice(&spec.header.encode());

            if self.fail_sync_on_segment == Some(spec.segment_id) {
                state.fail_sync_on_call = Some(self.fail_sync_on_call);
            }

            let state = Arc::new(Mutex::new(state));
            self.segments.lock().unwrap().insert(
                spec.segment_id,
                InjectedSegmentMeta {
                    base_lsn: spec.base_lsn,
                    state: state.clone(),
                },
            );

            Ok(InjectedSegmentFile { state })
        }

        fn open_segment(&self, id: SegmentId) -> io::Result<Self::File> {
            let meta = self
                .segments
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing segment"))?;

            Ok(InjectedSegmentFile { state: meta.state })
        }

        fn remove_segment(&self, id: SegmentId) -> io::Result<()> {
            self.segments.lock().unwrap().remove(&id);
            Ok(())
        }

        fn recycle_sgement(
            &self,
            old_id: SegmentId,
            new_spec: NewSegment,
        ) -> io::Result<Self::File> {
            self.remove_segment(old_id)?;
            self.create_segment(new_spec)
        }

        fn sync_directory(&self) -> io::Result<()> {
            Ok(())
        }
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
        config.max_record_size = 600;

        let (mut wal, _report) = Wal::open(
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
        config.max_record_size = 16;
        config.target_segment_size = SEGMENT_HEADER_LEN + 48 + 56;

        let (mut wal, _report) = Wal::open(
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

        let (reopened, _report) = Wal::open(
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

    #[test]
    fn failed_sync_enters_sticky_fatal_state_and_clamps_reads_to_durable_prefix() {
        let test_dir = TestDir::new("sticky-fatal");
        let directory = InjectedDirectory::with_sync_failure(1, 2);

        let (mut wal, _report) = Wal::open(directory.clone(), test_dir.config(), ()).unwrap();

        let first_lsn = wal
            .append(RecordType::new(record_types::USER_MIN), b"first")
            .unwrap();
        wal.sync().unwrap();

        let second_lsn = wal
            .append(RecordType::new(record_types::USER_MIN), b"second")
            .unwrap();

        let sync_err = wal.sync().unwrap_err();
        assert!(matches!(
            sync_err,
            WalError::FatalIo {
                operation: "sync wal segment",
                ..
            }
        ));

        let append_err = wal
            .append(RecordType::new(record_types::USER_MIN), b"third")
            .unwrap_err();
        let flush_err = wal.flush().unwrap_err();
        let second_sync_err = wal.sync().unwrap_err();
        let shutdown_err = wal.shutdown().unwrap_err();

        assert!(matches!(
            append_err,
            WalError::FatalIo {
                operation: "sync wal segment",
                ..
            }
        ));
        assert!(matches!(
            flush_err,
            WalError::FatalIo {
                operation: "sync wal segment",
                ..
            }
        ));
        assert!(matches!(
            second_sync_err,
            WalError::FatalIo {
                operation: "sync wal segment",
                ..
            }
        ));
        assert!(matches!(
            shutdown_err,
            WalError::FatalIo {
                operation: "sync wal segment",
                ..
            }
        ));

        let first = wal.read_at(first_lsn).unwrap();
        assert_eq!(first.payload, b"first");

        let second_err = wal.read_at(second_lsn).unwrap_err();
        assert!(matches!(
            second_err,
            WalError::LsnOutOfRange { lsn } if lsn == second_lsn
        ));

        let mut iter = wal.iter_from(first_lsn).unwrap();
        let first_seen = iter.next().unwrap().unwrap();
        assert_eq!(first_seen.lsn, first_lsn);
        assert_eq!(first_seen.payload, b"first");
        assert!(iter.next().unwrap().is_none());
    }
}
