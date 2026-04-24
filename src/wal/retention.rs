use std::collections::BTreeSet;

use crate::{
    error::WalError,
    format::segment_header::SegmentHeader,
    io::{directory::SegmentDirectory, segment_file::SegmentFile},
    lsn::Lsn,
    types::{SegmentId, WalIdentity},
    wal::{
        retention_pin::{RetentionPinGuard, RetentionPinRegistry},
        segment::SegmentDescriptor,
    },
};

#[derive(Debug, Clone)]
pub struct RetentionState {
    min_retention_lsn: Lsn,
    pin_registry: RetentionPinRegistry,
}

impl Default for RetentionState {
    fn default() -> Self {
        Self {
            min_retention_lsn: Lsn::ZERO,
            pin_registry: RetentionPinRegistry::default(),
        }
    }
}

impl RetentionState {
    pub fn set_min_retention_lsn(&mut self, lsn: Lsn) {
        self.min_retention_lsn = lsn;
    }

    pub fn acquire_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<RetentionPinGuard, WalError> {
        self.pin_registry.acquire(holder_name, min_lsn)
    }

    pub fn active_pin_count(&self) -> usize {
        self.pin_registry.active_pin_count()
    }

    pub fn effective_floor(&self) -> Lsn {
        match self.pin_registry.min_pinned_lsn() {
            Some(min_pinned_lsn) => self.min_retention_lsn.min(min_pinned_lsn),
            None => self.min_retention_lsn,
        }
    }

    pub fn requested_prune_floor(&self, requested_lsn: Lsn) -> Lsn {
        let configured_floor = requested_lsn.max(self.min_retention_lsn);

        match self.pin_registry.min_pinned_lsn() {
            Some(min_pinned_lsn) => configured_floor.min(min_pinned_lsn),
            None => configured_floor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncatePlan {
    pub removable_segment_ids: Vec<SegmentId>,
    pub removed_bytes: u64,
    pub first_remaining_lsn: Option<Lsn>,
}

pub fn plan_truncate_segments_before<D: SegmentDirectory>(
    directory: &D,
    expected_identity: WalIdentity,
    active_segment_id: Option<SegmentId>,
    floor_lsn: Lsn,
) -> Result<TruncatePlan, WalError> {
    let metas = directory.list_segments()?;
    let mut removable_segment_ids = Vec::new();
    let mut removed_bytes = 0u64;
    let mut first_remaining_lsn = None;
    let mut previous_end_lsn = None;
    let mut prefix_is_still_removable = true;
    let mut seen_segment_ids = BTreeSet::new();

    for meta in metas {
        if !seen_segment_ids.insert(meta.segment_id) {
            return Err(WalError::SegmentOrderingViolation);
        }

        let file = directory.open_segment_meta(&meta)?;
        let file_len = file.len()?;
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

        let descriptor = SegmentDescriptor::from_header_with_sealed(
            &header,
            file_len,
            Some(meta.segment_id) != active_segment_id,
        )?;

        if let Some(previous_end_lsn) = previous_end_lsn {
            if descriptor.base_lsn < previous_end_lsn {
                return Err(WalError::SegmentOrderingViolation);
            }
        }

        let segment_end_lsn = descriptor.written_end_lsn()?;
        let is_active = Some(meta.segment_id) == active_segment_id;
        let has_records = descriptor.file_len > descriptor.header_len;

        if prefix_is_still_removable && !is_active && segment_end_lsn <= floor_lsn {
            removable_segment_ids.push(meta.segment_id);
            removed_bytes = removed_bytes
                .checked_add(descriptor.file_len)
                .ok_or(WalError::ReservationOverflow)?;
        } else {
            prefix_is_still_removable = false;

            if first_remaining_lsn.is_none() && has_records {
                first_remaining_lsn = Some(descriptor.base_lsn);
            }
        }

        previous_end_lsn = Some(segment_end_lsn);
    }

    Ok(TruncatePlan {
        removable_segment_ids,
        removed_bytes,
        first_remaining_lsn,
    })
}

pub fn execute_truncate_plan<D: SegmentDirectory>(
    directory: &D,
    plan: &TruncatePlan,
) -> Result<usize, WalError> {
    Ok(directory.remove_segments(&plan.removable_segment_ids)?)
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
    use super::*;

    #[test]
    fn requested_prune_floor_uses_configured_floor_without_pins() {
        let mut state = RetentionState::default();
        state.set_min_retention_lsn(Lsn::new(200));

        assert_eq!(state.requested_prune_floor(Lsn::new(100)), Lsn::new(200));
        assert_eq!(state.requested_prune_floor(Lsn::new(300)), Lsn::new(300));
    }

    #[test]
    fn default_retention_floor_is_zero() {
        let state = RetentionState::default();

        assert_eq!(state.effective_floor(), Lsn::ZERO);
    }

    #[test]
    fn active_pin_lowers_effective_floor_until_released() {
        let mut state = RetentionState::default();
        state.set_min_retention_lsn(Lsn::new(500));

        let pin = state.acquire_pin("tail", Lsn::new(200)).unwrap();

        assert_eq!(state.effective_floor(), Lsn::new(200));
        assert_eq!(state.active_pin_count(), 1);

        drop(pin);

        assert_eq!(state.effective_floor(), Lsn::new(500));
        assert_eq!(state.active_pin_count(), 0);
    }

    #[test]
    fn requested_prune_floor_never_moves_past_oldest_active_pin() {
        let mut state = RetentionState::default();
        state.set_min_retention_lsn(Lsn::new(200));

        let _pin = state.acquire_pin("tail", Lsn::new(120)).unwrap();

        assert_eq!(state.requested_prune_floor(Lsn::new(300)), Lsn::new(120));
        assert_eq!(state.requested_prune_floor(Lsn::new(150)), Lsn::new(120));
    }
}
