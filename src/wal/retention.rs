use crate::{
    error::WalError,
    format::segment_header::SegmentHeader,
    io::{directory::SegmentDirectory, segment_file::SegmentFile},
    lsn::Lsn,
    types::{SegmentId, WalIdentity},
    wal::segment::SegmentDescriptor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionState {
    min_retention_lsn: Lsn,
}

impl Default for RetentionState {
    fn default() -> Self {
        Self {
            min_retention_lsn: Lsn::ZERO,
        }
    }
}

impl RetentionState {
    pub fn set_min_retention_lsn(&mut self, lsn: Lsn) {
        self.min_retention_lsn = lsn;
    }

    pub fn effective_floor(&self) -> Lsn {
        self.min_retention_lsn
    }

    pub fn requested_prune_floor(&self, requested_lsn: Lsn) -> Lsn {
        requested_lsn.max(self.effective_floor())
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

    for meta in metas {
        let file = directory.open_segment(meta.segment_id)?;
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
    let mut removed = 0usize;

    for &segment_id in &plan.removable_segment_ids {
        directory.remove_segment(segment_id)?;
        directory.sync_directory()?;
        removed += 1;
    }

    Ok(removed)
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
    fn requested_prune_floor_uses_the_more_aggressive_of_request_and_configured_floor() {
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
}
