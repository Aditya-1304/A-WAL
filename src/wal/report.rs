use std::time::Duration;

use crate::lsn::Lsn;

pub struct RecoveryReport {
    pub segments_scanned: usize,
    pub records_scanned: u64,
    pub corrupt_records_found: u64,
    pub first_lsn: Option<Lsn>,
    pub last_valid_lsn: Option<Lsn>,
    pub next_lsn: Lsn,
    pub checkpoint_lsn: Option<Lsn>,
    pub truncated_bytes: u64,
    pub sealed_segments: usize,
    pub segments_prunable: usize,
    pub recovery_duration: Duration,
    pub clean_shutdown: bool,
    pub recovery_skipped: bool,
}

impl RecoveryReport {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn note_segment_scanned(&mut self, sealed: bool) {
        self.segments_scanned += 1;

        if sealed {
            self.sealed_segments += 1;
        }
    }

    pub fn note_record_scanned(&mut self, lsn: Lsn) {
        self.records_scanned += 1;

        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn)
        }

        self.last_valid_lsn = Some(lsn)
    }

    pub fn note_corruption(&mut self) {
        self.corrupt_records_found += 1;
    }

    pub fn note_truncation(&mut self, truncated_bytes: u64) {
        self.truncated_bytes = self.truncated_bytes.saturating_add(truncated_bytes);
    }

    pub fn set_next_lsn(&mut self, next_lsn: Lsn) {
        self.next_lsn = next_lsn;
    }

    pub fn set_checkpoint_lsn(&mut self, checkpoint_lsn: Option<Lsn>) {
        self.checkpoint_lsn = checkpoint_lsn
    }

    pub fn set_segments_prunable(&mut self, segments_prunable: usize) {
        self.segments_prunable = segments_prunable;
    }

    pub fn set_recovery_duration(&mut self, recovery_duration: Duration) {
        self.recovery_duration = recovery_duration;
    }

    pub fn mark_clean_shutdown(&mut self, clean_shutdown: bool) {
        self.clean_shutdown = clean_shutdown;
    }

    pub fn mark_recovery_skipped(&mut self, recovery_skipped: bool) {
        self.recovery_skipped = recovery_skipped;
    }
}

impl Default for RecoveryReport {
    fn default() -> Self {
        Self {
            segments_scanned: 0,
            records_scanned: 0,
            corrupt_records_found: 0,
            first_lsn: None,
            last_valid_lsn: None,
            next_lsn: Lsn::ZERO,
            checkpoint_lsn: None,
            truncated_bytes: 0,
            sealed_segments: 0,
            segments_prunable: 0,
            recovery_duration: Duration::ZERO,
            clean_shutdown: false,
            recovery_skipped: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_report_starts_with_zeroed_recovery_state() {
        let report = RecoveryReport::empty();

        assert_eq!(report.segments_scanned, 0);
        assert_eq!(report.records_scanned, 0);
        assert_eq!(report.corrupt_records_found, 0);
        assert_eq!(report.first_lsn, None);
        assert_eq!(report.last_valid_lsn, None);
        assert_eq!(report.next_lsn, Lsn::ZERO);
        assert_eq!(report.checkpoint_lsn, None);
        assert_eq!(report.truncated_bytes, 0);
        assert_eq!(report.sealed_segments, 0);
        assert_eq!(report.segments_prunable, 0);
        assert_eq!(report.recovery_duration, Duration::ZERO);
        assert!(!report.clean_shutdown);
        assert!(!report.recovery_skipped);
    }

    #[test]
    fn note_record_scanned_sets_first_and_last_lsns() {
        let mut report = RecoveryReport::empty();

        report.note_record_scanned(Lsn::new(100));
        report.note_record_scanned(Lsn::new(148));

        assert_eq!(report.records_scanned, 2);
        assert_eq!(report.first_lsn, Some(Lsn::new(100)));
        assert_eq!(report.last_valid_lsn, Some(Lsn::new(148)));
    }

    #[test]
    fn note_segment_scanned_counts_total_and_sealed_segments() {
        let mut report = RecoveryReport::empty();

        report.note_segment_scanned(false);
        report.note_segment_scanned(true);
        report.note_segment_scanned(true);

        assert_eq!(report.segments_scanned, 3);
        assert_eq!(report.sealed_segments, 2);
    }

    #[test]
    fn note_corruption_and_truncation_accumulate() {
        let mut report = RecoveryReport::empty();

        report.note_corruption();
        report.note_corruption();
        report.note_truncation(128);
        report.note_truncation(64);

        assert_eq!(report.corrupt_records_found, 2);
        assert_eq!(report.truncated_bytes, 192);
    }

    #[test]
    fn setters_update_recovery_outcome_fields() {
        let mut report = RecoveryReport::empty();

        report.set_next_lsn(Lsn::new(4096));
        report.set_checkpoint_lsn(Some(Lsn::new(2048)));
        report.set_segments_prunable(3);
        report.set_recovery_duration(Duration::from_millis(15));
        report.mark_clean_shutdown(true);
        report.mark_recovery_skipped(true);

        assert_eq!(report.next_lsn, Lsn::new(4096));
        assert_eq!(report.checkpoint_lsn, Some(Lsn::new(2048)));
        assert_eq!(report.segments_prunable, 3);
        assert_eq!(report.recovery_duration, Duration::from_millis(15));
        assert!(report.clean_shutdown);
        assert!(report.recovery_skipped);
    }
}
