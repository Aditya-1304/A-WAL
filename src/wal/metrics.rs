use std::time::Duration;

use crate::wal::report::RecoveryReport;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalMetrics {
    pub records_appended: u64,
    pub batch_appends: u64,
    pub bytes_appended: u64,
    pub bytes_synced: u64,
    pub sync_calls: u64,
    pub total_sync_duration: Duration,
    pub last_sync_duration: Duration,
    pub segment_rollovers: u64,
    pub segments_recycled: u64,
    pub recovery_repairs: u64,
    pub group_commit_batches: u64,
    pub group_commit_waiters: u64,
    pub last_recovery_duration: Duration,
    pub clean_shutdowns: u64,
    pub current_wal_size: u64,
    pub retention_pins_active: usize,
    pub records_compressed: u64,
    pub compression_bytes_saved: u64,
    pub tailing_iterators_active: usize,
}

impl WalMetrics {
    pub fn new(current_wal_size: u64) -> Self {
        Self {
            current_wal_size,
            ..Self::default()
        }
    }

    pub fn note_record_append(&mut self, bytes: u64) {
        self.records_appended = self.records_appended.saturating_add(1);
        self.bytes_appended = self.bytes_appended.saturating_add(bytes);
    }

    pub fn note_appended_records(&mut self, records: u64, bytes: u64) {
        self.records_appended = self.records_appended.saturating_add(records);
        self.bytes_appended = self.bytes_appended.saturating_add(bytes);
    }

    pub fn note_batch_append(&mut self, records: u64, bytes: u64) {
        self.batch_appends = self.batch_appends.saturating_add(1);
        self.note_appended_records(records, bytes);
    }

    pub fn note_sync(&mut self, bytes_synced: u64, duration: Duration) {
        self.sync_calls = self.sync_calls.saturating_add(1);
        self.bytes_synced = self.bytes_synced.saturating_add(bytes_synced);
        self.total_sync_duration = self.total_sync_duration.saturating_add(duration);
        self.last_sync_duration = duration;
    }

    pub fn note_segment_rollover(&mut self) {
        self.segment_rollovers = self.segment_rollovers.saturating_add(1);
    }

    pub fn note_segment_recycled(&mut self) {
        self.segments_recycled = self.segments_recycled.saturating_add(1);
    }

    pub fn note_recovery(&mut self, report: &RecoveryReport) {
        self.last_recovery_duration = report.recovery_duration;

        if report.truncated_bytes > 0 {
            self.recovery_repairs = self.recovery_repairs.saturating_add(1);
        }
    }

    pub fn note_group_commit_batch(&mut self, waiters: u64) {
        self.group_commit_batches = self.group_commit_batches.saturating_add(1);
        self.group_commit_waiters = self.group_commit_waiters.saturating_add(waiters);
    }

    pub fn note_clean_shutdown(&mut self) {
        self.clean_shutdowns = self.clean_shutdowns.saturating_add(1);
    }

    pub fn set_current_wal_size(&mut self, current_wal_size: u64) {
        self.current_wal_size = current_wal_size;
    }

    pub fn set_retention_pins_active(&mut self, retention_pins_active: usize) {
        self.retention_pins_active = retention_pins_active;
    }

    pub fn note_compression(&mut self, original_len: u64, compressed_len: u64) {
        self.records_compressed = self.records_compressed.saturating_add(1);
        self.compression_bytes_saved = self
            .compression_bytes_saved
            .saturating_add(original_len.saturating_sub(compressed_len));
    }

    pub fn note_tailing_iterator_opened(&mut self) {
        self.tailing_iterators_active = self.tailing_iterators_active.saturating_add(1);
    }

    pub fn note_tailing_iterator_closed(&mut self) {
        self.tailing_iterators_active = self.tailing_iterators_active.saturating_sub(1);
    }
}

impl Default for WalMetrics {
    fn default() -> Self {
        Self {
            records_appended: 0,
            batch_appends: 0,
            bytes_appended: 0,
            bytes_synced: 0,
            sync_calls: 0,
            total_sync_duration: Duration::ZERO,
            last_sync_duration: Duration::ZERO,
            segment_rollovers: 0,
            segments_recycled: 0,
            recovery_repairs: 0,
            group_commit_batches: 0,
            group_commit_waiters: 0,
            last_recovery_duration: Duration::ZERO,
            clean_shutdowns: 0,
            current_wal_size: 0,
            retention_pins_active: 0,
            records_compressed: 0,
            compression_bytes_saved: 0,
            tailing_iterators_active: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::report::RecoveryReport;

    #[test]
    fn new_metrics_start_with_given_wal_size() {
        let metrics = WalMetrics::new(4096);

        assert_eq!(metrics.current_wal_size, 4096);
        assert_eq!(metrics.records_appended, 0);
        assert_eq!(metrics.sync_calls, 0);
        assert_eq!(metrics.clean_shutdowns, 0);
    }

    #[test]
    fn append_metrics_accumulate_single_and_batch_appends() {
        let mut metrics = WalMetrics::default();

        metrics.note_record_append(37);
        metrics.note_batch_append(3, 144);

        assert_eq!(metrics.records_appended, 4);
        assert_eq!(metrics.batch_appends, 1);
        assert_eq!(metrics.bytes_appended, 181);
    }

    #[test]
    fn sync_metrics_track_bytes_and_durations() {
        let mut metrics = WalMetrics::default();

        metrics.note_sync(512, Duration::from_millis(4));
        metrics.note_sync(128, Duration::from_millis(7));

        assert_eq!(metrics.sync_calls, 2);
        assert_eq!(metrics.bytes_synced, 640);
        assert_eq!(metrics.total_sync_duration, Duration::from_millis(11));
        assert_eq!(metrics.last_sync_duration, Duration::from_millis(7));
    }

    #[test]
    fn recovery_metrics_record_last_duration_and_repairs() {
        let mut metrics = WalMetrics::default();

        let mut repaired = RecoveryReport::empty();
        repaired.note_truncation(64);
        repaired.set_recovery_duration(Duration::from_millis(12));
        metrics.note_recovery(&repaired);

        let mut clean = RecoveryReport::empty();
        clean.set_recovery_duration(Duration::from_millis(3));
        metrics.note_recovery(&clean);

        assert_eq!(metrics.recovery_repairs, 1);
        assert_eq!(metrics.last_recovery_duration, Duration::from_millis(3));
    }

    #[test]
    fn gauge_and_auxiliary_counters_update() {
        let mut metrics = WalMetrics::default();

        metrics.note_segment_rollover();
        metrics.note_segment_recycled();
        metrics.note_group_commit_batch(5);
        metrics.note_clean_shutdown();
        metrics.set_current_wal_size(8192);
        metrics.set_retention_pins_active(2);
        metrics.note_compression(100, 60);
        metrics.note_tailing_iterator_opened();
        metrics.note_tailing_iterator_opened();
        metrics.note_tailing_iterator_closed();

        assert_eq!(metrics.segment_rollovers, 1);
        assert_eq!(metrics.segments_recycled, 1);
        assert_eq!(metrics.group_commit_batches, 1);
        assert_eq!(metrics.group_commit_waiters, 5);
        assert_eq!(metrics.clean_shutdowns, 1);
        assert_eq!(metrics.current_wal_size, 8192);
        assert_eq!(metrics.retention_pins_active, 2);
        assert_eq!(metrics.records_compressed, 1);
        assert_eq!(metrics.compression_bytes_saved, 40);
        assert_eq!(metrics.tailing_iterators_active, 1);
    }
}
