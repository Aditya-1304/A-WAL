use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{error::WalError, lsn::Lsn, types::SegmentId};

pub trait RecoveryObserver {
    fn on_segment_start(&self, segment_id: SegmentId, base_lsn: Lsn);
    fn on_records_scanned(&self, count: u64, current_lsn: Lsn);
    fn on_corruption_found(&self, lsn: Lsn, error: &WalError);
    fn on_truncation(&self, at_lsn: Lsn, truncated_bytes: u64);
    fn on_checkpoint_found(&self, checkpoint_lsn: Lsn, checkpoint_no: u64);
}

#[derive(Clone, Copy)]
pub(crate) struct RecoveryCallbacks<'a> {
    observer: Option<&'a dyn RecoveryObserver>,
}

impl<'a> RecoveryCallbacks<'a> {
    pub(crate) const fn new(observer: Option<&'a dyn RecoveryObserver>) -> Self {
        Self { observer }
    }

    pub(crate) fn on_segment_start(&self, segment_id: SegmentId, base_lsn: Lsn) {
        self.call(|observer| observer.on_segment_start(segment_id, base_lsn));
    }

    pub(crate) fn on_records_scanned(&self, count: u64, current_lsn: Lsn) {
        self.call(|observer| observer.on_records_scanned(count, current_lsn));
    }

    pub(crate) fn on_corruption_found(&self, lsn: Lsn, error: &WalError) {
        self.call(|observer| observer.on_corruption_found(lsn, error));
    }

    pub(crate) fn on_truncation(&self, at_lsn: Lsn, truncated_bytes: u64) {
        self.call(|observer| observer.on_truncation(at_lsn, truncated_bytes));
    }

    pub(crate) fn on_checkpoint_found(&self, checkpoint_lsn: Lsn, checkpoint_no: u64) {
        self.call(|observer| observer.on_checkpoint_found(checkpoint_lsn, checkpoint_no));
    }

    fn call(&self, f: impl FnOnce(&dyn RecoveryObserver)) {
        if let Some(observer) = self.observer {
            let _ = catch_unwind(AssertUnwindSafe(|| f(observer)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanickingObserver;

    impl RecoveryObserver for PanickingObserver {
        fn on_segment_start(&self, _segment_id: SegmentId, _base_lsn: Lsn) {
            panic!("boom");
        }

        fn on_records_scanned(&self, _count: u64, _current_lsn: Lsn) {
            panic!("boom");
        }

        fn on_corruption_found(&self, _lsn: Lsn, _error: &WalError) {
            panic!("boom");
        }

        fn on_truncation(&self, _at_lsn: Lsn, _truncated_bytes: u64) {
            panic!("boom");
        }

        fn on_checkpoint_found(&self, _checkpoint_lsn: Lsn, _checkpoint_no: u64) {
            panic!("boom");
        }
    }

    #[test]
    fn callback_helper_swallows_observer_panics() {
        let observer = PanickingObserver;
        let callbacks = RecoveryCallbacks::new(Some(&observer));

        callbacks.on_segment_start(1, Lsn::ZERO);
        callbacks.on_records_scanned(1, Lsn::ZERO);
        callbacks.on_corruption_found(Lsn::ZERO, &WalError::ShortRead);
        callbacks.on_truncation(Lsn::ZERO, 16);
        callbacks.on_checkpoint_found(Lsn::ZERO, 0);
    }
}
