use std::sync::atomic::{AtomicU64, Ordering};

use crate::lsn::Lsn;

#[derive(Debug)]
pub struct SyncCoordinator {
    durable_lsn: AtomicU64,
}

impl SyncCoordinator {
    pub fn new(durable_lsn: Lsn) -> Self {
        Self {
            durable_lsn: AtomicU64::new(durable_lsn.as_u64()),
        }
    }

    pub fn durable_lsn(&self) -> Lsn {
        Lsn::new(self.durable_lsn.load(Ordering::Acquire))
    }

    pub fn publish_durable_lsn(&self, durable_lsn: Lsn) {
        self.durable_lsn
            .fetch_max(durable_lsn.as_u64(), Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_lsn_starts_at_initial_value() {
        let coordinator = SyncCoordinator::new(Lsn::new(42));

        assert_eq!(coordinator.durable_lsn(), Lsn::new(42));
    }

    #[test]
    fn durable_lsn_publication_is_monotonic() {
        let coordinator = SyncCoordinator::new(Lsn::new(10));

        coordinator.publish_durable_lsn(Lsn::new(50));
        coordinator.publish_durable_lsn(Lsn::new(25));

        assert_eq!(coordinator.durable_lsn(), Lsn::new(50));
    }
}
