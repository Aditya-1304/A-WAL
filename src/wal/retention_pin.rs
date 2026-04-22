use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::Instant,
};

use crate::{error::WalError, lsn::Lsn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPin {
    pub pin_id: u64,
    pub holder_name: String,
    pub min_lsn: Lsn,
    pub created_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RetentionPinRegistry {
    inner: Arc<Mutex<RetentionPinRegistryInner>>,
}

#[derive(Debug)]
struct RetentionPinRegistryInner {
    next_pin_id: u64,
    pins: BTreeMap<u64, RetentionPin>,
}

#[derive(Debug)]
pub struct RetentionPinGuard {
    pin: RetentionPin,
    registry: Weak<Mutex<RetentionPinRegistryInner>>,
    released: bool,
}

impl Default for RetentionPinRegistryInner {
    fn default() -> Self {
        Self {
            next_pin_id: 1,
            pins: BTreeMap::new(),
        }
    }
}

impl RetentionPinRegistry {
    pub(crate) fn acquire(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<RetentionPinGuard, WalError> {
        let mut inner = self.lock_inner();

        let pin_id = inner.next_pin_id;
        inner.next_pin_id = inner
            .next_pin_id
            .checked_add(1)
            .ok_or(WalError::ReservationOverflow)?;

        let pin = RetentionPin {
            pin_id,
            holder_name: holder_name.to_owned(),
            min_lsn,
            created_at: Instant::now(),
        };

        inner.pins.insert(pin_id, pin.clone());

        Ok(RetentionPinGuard {
            pin,
            registry: Arc::downgrade(&self.inner),
            released: false,
        })
    }

    pub(crate) fn active_pin_count(&self) -> usize {
        self.lock_inner().pins.len()
    }

    pub(crate) fn min_pinned_lsn(&self) -> Option<Lsn> {
        self.lock_inner().pins.values().map(|pin| pin.min_lsn).min()
    }

    fn lock_inner(&self) -> MutexGuard<'_, RetentionPinRegistryInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl RetentionPinGuard {
    pub fn pin(&self) -> &RetentionPin {
        &self.pin
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }

        if let Some(registry) = self.registry.upgrade() {
            let mut inner = registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.pins.remove(&self.pin.pin_id);
        }

        self.released = true;
    }
}

impl Drop for RetentionPinGuard {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_guard_releases_pin() {
        let registry = RetentionPinRegistry::default();

        let guard = registry.acquire("tail", Lsn::new(100)).unwrap();
        assert_eq!(registry.active_pin_count(), 1);
        assert_eq!(registry.min_pinned_lsn(), Some(Lsn::new(100)));
        assert_eq!(guard.pin().holder_name, "tail");

        drop(guard);

        assert_eq!(registry.active_pin_count(), 0);
        assert_eq!(registry.min_pinned_lsn(), None);
    }

    #[test]
    fn explicit_release_is_idempotent_via_drop() {
        let registry = RetentionPinRegistry::default();

        let guard = registry.acquire("backup", Lsn::new(200)).unwrap();
        assert_eq!(registry.active_pin_count(), 1);

        guard.release();

        assert_eq!(registry.active_pin_count(), 0);
        assert_eq!(registry.min_pinned_lsn(), None);
    }

    #[test]
    fn min_pinned_lsn_tracks_oldest_active_pin() {
        let registry = RetentionPinRegistry::default();

        let _pin1 = registry.acquire("tail", Lsn::new(300)).unwrap();
        let pin2 = registry.acquire("backup", Lsn::new(150)).unwrap();
        let _pin3 = registry.acquire("reader", Lsn::new(220)).unwrap();

        assert_eq!(registry.min_pinned_lsn(), Some(Lsn::new(150)));

        drop(pin2);

        assert_eq!(registry.min_pinned_lsn(), Some(Lsn::new(220)));
    }
}
