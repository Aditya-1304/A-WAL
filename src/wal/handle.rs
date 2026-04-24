use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::wal::iterator::{BufferedSegmentFile, snapshot_segments_with_buffer};
use crate::{
    config::WalConfig,
    error::WalError,
    io::directory::SegmentDirectory,
    lsn::Lsn,
    types::{RecordType, WalIdentity},
    wal::{
        engine::Wal,
        iterator::{
            SnapshotSegment, WalIterator, WalRecord, read_record_at_snapshot, snapshot_segments,
            snapshot_segments_through,
        },
        metrics::WalMetrics,
        recovery_observer::RecoveryObserver,
        report::RecoveryReport,
        retention_pin::RetentionPinGuard,
        sync_coordinator::SyncCoordinator,
    },
};

pub struct WalHandle<D, C>
where
    D: SegmentDirectory,
{
    inner: Arc<Mutex<Wal<D, C>>>,
    sync_state: Arc<(Mutex<SyncCoordinator>, Condvar)>,
    tail_state: Arc<(Mutex<TailCoordinator>, Condvar)>,
}

#[derive(Debug, Clone)]
struct ReadSnapshotPlan<D> {
    directory: D,
    identity: WalIdentity,
    record_alignment: u32,
    cap_lsn: Option<Lsn>,
}

#[derive(Debug, Default)]
struct TailCoordinator {
    generation: u64,
}

pub struct WalTailIterator<D, C>
where
    D: SegmentDirectory,
{
    handle: WalHandle<D, C>,
    current_lsn: Lsn,
    observed_generation: u64,
    _pin: RetentionPinGuard,
}

#[derive(Debug, Clone)]
struct TailSnapshotPlan<D> {
    directory: D,
    identity: WalIdentity,
    record_alignment: u32,
    active_buffer: Option<(u64, u64, Vec<u8>)>,
}

impl<D> TailSnapshotPlan<D>
where
    D: SegmentDirectory,
{
    fn load_segments(
        &self,
    ) -> Result<Vec<SnapshotSegment<BufferedSegmentFile<D::File>>>, WalError> {
        snapshot_segments_with_buffer(&self.directory, self.identity, self.active_buffer.clone())
    }
}

impl<D, C> Clone for WalHandle<D, C>
where
    D: SegmentDirectory,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            sync_state: Arc::clone(&self.sync_state),
            tail_state: Arc::clone(&self.tail_state),
        }
    }
}

impl<D> ReadSnapshotPlan<D>
where
    D: SegmentDirectory,
{
    fn load_segments(&self) -> Result<Vec<SnapshotSegment<D::File>>, WalError> {
        match self.cap_lsn {
            Some(end_lsn) => snapshot_segments_through(&self.directory, self.identity, end_lsn),
            None => snapshot_segments(&self.directory, self.identity),
        }
    }
}

impl<D, C> WalHandle<D, C>
where
    D: SegmentDirectory + Clone,
{
    pub fn new(wal: Wal<D, C>) -> Self {
        let durable_lsn = wal.durable_lsn();

        Self {
            inner: Arc::new(Mutex::new(wal)),
            sync_state: Arc::new((
                Mutex::new(SyncCoordinator::new(durable_lsn)),
                Condvar::new(),
            )),
            tail_state: Arc::new((Mutex::new(TailCoordinator::default()), Condvar::new())),
        }
    }

    pub fn open(
        directory: D,
        config: WalConfig,
        checksummer: C,
    ) -> Result<(Self, RecoveryReport), WalError> {
        let (wal, report) = Wal::open(directory, config, checksummer)?;
        Ok((Self::new(wal), report))
    }

    pub fn open_with_observer(
        directory: D,
        config: WalConfig,
        checksummer: C,
        observer: &dyn RecoveryObserver,
    ) -> Result<(Self, RecoveryReport), WalError> {
        let (wal, report) = Wal::open_with_observer(directory, config, checksummer, observer)?;
        Ok((Self::new(wal), report))
    }

    pub fn append(&self, record_type: RecordType, payload: &[u8]) -> Result<Lsn, WalError> {
        self.with_wal_mut(|wal| wal.append(record_type, payload))
    }

    pub fn append_batch(&self, records: &[(RecordType, &[u8])]) -> Result<Vec<Lsn>, WalError> {
        self.with_wal_mut(|wal| wal.append_batch(records))
    }

    pub fn flush(&self) -> Result<(), WalError> {
        self.with_wal_mut(Wal::flush)
    }

    pub fn sync(&self) -> Result<(), WalError> {
        self.with_wal_mut(Wal::sync)
    }

    pub fn sync_through(&self, lsn: Lsn) -> Result<(), WalError> {
        let (lock, condvar) = &*self.sync_state;

        let mut coordinator = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if coordinator.durable_lsn() >= lsn {
            return Ok(());
        }

        coordinator.add_waiter(lsn);

        loop {
            if coordinator.durable_lsn() >= lsn {
                return coordinator.finish_waiter(Ok(()));
            }

            if let Some(error) = coordinator.last_error() {
                return coordinator.finish_waiter(Err(error));
            }

            coordinator.refresh_requested_lsn(lsn);

            if coordinator.sync_in_flight {
                coordinator = condvar
                    .wait(coordinator)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());

                continue;
            }

            let requested_lsn = coordinator.requested_lsn.unwrap_or(lsn);

            coordinator.begin_sync();

            drop(coordinator);

            let sync_result = self.perform_group_sync(lsn, requested_lsn);

            coordinator = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

            match sync_result {
                Ok(durable_lsn) => {
                    coordinator.finish_sync_success(durable_lsn);

                    condvar.notify_all();
                }

                Err(WalError::LsnOutOfRange { lsn }) => {
                    coordinator.finish_sync_without_error();

                    let result = coordinator.finish_waiter(Err(WalError::LsnOutOfRange { lsn }));

                    condvar.notify_all();

                    return result;
                }

                Err(error) => {
                    let result = Err(error.clone());

                    coordinator.finish_sync_error(error);

                    let result = coordinator.finish_waiter(result);

                    condvar.notify_all();

                    return result;
                }
            }
        }
    }

    pub fn shutdown(&self) -> Result<(), WalError> {
        self.with_wal_mut(Wal::shutdown)
    }

    pub fn read_at(&self, lsn: Lsn) -> Result<WalRecord, WalError> {
        let snapshot = self.read_snapshot_plan();
        let segments = snapshot.load_segments()?;
        read_record_at_snapshot(&segments, snapshot.record_alignment, lsn)
    }

    pub fn iter_from(&self, from: Lsn) -> Result<WalIterator<D::File>, WalError> {
        let snapshot = self.read_snapshot_plan();
        let segments = snapshot.load_segments()?;
        WalIterator::new(segments, snapshot.record_alignment, from)
    }

    pub fn tail_from(&self, from: Lsn) -> Result<WalTailIterator<D, C>, WalError> {
        let pin = self.acquire_retention_pin("tail", from)?;
        let observed_generation = self.tail_generation();

        let tail = WalTailIterator {
            handle: self.clone(),
            current_lsn: from,
            observed_generation,
            _pin: pin,
        };

        tail.validate_start()?;
        Ok(tail)
    }

    pub fn durable_lsn(&self) -> Lsn {
        let (lock, _) = &*self.sync_state;

        lock.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .durable_lsn()
    }

    pub fn set_min_retention_lsn(&self, lsn: Lsn) -> Result<(), WalError> {
        self.with_wal_mut(|wal| wal.set_min_retention_lsn(lsn))
    }

    pub fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<RetentionPinGuard, WalError> {
        self.lock().acquire_retention_pin(holder_name, min_lsn)
    }

    pub fn truncate_segments_before(&self, lsn: Lsn) -> Result<usize, WalError> {
        self.with_wal_mut(|wal| wal.truncate_segments_before(lsn))
    }

    pub fn metrics(&self) -> WalMetrics {
        self.lock().metrics()
    }

    pub fn current_wal_size(&self) -> u64 {
        self.lock().current_wal_size()
    }

    fn read_snapshot_plan(&self) -> ReadSnapshotPlan<D> {
        let wal = self.lock();

        ReadSnapshotPlan {
            directory: wal.cloned_directory(),
            identity: wal.identity(),
            record_alignment: wal.record_alignment(),
            cap_lsn: wal.readable_cap_lsn(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Wal<D, C>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn with_wal_mut<T>(
        &self,
        operation: impl FnOnce(&mut Wal<D, C>) -> Result<T, WalError>,
    ) -> Result<T, WalError> {
        let mut wal = self.lock();
        let result = operation(&mut wal);
        self.publish_locked_durable_lsn(&wal);
        drop(wal);
        self.notify_tailers();
        result
    }

    fn publish_locked_durable_lsn(&self, wal: &Wal<D, C>) {
        let (lock, condvar) = &*self.sync_state;

        let mut coordinator = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        coordinator.publish_durable_lsn(wal.durable_lsn());

        drop(coordinator);

        condvar.notify_all();
    }

    fn perform_group_sync(&self, own_lsn: Lsn, requested_lsn: Lsn) -> Result<Lsn, WalError> {
        let mut wal = self.lock();

        if wal.durable_lsn() >= own_lsn {
            return Ok(wal.durable_lsn());
        }

        if wal.next_lsn() < own_lsn {
            return Err(WalError::LsnOutOfRange { lsn: own_lsn });
        }

        let sync_target = requested_lsn.min(wal.next_lsn());

        wal.sync()?;

        let durable_lsn = wal.durable_lsn();

        if durable_lsn < sync_target {
            return Err(WalError::BrokenDurabilityContract);
        }

        Ok(durable_lsn)
    }

    fn tail_snapshot_plan(&self) -> Result<TailSnapshotPlan<D>, WalError> {
        let wal = self.lock();
        wal.ensure_tail_available()?;

        Ok(TailSnapshotPlan {
            directory: wal.cloned_directory(),
            identity: wal.identity(),
            record_alignment: wal.record_alignment(),
            active_buffer: wal.active_buffer_snapshot(),
        })
    }

    fn tail_generation(&self) -> u64 {
        let (lock, _) = &*self.tail_state;
        lock.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }

    fn notify_tailers(&self) {
        let (lock, condvar) = &*self.tail_state;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.generation = state.generation.saturating_add(1);
        condvar.notify_all();
    }
}

impl<D, C> WalTailIterator<D, C>
where
    D: SegmentDirectory + Clone,
{
    pub fn next_nonblocking(&mut self) -> Result<Option<WalRecord>, WalError> {
        let snapshot = self.handle.tail_snapshot_plan()?;
        let segments = snapshot.load_segments()?;
        let mut iter = WalIterator::new(segments, snapshot.record_alignment, self.current_lsn)?;

        match iter.next()? {
            Some(record) => {
                self.current_lsn = iter.current_lsn();
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    pub fn next_blocking(&mut self, timeout: Duration) -> Result<Option<WalRecord>, WalError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(WalError::ReservationOverflow)?;

        loop {
            if let Some(record) = self.next_nonblocking()? {
                return Ok(Some(record));
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }

            let remaining = deadline.duration_since(now);
            let (lock, condvar) = &*self.handle.tail_state;
            let state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

            if state.generation != self.observed_generation {
                self.observed_generation = state.generation;
                continue;
            }

            let (state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            self.observed_generation = state.generation;

            if wait_result.timed_out() {
                return Ok(None);
            }
        }
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    fn validate_start(&self) -> Result<(), WalError> {
        let snapshot = self.handle.tail_snapshot_plan()?;
        let segments = snapshot.load_segments()?;
        let _ = WalIterator::new(segments, snapshot.record_alignment, self.current_lsn)?;
        Ok(())
    }
}
