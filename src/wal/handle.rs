use std::sync::{Arc, Mutex, MutexGuard};

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
    sync_state: Arc<SyncCoordinator>,
}

#[derive(Debug, Clone)]
struct ReadSnapshotPlan<D> {
    directory: D,
    identity: WalIdentity,
    record_alignment: u32,
    cap_lsn: Option<Lsn>,
}

impl<D, C> Clone for WalHandle<D, C>
where
    D: SegmentDirectory,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            sync_state: Arc::clone(&self.sync_state),
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
            sync_state: Arc::new(SyncCoordinator::new(durable_lsn)),
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
        if self.sync_state.durable_lsn() >= lsn {
            return Ok(());
        }

        let mut wal = self.lock();

        if wal.durable_lsn() >= lsn {
            self.publish_locked_durable_lsn(&wal);
            return Ok(());
        }

        if wal.next_lsn() < lsn {
            return Err(WalError::LsnOutOfRange { lsn });
        }

        let result = wal.sync();
        self.publish_locked_durable_lsn(&wal);
        result?;

        if self.sync_state.durable_lsn() >= lsn {
            Ok(())
        } else {
            Err(WalError::BrokenDurabilityContract)
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

    pub fn durable_lsn(&self) -> Lsn {
        self.sync_state.durable_lsn()
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
        result
    }

    fn publish_locked_durable_lsn(&self, wal: &Wal<D, C>) {
        self.sync_state.publish_durable_lsn(wal.durable_lsn());
    }
}
