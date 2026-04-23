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
    },
};

pub struct WalHandle<D, C>
where
    D: SegmentDirectory,
{
    inner: Arc<Mutex<Wal<D, C>>>,
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
        Self {
            inner: Arc::new(Mutex::new(wal)),
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
        self.lock().append(record_type, payload)
    }

    pub fn flush(&self) -> Result<(), WalError> {
        self.lock().flush()
    }

    pub fn sync(&self) -> Result<(), WalError> {
        self.lock().sync()
    }

    pub fn sync_through(&self, lsn: Lsn) -> Result<(), WalError> {
        let mut wal = self.lock();

        if wal.durable_lsn() >= lsn {
            return Ok(());
        }

        if wal.next_lsn() < lsn {
            return Err(WalError::LsnOutOfRange { lsn });
        }

        wal.sync()?;

        if wal.durable_lsn() >= lsn {
            Ok(())
        } else {
            Err(WalError::BrokenDurabilityContract)
        }
    }

    pub fn shutdown(&self) -> Result<(), WalError> {
        self.lock().shutdown()
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
        self.lock().durable_lsn()
    }

    pub fn set_min_retention_lsn(&self, lsn: Lsn) -> Result<(), WalError> {
        self.lock().set_min_retention_lsn(lsn)
    }

    pub fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<RetentionPinGuard, WalError> {
        self.lock().acquire_retention_pin(holder_name, min_lsn)
    }

    pub fn truncate_segments_before(&self, lsn: Lsn) -> Result<usize, WalError> {
        self.lock().truncate_segments_before(lsn)
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
}
