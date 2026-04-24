use std::{
    fs, io,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{RECORD_HEADER_LEN, WalConfig},
    error::WalError,
    io::{
        directory::{FsSegmentDirectory, NewSegment, SegmentDirectory, SegmentMeta},
        segment_file::{FsSegmentFile, SegmentFile},
    },
    lsn::Lsn,
    types::{RecordType, SegmentId, WalIdentity, record_types},
    wal::{WalHandle, engine::Wal},
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();

        let path =
            std::env::temp_dir().join(format!("wal-handle-{prefix}-{}-{nanos}", process::id()));

        fs::create_dir_all(&path).expect("failed to create test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn config(&self) -> WalConfig {
        WalConfig {
            dir: self.path.clone(),
            identity: WalIdentity::new(11, 22, 1),
            ..WalConfig::default()
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone)]
struct BlockingSyncDirectory {
    inner: FsSegmentDirectory,
    state: Arc<BlockingSyncState>,
}

struct BlockingSyncState {
    block_next_sync: AtomicBool,
    sync_entered_tx: mpsc::Sender<()>,
    sync_entered_rx: Mutex<mpsc::Receiver<()>>,
    release_tx: mpsc::Sender<()>,
    release_rx: Mutex<mpsc::Receiver<()>>,
}

struct BlockingSyncFile {
    inner: FsSegmentFile,
    state: Arc<BlockingSyncState>,
}

impl BlockingSyncDirectory {
    fn new(path: PathBuf) -> Self {
        let (sync_entered_tx, sync_entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        Self {
            inner: FsSegmentDirectory::new(path),
            state: Arc::new(BlockingSyncState {
                block_next_sync: AtomicBool::new(false),
                sync_entered_tx,
                sync_entered_rx: Mutex::new(sync_entered_rx),
                release_tx,
                release_rx: Mutex::new(release_rx),
            }),
        }
    }

    fn block_next_sync(&self) {
        self.state.block_next_sync.store(true, Ordering::SeqCst);
    }

    fn wait_until_sync_blocked(&self) {
        self.state
            .sync_entered_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .expect("sync did not reach the blocking point");
    }

    fn release_blocked_sync(&self) {
        self.state
            .release_tx
            .send(())
            .expect("blocked sync receiver was dropped");
    }
}

impl SegmentFile for BlockingSyncFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }

    fn append_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.append_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        if self.state.block_next_sync.swap(false, Ordering::SeqCst) {
            let _ = self.state.sync_entered_tx.send(());
            let _ = self.state.release_rx.lock().unwrap().recv();
        }

        self.inner.sync()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.inner.truncate(len)
    }

    fn advise_sequential(&self) -> io::Result<()> {
        self.inner.advise_sequential()
    }

    fn prefetch(&self, offset: u64, len: u64) -> io::Result<()> {
        self.inner.prefetch(offset, len)
    }
}

impl SegmentDirectory for BlockingSyncDirectory {
    type File = BlockingSyncFile;

    fn list_segments(&self) -> io::Result<Vec<SegmentMeta>> {
        self.inner.list_segments()
    }

    fn create_segment(&self, spec: NewSegment) -> io::Result<Self::File> {
        let inner = self.inner.create_segment(spec)?;
        Ok(BlockingSyncFile {
            inner,
            state: Arc::clone(&self.state),
        })
    }

    fn open_segment(&self, id: SegmentId) -> io::Result<Self::File> {
        let inner = self.inner.open_segment(id)?;
        Ok(BlockingSyncFile {
            inner,
            state: Arc::clone(&self.state),
        })
    }

    fn remove_segment(&self, id: SegmentId) -> io::Result<()> {
        self.inner.remove_segment(id)
    }

    fn recycle_sgement(&self, old_id: SegmentId, new_spec: NewSegment) -> io::Result<Self::File> {
        let inner = self.inner.recycle_sgement(old_id, new_spec)?;
        Ok(BlockingSyncFile {
            inner,
            state: Arc::clone(&self.state),
        })
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }
}

#[test]
fn handle_round_trips_append_sync_and_read_paths() {
    let test_dir = TestDir::new("round-trip");
    let (handle, report) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(report.next_lsn, Lsn::ZERO);

    let lsn = handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    assert_eq!(handle.durable_lsn(), Lsn::ZERO);

    handle.sync_through(Lsn::new(37)).unwrap();

    let record = handle.read_at(lsn).unwrap();
    assert_eq!(record.payload, b"hello");

    let mut iter = handle.iter_from(lsn).unwrap();
    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.lsn, lsn);
    assert_eq!(first.payload, b"hello");
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn durable_lsn_returns_without_waiting_for_in_flight_sync() {
    let test_dir = TestDir::new("durable-lsn-nonblocking");
    let directory = BlockingSyncDirectory::new(test_dir.path().to_path_buf());
    let (handle, _) = WalHandle::open(directory.clone(), test_dir.config(), ()).unwrap();

    handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    directory.block_next_sync();
    let sync_join = {
        let handle = handle.clone();
        thread::spawn(move || handle.sync())
    };
    directory.wait_until_sync_blocked();

    let (observed_tx, observed_rx) = mpsc::channel();
    let reader_join = {
        let handle = handle.clone();
        thread::spawn(move || {
            let durable_lsn = handle.durable_lsn();
            observed_tx.send(durable_lsn).unwrap();
            durable_lsn
        })
    };

    let observed_while_sync_blocked = observed_rx.recv_timeout(Duration::from_secs(1));

    directory.release_blocked_sync();
    sync_join.join().unwrap().unwrap();
    let reader_lsn = reader_join.join().unwrap();

    let observed_while_sync_blocked =
        observed_while_sync_blocked.expect("durable_lsn blocked behind an in-flight sync");
    assert_eq!(observed_while_sync_blocked, Lsn::ZERO);
    assert_eq!(reader_lsn, Lsn::ZERO);
    assert_eq!(handle.durable_lsn(), Lsn::new(37));
}

#[test]
fn concurrent_appends_through_handle_produce_unique_ordered_lsns() {
    let test_dir = TestDir::new("concurrent");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let mut joins = Vec::new();

    for i in 0..8u16 {
        let handle = handle.clone();

        joins.push(thread::spawn(move || {
            let payload = format!("record-{i}").into_bytes();
            let lsn = handle
                .append(RecordType::new(record_types::USER_MIN + i), &payload)
                .unwrap();

            (lsn, payload)
        }));
    }

    let mut results = joins
        .into_iter()
        .map(|join| join.join().unwrap())
        .collect::<Vec<_>>();

    handle.sync().unwrap();

    results.sort_by_key(|(lsn, _)| lsn.as_u64());

    for window in results.windows(2) {
        assert!(window[0].0 < window[1].0);
    }

    for (lsn, payload) in results {
        let record = handle.read_at(lsn).unwrap();
        assert_eq!(record.payload, payload);
    }
}

#[test]
fn handle_retention_pin_updates_metrics() {
    let test_dir = TestDir::new("pin-metrics");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    assert_eq!(handle.metrics().retention_pins_active, 0);

    let pin = handle.acquire_retention_pin("tail", Lsn::ZERO).unwrap();
    assert_eq!(handle.metrics().retention_pins_active, 1);

    drop(pin);
    assert_eq!(handle.metrics().retention_pins_active, 0);
}

#[test]
fn sync_through_returns_immediately_when_target_is_already_durable() {
    let test_dir = TestDir::new("sync-through-already-durable");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let lsn = handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    handle.sync().unwrap();
    let sync_calls_before = handle.metrics().sync_calls;

    handle.sync_through(Lsn::new(37)).unwrap();

    assert_eq!(handle.durable_lsn(), Lsn::new(37));
    assert_eq!(handle.metrics().sync_calls, sync_calls_before);
    assert_eq!(handle.read_at(lsn).unwrap().payload, b"hello");
}

#[test]
fn sync_through_rejects_lsn_beyond_next_lsn() {
    let test_dir = TestDir::new("sync-through-out-of-range");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    handle
        .append(RecordType::new(record_types::USER_MIN), b"hello")
        .unwrap();

    let err = handle.sync_through(Lsn::new(38)).unwrap_err();

    assert!(matches!(err, WalError::LsnOutOfRange { lsn } if lsn == Lsn::new(38)));
    assert_eq!(handle.durable_lsn(), Lsn::ZERO);
}

#[test]
fn sync_through_makes_append_batch_visible_and_durable() {
    let test_dir = TestDir::new("sync-through-batch");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let batch = [
        (RecordType::new(record_types::USER_MIN), &b"alpha"[..]),
        (RecordType::new(record_types::USER_MIN + 1), &b"beta"[..]),
    ];

    let lsns = handle.append_batch(&batch).unwrap();
    let end_lsn =
        Lsn::new((RECORD_HEADER_LEN + b"alpha".len() + RECORD_HEADER_LEN + b"beta".len()) as u64);

    handle.sync_through(end_lsn).unwrap();

    assert_eq!(handle.durable_lsn(), end_lsn);
    assert_eq!(handle.read_at(lsns[0]).unwrap().payload, b"alpha");
    assert_eq!(handle.read_at(lsns[1]).unwrap().payload, b"beta");
}

#[test]
fn sync_through_makes_committed_reservation_durable() {
    let test_dir = TestDir::new("sync-through-reservation");
    let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());
    let mut wal = Wal::open(directory.clone(), test_dir.config(), ())
        .unwrap()
        .0;

    let end_lsn = {
        let mut reservation = wal.reserve(2, 10).unwrap();
        reservation
            .append(RecordType::new(record_types::USER_MIN), b"ab")
            .unwrap();
        reservation
            .append(RecordType::new(record_types::USER_MIN + 1), b"cdef")
            .unwrap();
        reservation.commit().unwrap();
        wal.next_lsn()
    };

    let handle = WalHandle::new(wal);

    handle.sync_through(end_lsn).unwrap();

    assert_eq!(handle.durable_lsn(), end_lsn);
    assert_eq!(handle.read_at(Lsn::ZERO).unwrap().payload, b"ab");
    assert_eq!(handle.read_at(Lsn::new(34)).unwrap().payload, b"cdef");
}

#[test]
fn tail_from_reads_existing_and_future_records() {
    let test_dir = TestDir::new("tail-existing-future");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let first = handle
        .append(RecordType::new(record_types::USER_MIN), b"first")
        .unwrap();

    let mut tail = handle.tail_from(first).unwrap();

    let first_seen = tail.next_nonblocking().unwrap().unwrap();
    assert_eq!(first_seen.payload, b"first");

    assert!(tail.next_nonblocking().unwrap().is_none());

    handle
        .append(RecordType::new(record_types::USER_MIN + 1), b"second")
        .unwrap();

    let second_seen = tail.next_blocking(Duration::from_secs(1)).unwrap().unwrap();

    assert_eq!(second_seen.payload, b"second");
}

#[test]
fn tail_from_timeout_returns_none_at_live_tip() {
    let test_dir = TestDir::new("tail-timeout");
    let (handle, _) = WalHandle::open(
        FsSegmentDirectory::new(test_dir.path().to_path_buf()),
        test_dir.config(),
        (),
    )
    .unwrap();

    let mut tail = handle.tail_from(Lsn::ZERO).unwrap();

    assert!(
        tail.next_blocking(Duration::from_millis(10))
            .unwrap()
            .is_none()
    );
}
