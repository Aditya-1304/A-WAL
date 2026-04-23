use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    io::{
        directory::{NewSegment, SegmentDirectory, SegmentMeta, format_segment_filename},
        segment_file::SegmentFile,
    },
    lsn::Lsn,
    types::SegmentId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CrashPlan {
    pub keep_flushed_prefix: usize,
}

impl CrashPlan {
    pub const fn drop_all_unsynced() -> Self {
        Self {
            keep_flushed_prefix: 0,
        }
    }

    pub const fn keep_flushed_prefix(bytes: usize) -> Self {
        Self {
            keep_flushed_prefix: bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FaultDirectory {
    dir: PathBuf,
    inner: Arc<Mutex<FaultDirectoryInner>>,
}

#[derive(Debug, Default)]
struct FaultDirectoryInner {
    segments: BTreeMap<SegmentId, FaultSegmentMeta>,
    pending_partial_append: BTreeMap<SegmentId, usize>,
    pending_flush_failure: BTreeSet<SegmentId>,
    pending_sync_failure: BTreeSet<SegmentId>,
}

#[derive(Debug, Clone)]
struct FaultSegmentMeta {
    base_lsn: Lsn,
    state: Arc<Mutex<FaultSegmentState>>,
}

#[derive(Debug, Default)]
struct FaultSegmentState {
    bytes: Vec<u8>,
    flushed_len: usize,
    durable_len: usize,
    next_partial_append: Option<usize>,
    fail_flush_once: bool,
    fail_sync_once: bool,
}

#[derive(Debug, Clone)]
pub struct FaultSegmentFile {
    state: Arc<Mutex<FaultSegmentState>>,
}

impl FaultDirectory {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            inner: Arc::new(Mutex::new(FaultDirectoryInner::default())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    pub fn inject_partial_append(
        &self,
        segment_id: SegmentId,
        keep_bytes: usize,
    ) -> io::Result<()> {
        let state = {
            let mut inner = self.lock_inner();
            match inner.segments.get(&segment_id) {
                Some(meta) => Some(meta.state.clone()),
                None => {
                    inner.pending_partial_append.insert(segment_id, keep_bytes);
                    None
                }
            }
        };

        if let Some(state) = state {
            state.lock().unwrap().next_partial_append = Some(keep_bytes);
        }

        Ok(())
    }

    pub fn inject_flush_error(&self, segment_id: SegmentId) -> io::Result<()> {
        let state = {
            let mut inner = self.lock_inner();
            match inner.segments.get(&segment_id) {
                Some(meta) => Some(meta.state.clone()),
                None => {
                    inner.pending_flush_failure.insert(segment_id);
                    None
                }
            }
        };

        if let Some(state) = state {
            state.lock().unwrap().fail_flush_once = true;
        }

        Ok(())
    }

    pub fn inject_sync_error(&self, segment_id: SegmentId) -> io::Result<()> {
        let state = {
            let mut inner = self.lock_inner();
            match inner.segments.get(&segment_id) {
                Some(meta) => Some(meta.state.clone()),
                None => {
                    inner.pending_sync_failure.insert(segment_id);
                    None
                }
            }
        };

        if let Some(state) = state {
            state.lock().unwrap().fail_sync_once = true;
        }

        Ok(())
    }

    pub fn flip_byte(&self, segment_id: SegmentId, offset: usize) -> io::Result<()> {
        let state = self.segment_state(segment_id)?;
        let mut state = state.lock().unwrap();

        if offset >= state.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "cannot flip byte at offset {offset}; segment {segment_id} len is {}",
                    state.bytes.len()
                ),
            ));
        }

        state.bytes[offset] ^= 0xFF;
        Ok(())
    }

    pub fn segment_bytes(&self, segment_id: SegmentId) -> io::Result<Vec<u8>> {
        let state = self.segment_state(segment_id)?;
        Ok(state.lock().unwrap().bytes.clone())
    }

    pub fn crash_reset(&self, plan: CrashPlan) {
        let states = {
            let inner = self.lock_inner();
            inner
                .segments
                .values()
                .map(|meta| meta.state.clone())
                .collect::<Vec<_>>()
        };

        for state in states {
            let mut state = state.lock().unwrap();
            let durable_len = state.durable_len.min(state.bytes.len());
            let flushed_len = state.flushed_len.min(state.bytes.len());
            let unsynced_len = flushed_len.saturating_sub(durable_len);
            let keep_unsynced = unsynced_len.min(plan.keep_flushed_prefix);
            let new_len = durable_len + keep_unsynced;

            state.bytes.truncate(new_len);
            state.flushed_len = new_len;
            state.durable_len = durable_len.min(new_len);
            state.next_partial_append = None;
            state.fail_flush_once = false;
            state.fail_sync_once = false;
        }
    }

    fn segment_state(&self, segment_id: SegmentId) -> io::Result<Arc<Mutex<FaultSegmentState>>> {
        let inner = self.lock_inner();
        inner
            .segments
            .get(&segment_id)
            .map(|meta| meta.state.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment {segment_id} was not found"),
                )
            })
    }

    fn lock_inner(&self) -> MutexGuard<'_, FaultDirectoryInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl SegmentDirectory for FaultDirectory {
    type File = FaultSegmentFile;

    fn list_segments(&self) -> io::Result<Vec<SegmentMeta>> {
        let inner = self.lock_inner();
        let mut metas = inner
            .segments
            .iter()
            .map(|(&segment_id, meta)| SegmentMeta {
                segment_id,
                base_lsn: meta.base_lsn,
                path: self
                    .dir
                    .join(format_segment_filename(segment_id, meta.base_lsn)),
            })
            .collect::<Vec<_>>();

        metas.sort_by_key(|meta| (meta.base_lsn, meta.segment_id));
        Ok(metas)
    }

    fn create_segment(&self, spec: NewSegment) -> io::Result<Self::File> {
        let mut inner = self.lock_inner();

        if inner.segments.contains_key(&spec.segment_id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("segment {} already exists", spec.segment_id),
            ));
        }

        if spec.header.segment_id != spec.segment_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment header segment_id does not match create spec",
            ));
        }

        if spec.header.base_lsn != spec.base_lsn {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment header base_lsn does not match create spec",
            ));
        }

        let header_bytes = spec.header.encode();
        let mut state = FaultSegmentState {
            bytes: header_bytes.clone(),
            flushed_len: header_bytes.len(),
            durable_len: header_bytes.len(),
            ..FaultSegmentState::default()
        };

        if let Some(keep_bytes) = inner.pending_partial_append.remove(&spec.segment_id) {
            state.next_partial_append = Some(keep_bytes);
        }

        if inner.pending_flush_failure.remove(&spec.segment_id) {
            state.fail_flush_once = true;
        }

        if inner.pending_sync_failure.remove(&spec.segment_id) {
            state.fail_sync_once = true;
        }

        let state = Arc::new(Mutex::new(state));
        inner.segments.insert(
            spec.segment_id,
            FaultSegmentMeta {
                base_lsn: spec.base_lsn,
                state: state.clone(),
            },
        );

        Ok(FaultSegmentFile { state })
    }

    fn open_segment(&self, id: SegmentId) -> io::Result<Self::File> {
        Ok(FaultSegmentFile {
            state: self.segment_state(id)?,
        })
    }

    fn remove_segment(&self, id: SegmentId) -> io::Result<()> {
        let removed = self.lock_inner().segments.remove(&id);
        if removed.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment {id} was not found"),
            ));
        }

        Ok(())
    }

    fn recycle_sgement(&self, old_id: SegmentId, new_spec: NewSegment) -> io::Result<Self::File> {
        self.remove_segment(old_id)?;
        self.create_segment(new_spec)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

impl SegmentFile for FaultSegmentFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.state.lock().unwrap().bytes.len() as u64)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let state = self.state.lock().unwrap();
        let start = offset as usize;

        if start >= state.bytes.len() {
            return Ok(0);
        }

        let end = (start + buf.len()).min(state.bytes.len());
        let len = end - start;
        buf[..len].copy_from_slice(&state.bytes[start..end]);
        Ok(len)
    }

    fn append_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();

        if let Some(keep_bytes) = state.next_partial_append.take() {
            let keep = keep_bytes.min(buf.len());
            state.bytes.extend_from_slice(&buf[..keep]);

            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("injected short write after {keep} bytes"),
            ));
        }

        state.bytes.extend_from_slice(buf);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();

        if state.fail_flush_once {
            state.fail_flush_once = false;
            return Err(io::Error::other("injected flush failure"));
        }

        state.flushed_len = state.bytes.len();
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();

        if state.fail_sync_once {
            state.fail_sync_once = false;
            return Err(io::Error::other("injected sync failure"));
        }

        state.durable_len = state.flushed_len.min(state.bytes.len());
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        let len = len as usize;

        state.bytes.truncate(len);
        state.flushed_len = state.flushed_len.min(len);
        state.durable_len = state.durable_len.min(len);
        Ok(())
    }

    fn advise_sequential(&self) -> io::Result<()> {
        Ok(())
    }

    fn prefetch(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}
