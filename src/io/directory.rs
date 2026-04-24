use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    process::{self},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    error::WalError,
    format::segment_header::SegmentHeader,
    io::segment_file::{FsSegmentFile, SegmentFile},
    lsn::Lsn,
    types::SegmentId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub base_lsn: Lsn,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct NewSegment {
    pub segment_id: SegmentId,
    pub base_lsn: Lsn,
    pub header: SegmentHeader,
}

pub trait SegmentDirectory {
    type File: SegmentFile;

    fn list_segments(&self) -> io::Result<Vec<SegmentMeta>>;
    fn create_segment(&self, spec: NewSegment) -> io::Result<Self::File>;
    fn open_segment(&self, id: SegmentId) -> io::Result<Self::File>;
    fn remove_segment(&self, ig: SegmentId) -> io::Result<()>;

    fn open_segment_meta(&self, meta: &SegmentMeta) -> io::Result<Self::File> {
        self.open_segment(meta.segment_id)
    }

    fn remove_segments(&self, ids: &[SegmentId]) -> io::Result<usize> {
        for &id in ids {
            self.remove_segment(id)?;
        }

        Ok(ids.len())
    }

    fn recycle_sgement(&self, old_id: SegmentId, new_spec: NewSegment) -> io::Result<Self::File>;
    fn sync_directory(&self) -> io::Result<()>;
}

#[derive(Debug, Clone)]
pub struct FsSegmentDirectory {
    dir: PathBuf,
}

impl FsSegmentDirectory {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    fn canonical_segement_path(&self, segment_id: SegmentId, base_lsn: Lsn) -> PathBuf {
        self.dir.join(format_segment_filename(segment_id, base_lsn))
    }

    fn temporary_segment_path(&self, segment_id: SegmentId) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();

        self.dir.join(format!(
            ".tmp-segment-{segment_id:020}-{}-{nanos}.tmp",
            process::id()
        ))
    }
}

impl SegmentDirectory for FsSegmentDirectory {
    type File = FsSegmentFile;

    fn list_segments(&self) -> io::Result<Vec<SegmentMeta>> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if !file_type.is_file() {
                continue;
            }

            if let Some((segment_id, base_lsn)) = parse_segment_filename(&entry.path())? {
                segments.push(SegmentMeta {
                    segment_id,
                    base_lsn,
                    path: entry.path(),
                });
            }
        }

        segments.sort_by_key(|meta| (meta.base_lsn, meta.segment_id));
        Ok(segments)
    }

    fn create_segment(&self, spec: NewSegment) -> io::Result<Self::File> {
        let canonical_path = self.canonical_segement_path(spec.segment_id, spec.base_lsn);

        if canonical_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "segment {} already exists at {}",
                    spec.segment_id,
                    canonical_path.display()
                ),
            ));
        }

        if spec.header.segment_id != spec.segment_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "new segment header segment_id {} does not match spec segment_id{}",
                    spec.header.segment_id, spec.segment_id
                ),
            ));
        }

        if spec.header.base_lsn != spec.base_lsn {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "new segment header base_lsn {} does not match spec base_lsn {}",
                    spec.header.base_lsn.as_u64(),
                    spec.base_lsn.as_u64()
                ),
            ));
        }

        let mut header = spec.header.clone();
        header.validate().map_err(wal_error_to_invalid_data)?;
        header.finalize_checksum();

        let temp_path = self.temporary_segment_path(spec.segment_id);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&temp_path)?;

        let mut segment_file = FsSegmentFile::from_file(file);
        let header_bytes = header.encode();

        segment_file.append_all(&header_bytes)?;
        segment_file.flush()?;
        segment_file.sync()?;

        fs::rename(&temp_path, &canonical_path)?;
        self.sync_directory()?;

        Ok(segment_file)
    }

    fn open_segment(&self, id: SegmentId) -> io::Result<Self::File> {
        let mut matches = self
            .list_segments()?
            .into_iter()
            .filter(|meta| meta.segment_id == id);

        let meta = matches.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment {id} was not found"),
            )
        })?;

        if matches.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("multiple sehments found for segment id {id}"),
            ));
        }

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&meta.path)?;
        Ok(FsSegmentFile::from_file(file))
    }

    fn open_segment_meta(&self, meta: &SegmentMeta) -> io::Result<Self::File> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&meta.path)?;

        Ok(FsSegmentFile::from_file(file))
    }

    fn remove_segment(&self, ig: SegmentId) -> io::Result<()> {
        let mut matches = self
            .list_segments()?
            .into_iter()
            .filter(|meta| meta.segment_id == ig);

        let meta = matches.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("segment {ig} was not found"),
            )
        })?;

        if matches.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("multiple segments found for segment id {ig}"),
            ));
        }

        fs::remove_file(&meta.path)?;
        self.sync_directory()?;
        Ok(())
    }

    fn remove_segments(&self, ids: &[SegmentId]) -> io::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        let mut requested = BTreeSet::new();
        for &id in ids {
            if !requested.insert(id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate segment id {id} in remove request"),
                ));
            }
        }

        let mut segments_by_id = BTreeMap::new();
        for meta in self.list_segments()? {
            if segments_by_id.insert(meta.segment_id, meta.path).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("multiple segments found for segment id {}", meta.segment_id),
                ));
            }
        }

        let mut paths = Vec::with_capacity(ids.len());
        for &id in ids {
            let path = segments_by_id.get(&id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment {id} was not found"),
                )
            })?;

            paths.push(path.clone());
        }

        for path in paths {
            fs::remove_file(path)?;
        }

        self.sync_directory()?;
        Ok(ids.len())
    }

    fn recycle_sgement(&self, old_id: SegmentId, new_spec: NewSegment) -> io::Result<Self::File> {
        self.remove_segment(old_id)?;
        self.create_segment(new_spec)
    }

    fn sync_directory(&self) -> io::Result<()> {
        let dir = File::open(&self.dir)?;
        dir.sync_all()
    }
}

pub fn format_segment_filename(segment_id: SegmentId, base_lsn: Lsn) -> String {
    format!("{segment_id:020}_{:020}.wal", base_lsn.as_u64())
}

fn parse_segment_filename(path: &Path) -> io::Result<Option<(SegmentId, Lsn)>> {
    let file_name = match path.file_name() {
        Some(name) => name,
        None => return Ok(None),
    };

    if path.extension() != Some(OsStr::new("wal")) {
        return Ok(None);
    }

    let file_name = file_name.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("non-utf8 wal filename: {:?}", file_name),
        )
    })?;

    let stem = file_name.strip_suffix(".wal").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal filename is missing .wal suffix: {file_name}"),
        )
    })?;

    let mut parts = stem.split('_');
    let segment_part = parts.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("malformed segment filename: {file_name}"),
        )
    })?;
    let base_lsn_part = parts.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("malformed segment filename: {file_name}"),
        )
    })?;

    if parts.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("malformed segment filename: {file_name}"),
        ));
    }

    if segment_part.len() != 20 || base_lsn_part.len() != 20 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("non-canonical segment filename: {file_name}"),
        ));
    }

    if !segment_part.chars().all(|c| c.is_ascii_digit())
        || !base_lsn_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("non-numeric segment filename: {file_name}"),
        ));
    }

    let segment_id = segment_part.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid segment id in filename: {file_name}"),
        )
    })?;

    let base_lsn = base_lsn_part.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid base lsn in filename: {file_name}"),
        )
    })?;

    Ok(Some((segment_id, Lsn::new(base_lsn))))
}

fn wal_error_to_invalid_data(err: WalError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        format::segment_header::{compression_algorithms, SegmentHeader},
        io::segment_file::SegmentFile,
        types::WalIdentity,
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

            let path = std::env::temp_dir()
                .join(format!("wal-directory-{prefix}-{}-{nanos}", process::id()));

            fs::create_dir_all(&path).expect("failed to create test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn sample_new_segment(segment_id: SegmentId, base_lsn: Lsn) -> NewSegment {
        let header = SegmentHeader::new(
            WalIdentity::new(11, 22, 1),
            segment_id,
            base_lsn,
            compression_algorithms::NONE,
            SegmentHeader::SUPPORTED_VERSION,
        );

        NewSegment {
            segment_id,
            base_lsn,
            header,
        }
    }

    #[test]
    fn format_segment_filename_matches_spec() {
        let name = format_segment_filename(1, Lsn::new(65536));
        assert_eq!(name, "00000000000000000001_00000000000000065536.wal");
    }

    #[test]
    fn parse_segment_filename_round_trips_canonical_name() {
        let parsed =
            parse_segment_filename(Path::new("00000000000000000001_00000000000000065536.wal"))
                .unwrap();

        assert_eq!(parsed, Some((1, Lsn::new(65536))));
    }

    #[test]
    fn parse_segment_filename_ignores_non_wal_files() {
        let parsed = parse_segment_filename(Path::new("notes.txt")).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_segment_filename_rejects_non_canonical_wal_names() {
        let err = parse_segment_filename(Path::new("1_2.wal")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn list_segments_returns_sorted_segment_metadata() {
        let test_dir = TestDir::new("list-sorted");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        directory
            .create_segment(sample_new_segment(2, Lsn::new(65536)))
            .unwrap();
        directory
            .create_segment(sample_new_segment(1, Lsn::new(0)))
            .unwrap();

        let segments = directory.list_segments().unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].segment_id, 1);
        assert_eq!(segments[0].base_lsn, Lsn::new(0));
        assert_eq!(segments[1].segment_id, 2);
        assert_eq!(segments[1].base_lsn, Lsn::new(65536));
    }

    #[test]
    fn list_segments_ignores_non_segment_junk_files() {
        let test_dir = TestDir::new("ignore-junk");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        fs::write(test_dir.path().join("notes.txt"), b"hello").unwrap();
        fs::write(test_dir.path().join(".tmp-segment-file.tmp"), b"hello").unwrap();

        let segments = directory.list_segments().unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn create_segment_publishes_canonical_file_with_header_bytes() {
        let test_dir = TestDir::new("create");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let file = directory
            .create_segment(sample_new_segment(7, Lsn::new(4096)))
            .unwrap();

        let canonical_path = test_dir
            .path()
            .join(format_segment_filename(7, Lsn::new(4096)));

        assert!(canonical_path.exists());

        let entries = fs::read_dir(test_dir.path()).unwrap().count();
        assert_eq!(entries, 1);

        let mut buf = [0u8; SegmentHeader::ENCODED_LEN];
        let n = file.read_at(0, &mut buf).unwrap();

        assert_eq!(n, SegmentHeader::ENCODED_LEN);

        let decoded = SegmentHeader::decode(&buf).unwrap();
        assert_eq!(decoded.segment_id, 7);
        assert_eq!(decoded.base_lsn, Lsn::new(4096));
        assert_eq!(decoded.identity(), WalIdentity::new(11, 22, 1));
    }

    #[test]
    fn create_segment_rejects_header_spec_mismatch() {
        let test_dir = TestDir::new("mismatch");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        let mut spec = sample_new_segment(1, Lsn::new(0));
        spec.header.segment_id = 99;

        let err = directory.create_segment(spec).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn open_segment_reopens_existing_segment() {
        let test_dir = TestDir::new("open");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        directory
            .create_segment(sample_new_segment(3, Lsn::new(8192)))
            .unwrap();

        let file = directory.open_segment(3).unwrap();

        let mut buf = [0u8; SegmentHeader::ENCODED_LEN];
        let n = file.read_at(0, &mut buf).unwrap();

        assert_eq!(n, SegmentHeader::ENCODED_LEN);

        let decoded = SegmentHeader::decode(&buf).unwrap();
        assert_eq!(decoded.segment_id, 3);
        assert_eq!(decoded.base_lsn, Lsn::new(8192));
    }

    #[test]
    fn remove_segment_unlinks_canonical_file() {
        let test_dir = TestDir::new("remove");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        directory
            .create_segment(sample_new_segment(5, Lsn::new(0)))
            .unwrap();

        let canonical_path = test_dir
            .path()
            .join(format_segment_filename(5, Lsn::new(0)));
        assert!(canonical_path.exists());

        directory.remove_segment(5).unwrap();

        assert!(!canonical_path.exists());
        assert!(directory.list_segments().unwrap().is_empty());
    }

    #[test]
    fn remove_segments_unlinks_multiple_files_with_one_call() {
        let test_dir = TestDir::new("remove-many");
        let directory = FsSegmentDirectory::new(test_dir.path().to_path_buf());

        directory
            .create_segment(sample_new_segment(1, Lsn::new(0)))
            .unwrap();
        directory
            .create_segment(sample_new_segment(2, Lsn::new(100)))
            .unwrap();
        directory
            .create_segment(sample_new_segment(3, Lsn::new(200)))
            .unwrap();

        let first_path = test_dir
            .path()
            .join(format_segment_filename(1, Lsn::new(0)));
        let second_path = test_dir
            .path()
            .join(format_segment_filename(2, Lsn::new(100)));
        let third_path = test_dir
            .path()
            .join(format_segment_filename(3, Lsn::new(200)));

        let removed = directory.remove_segments(&[1, 2]).unwrap();

        assert_eq!(removed, 2);
        assert!(!first_path.exists());
        assert!(!second_path.exists());
        assert!(third_path.exists());

        let metas = directory.list_segments().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].segment_id, 3);
    }
}
