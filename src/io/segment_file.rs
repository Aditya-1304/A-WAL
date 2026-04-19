#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

pub trait SegmentFile {
    fn len(&self) -> io::Result<u64>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn append_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
    fn truncate(&mut self, len: u64) -> io::Result<()>;
    fn advise_sequential(&self) -> io::Result<()>;
    fn prefetch(&self, offset: u64, len: u64) -> io::Result<()>;
}

#[derive(Debug)]
pub struct FsSegmentFile {
    file: File,
}

impl FsSegmentFile {
    pub fn from_file(file: File) -> Self {
        Self { file }
    }

    pub fn open_append(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self::from_file(file))
    }
}

impl SegmentFile for FsSegmentFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read_at(buf, offset)
    }

    fn append_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn advise_sequential(&self) -> io::Result<()> {
        Ok(())
    }

    fn prefetch(&self, _offset: u64, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    struct TestFile {
        path: PathBuf,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos();

            let path = std::env::temp_dir()
                .join(format!("wal-segment-file-{name}-{}-{nanos}", process::id()));

            Self { path }
        }

        fn open(&self) -> FsSegmentFile {
            FsSegmentFile::open_append(&self.path).expect("failed to open test file")
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn append_all_grows_file_len() {
        let test_file = TestFile::new("append-len");
        let mut file = test_file.open();

        file.append_all(b"hello").unwrap();

        assert_eq!(file.len().unwrap(), 5);
    }

    #[test]
    fn read_at_reads_from_exact_offset() {
        let test_file = TestFile::new("read-at");
        let mut file = test_file.open();

        file.append_all(b"abcdef").unwrap();

        let mut buf = [0u8; 3];
        let n = file.read_at(2, &mut buf).unwrap();

        assert_eq!(n, 3);
        assert_eq!(&buf, b"cde");
    }

    #[test]
    fn read_at_near_eof_can_return_partial_read() {
        let test_file = TestFile::new("partial-read");
        let mut file = test_file.open();

        file.append_all(b"abc").unwrap();

        let mut buf = [0u8; 4];
        let n = file.read_at(2, &mut buf).unwrap();

        assert_eq!(n, 1);
        assert_eq!(buf[0], b'c');
    }

    #[test]
    fn append_after_read_at_still_appends_to_tail() {
        let test_file = TestFile::new("append-after-read");
        let mut file = test_file.open();

        file.append_all(b"abc").unwrap();

        let mut scratch = [0u8; 2];
        let _ = file.read_at(0, &mut scratch).unwrap();

        file.append_all(b"def").unwrap();

        let mut buf = [0u8; 6];
        let n = file.read_at(0, &mut buf).unwrap();

        assert_eq!(n, 6);
        assert_eq!(&buf, b"abcdef");
    }

    #[test]
    fn truncate_shrinks_file() {
        let test_file = TestFile::new("truncate");
        let mut file = test_file.open();

        file.append_all(b"abcdef").unwrap();
        file.truncate(3).unwrap();

        assert_eq!(file.len().unwrap(), 3);

        let mut buf = [0u8; 3];
        let n = file.read_at(0, &mut buf).unwrap();

        assert_eq!(n, 3);
        assert_eq!(&buf, b"abc");
    }

    #[test]
    fn flush_and_sync_succeed_on_normal_file() {
        let test_file = TestFile::new("flush-sync");
        let mut file = test_file.open();

        file.append_all(b"hello").unwrap();
        file.flush().unwrap();
        file.sync().unwrap();
    }

    #[test]
    fn advisory_methods_are_noops_for_now() {
        let test_file = TestFile::new("advisory");
        let file = test_file.open();

        file.advise_sequential().unwrap();
        file.prefetch(0, 4096).unwrap();
    }
}
