use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;

pub const DEFAULT_STORAGE_WRITE_UNIT: u32 = 4096;
pub const MIN_STORAGE_WRITE_UNIT: u32 = 512;
pub const MAX_STORAGE_WRITE_UNIT: u32 = 1024 * 1024;

#[cfg(target_os = "linux")]
pub fn detect_storage_write_unit(path: &Path) -> io::Result<u32> {
    let metadata = fs::metadata(path)?;
    let dev = metadata.dev();

    let major = libc::major(dev);
    let minor = libc::minor(dev);

    let queue_dir = queue_dir_for_device(major, minor);
    detect_storage_write_unit_from_queue_dir(&queue_dir)
}

#[cfg(not(target_os = "linux"))]
pub fn detect_storage_write_unit(path: &Path) -> io::Result<u32> {
    let _ = fs::metadata(path)?;
    Ok(DEFAULT_STORAGE_WRITE_UNIT)
}

#[cfg(target_os = "linux")]
fn queue_dir_for_device(major: u32, minor: u32) -> PathBuf {
    PathBuf::from(format!("/sys/dev/block/{major}:{minor}/queue"))
}

fn detect_storage_write_unit_from_queue_dir(queue_dir: &Path) -> io::Result<u32> {
    for name in [
        "minimum_io_size",
        "physical_block_size",
        "logical_block_size",
    ] {
        let candidate = queue_dir.join(name);

        if let Some(value) = read_valid_u32(&candidate)? {
            return Ok(value);
        }
    }

    Ok(DEFAULT_STORAGE_WRITE_UNIT)
}

fn read_valid_u32(path: &Path) -> io::Result<Option<u32>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(_) => return Ok(None),
    };

    let value = match contents.trim().parse::<u32>() {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    if is_valid_storage_write_unit(value) {
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn is_valid_storage_write_unit(value: u32) -> bool {
    value >= MIN_STORAGE_WRITE_UNIT && value <= MAX_STORAGE_WRITE_UNIT && value.is_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_nanos();

            let path = std::env::temp_dir().join(format!("wal-{prefix}-{}-{nanos}", process::id()));

            fs::create_dir_all(&path).expect("failed to create test dir");
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

    fn write_queue_value(dir: &Path, name: &str, value: &str) {
        fs::write(dir.join(name), value).expect("failed to write queue value");
    }

    #[test]
    fn read_valid_u32_accepts_power_of_two_in_range() {
        let dir = TestDir::new("read-valid");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "4096\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), Some(4096));
    }

    #[test]
    fn read_valid_u32_rejects_missing_file() {
        let dir = TestDir::new("read-missing");
        let file = dir.path().join("missing");

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn read_valid_u32_rejects_parse_failure() {
        let dir = TestDir::new("read-parse");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "not-a-number\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn read_valid_u32_rejects_zero() {
        let dir = TestDir::new("read-zero");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "0\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn read_valid_u32_rejects_too_small_value() {
        let dir = TestDir::new("read-small");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "256\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn read_valid_u32_rejects_non_power_of_two() {
        let dir = TestDir::new("read-not-power-two");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "3000\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn read_valid_u32_rejects_too_large_value() {
        let dir = TestDir::new("read-too-large");
        let file = dir.path().join("minimum_io_size");

        fs::write(&file, "2097152\n").unwrap();

        assert_eq!(read_valid_u32(&file).unwrap(), None);
    }

    #[test]
    fn detection_prefers_minimum_io_size_first() {
        let dir = TestDir::new("prefer-minimum");

        write_queue_value(dir.path(), "minimum_io_size", "8192\n");
        write_queue_value(dir.path(), "physical_block_size", "4096\n");
        write_queue_value(dir.path(), "logical_block_size", "512\n");

        assert_eq!(
            detect_storage_write_unit_from_queue_dir(dir.path()).unwrap(),
            8192
        );
    }

    #[test]
    fn detection_falls_back_to_physical_block_size() {
        let dir = TestDir::new("fallback-physical");

        write_queue_value(dir.path(), "minimum_io_size", "0\n");
        write_queue_value(dir.path(), "physical_block_size", "4096\n");
        write_queue_value(dir.path(), "logical_block_size", "512\n");

        assert_eq!(
            detect_storage_write_unit_from_queue_dir(dir.path()).unwrap(),
            4096
        );
    }

    #[test]
    fn detection_falls_back_to_logical_block_size() {
        let dir = TestDir::new("fallback-logical");

        write_queue_value(dir.path(), "minimum_io_size", "3000\n");
        write_queue_value(dir.path(), "physical_block_size", "2097152\n");
        write_queue_value(dir.path(), "logical_block_size", "512\n");

        assert_eq!(
            detect_storage_write_unit_from_queue_dir(dir.path()).unwrap(),
            512
        );
    }

    #[test]
    fn detection_returns_default_when_queue_metadata_is_missing() {
        let dir = TestDir::new("missing-queue-metadata");

        assert_eq!(
            detect_storage_write_unit_from_queue_dir(dir.path()).unwrap(),
            DEFAULT_STORAGE_WRITE_UNIT
        );
    }

    #[test]
    fn detection_returns_default_when_all_candidates_are_invalid() {
        let dir = TestDir::new("all-invalid");

        write_queue_value(dir.path(), "minimum_io_size", "0\n");
        write_queue_value(dir.path(), "physical_block_size", "1536\n");
        write_queue_value(dir.path(), "logical_block_size", "2097152\n");

        assert_eq!(
            detect_storage_write_unit_from_queue_dir(dir.path()).unwrap(),
            DEFAULT_STORAGE_WRITE_UNIT
        );
    }

    #[test]
    fn public_detection_returns_error_when_input_path_metadata_fails() {
        let missing = Path::new("/definitely-not-a-real-wal-path-for-tests");

        let err = detect_storage_write_unit(missing).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
