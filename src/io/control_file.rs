use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    error::WalError,
    format::codec::{
        copy_with_zeroed_range, crc32c, is_all_zero, put_bytes, put_u8, put_u16_le, put_u32_le,
        put_u64_le, read_array, read_u8, read_u16_le, read_u32_le, read_u64_le,
    },
    lsn::Lsn,
    types::WalIdentity,
};

pub const CONTROL_FILE_NAME: &str = "wal.control";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlFile {
    pub magic: u32,
    pub version: u16,
    pub header_len: u16,
    pub system_id: u64,
    pub wal_incarnation: u64,
    pub timeline_id: u64,
    pub last_checkpoint_lsn: Option<Lsn>,
    pub checkpoint_no: u64,
    pub clean_shutdown: bool,
    pub checksum: u32,
    pub reserved: [u8; 15],
}

impl ControlFile {
    pub const MAGIC: u32 = 0x5741_4C43;
    pub const SUPPORTED_VERSION: u16 = 1;
    pub const ENCODED_LEN: usize = 68;

    const CHECKSUM_FIELD_START: usize = 49;
    const CHECKSUM_FIELD_END: usize = 53;
    const NO_CHECKPOINT_LSN: u64 = u64::MAX;

    pub fn new(
        identity: WalIdentity,
        last_checkpoint_lsn: Option<Lsn>,
        checkpoint_no: u64,
        clean_shutdown: bool,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::SUPPORTED_VERSION,
            header_len: Self::ENCODED_LEN as u16,
            system_id: identity.system_id,
            wal_incarnation: identity.wal_incarnation,
            timeline_id: identity.timeline_id,
            last_checkpoint_lsn,
            checkpoint_no,
            clean_shutdown,
            checksum: 0,
            reserved: [0; 15],
        }
    }

    pub fn identity(&self) -> WalIdentity {
        WalIdentity::new(self.system_id, self.wal_incarnation, self.timeline_id)
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_inner(self.checksum)
    }

    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.encode_inner(0);
        crc32c(&bytes)
    }

    pub fn finalize_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        if bytes.len() < Self::ENCODED_LEN {
            return Err(WalError::ShortRead);
        }

        if bytes.len() != Self::ENCODED_LEN {
            return Err(WalError::BadSegmentHeader);
        }

        let mut offset = 0;

        let magic = read_u32_le(bytes, &mut offset)?;
        let version = read_u16_le(bytes, &mut offset)?;
        let header_len = read_u16_le(bytes, &mut offset)?;
        let system_id = read_u64_le(bytes, &mut offset)?;
        let wal_incarnation = read_u64_le(bytes, &mut offset)?;
        let timeline_id = read_u64_le(bytes, &mut offset)?;
        let last_checkpoint_raw = read_u64_le(bytes, &mut offset)?;
        let checkpoint_no = read_u64_le(bytes, &mut offset)?;
        let clean_shutdown_raw = read_u8(bytes, &mut offset)?;
        let checksum = read_u32_le(bytes, &mut offset)?;
        let reserved = read_array::<15>(bytes, &mut offset)?;

        if offset != bytes.len() {
            return Err(WalError::BadSegmentHeader);
        }

        let clean_shutdown = match clean_shutdown_raw {
            0 => false,
            1 => true,
            _ => return Err(WalError::BadSegmentHeader),
        };

        let last_checkpoint_lsn = if last_checkpoint_raw == Self::NO_CHECKPOINT_LSN {
            None
        } else {
            Some(Lsn::new(last_checkpoint_raw))
        };

        let control = Self {
            magic,
            version,
            header_len,
            system_id,
            wal_incarnation,
            timeline_id,
            last_checkpoint_lsn,
            checkpoint_no,
            clean_shutdown,
            checksum,
            reserved,
        };

        control.validate()?;
        control.verify_checksum(bytes)?;
        Ok(control)
    }

    pub fn validate(&self) -> Result<(), WalError> {
        if self.magic != Self::MAGIC {
            return Err(WalError::BadMagic { found: self.magic });
        }

        if self.version != Self::SUPPORTED_VERSION {
            return Err(WalError::UnsupportedVersion {
                found: self.version,
                expected: Self::SUPPORTED_VERSION,
            });
        }

        if self.header_len != Self::ENCODED_LEN as u16 {
            return Err(WalError::BadSegmentHeader);
        }

        if !is_all_zero(&self.reserved) {
            return Err(WalError::BadSegmentHeader);
        }

        if self.last_checkpoint_lsn.is_none() && self.checkpoint_no != 0 {
            return Err(WalError::BadSegmentHeader);
        }

        Ok(())
    }

    pub fn validate_identity(&self, expected_identity: WalIdentity) -> Result<(), WalError> {
        let found = self.identity();
        if found != expected_identity {
            return Err(WalError::IdentityMismatch {
                expected: expected_identity,
                found,
            });
        }

        Ok(())
    }

    pub fn verify_checksum(&self, encoded: &[u8]) -> Result<(), WalError> {
        if encoded.len() < Self::ENCODED_LEN {
            return Err(WalError::ShortRead);
        }

        if encoded.len() != Self::ENCODED_LEN {
            return Err(WalError::BadSegmentHeader);
        }

        let zeroed = copy_with_zeroed_range(
            encoded,
            Self::CHECKSUM_FIELD_START..Self::CHECKSUM_FIELD_END,
        );
        let expected = crc32c(&zeroed);

        if expected != self.checksum {
            return Err(WalError::ChecksumMismatch { lsn: None });
        }

        Ok(())
    }

    fn encode_inner(&self, checksum: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);

        put_u32_le(&mut buf, self.magic);
        put_u16_le(&mut buf, self.version);
        put_u16_le(&mut buf, self.header_len);
        put_u64_le(&mut buf, self.system_id);
        put_u64_le(&mut buf, self.wal_incarnation);
        put_u64_le(&mut buf, self.timeline_id);
        put_u64_le(
            &mut buf,
            self.last_checkpoint_lsn
                .map(Lsn::as_u64)
                .unwrap_or(Self::NO_CHECKPOINT_LSN),
        );
        put_u64_le(&mut buf, self.checkpoint_no);
        put_u8(&mut buf, u8::from(self.clean_shutdown));
        put_u32_le(&mut buf, checksum);
        put_bytes(&mut buf, &self.reserved);

        debug_assert_eq!(buf.len(), Self::ENCODED_LEN);
        buf
    }
}

#[derive(Debug, Clone)]
pub struct FsControlFileStore {
    dir: PathBuf,
}

impl FsControlFileStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(CONTROL_FILE_NAME)
    }

    pub fn read(&self) -> Result<Option<ControlFile>, WalError> {
        let bytes = match fs::read(self.path()) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        Ok(Some(ControlFile::decode(&bytes)?))
    }

    pub fn load_for_recovery(
        &self,
        expected_identity: WalIdentity,
    ) -> Result<Option<ControlFile>, WalError> {
        let control = match self.read() {
            Ok(control) => control,
            Err(
                WalError::BadMagic { .. }
                | WalError::UnsupportedVersion { .. }
                | WalError::BadSegmentHeader
                | WalError::ShortRead
                | WalError::ChecksumMismatch { .. },
            ) => return Ok(None),
            Err(err) => return Err(err),
        };

        let Some(control) = control else {
            return Ok(None);
        };

        control.validate_identity(expected_identity)?;
        Ok(Some(control))
    }

    pub fn publish(&self, control: &ControlFile) -> Result<(), WalError> {
        let mut control = control.clone();
        control.validate()?;
        control.finalize_checksum();

        let temp_path = self.temporary_path();
        let final_path = self.path();

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;

        file.write_all(&control.encode())?;
        file.flush()?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp_path, &final_path)?;
        self.sync_directory()?;

        Ok(())
    }

    pub fn sync_directory(&self) -> Result<(), WalError> {
        let dir = File::open(&self.dir)?;
        dir.sync_all()?;
        Ok(())
    }

    fn temporary_path(&self) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();

        self.dir.join(format!(
            ".tmp-{}-{}-{nanos}",
            CONTROL_FILE_NAME,
            process::id()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos();

            let path = std::env::temp_dir().join(format!(
                "wal-control-file-{prefix}-{}-{nanos}",
                process::id()
            ));

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

    fn sample_identity() -> WalIdentity {
        WalIdentity::new(11, 22, 1)
    }

    #[test]
    fn control_file_round_trips_through_encode_decode() {
        let mut control = ControlFile::new(sample_identity(), Some(Lsn::new(4096)), 7, true);
        control.finalize_checksum();

        let bytes = control.encode();
        let decoded = ControlFile::decode(&bytes).unwrap();

        assert_eq!(decoded, control);
    }

    #[test]
    fn control_file_round_trips_without_checkpoint() {
        let mut control = ControlFile::new(sample_identity(), None, 0, false);
        control.finalize_checksum();

        let bytes = control.encode();
        let decoded = ControlFile::decode(&bytes).unwrap();

        assert_eq!(decoded.last_checkpoint_lsn, None);
        assert_eq!(decoded.checkpoint_no, 0);
        assert!(!decoded.clean_shutdown);
    }

    #[test]
    fn decode_rejects_bad_checksum() {
        let mut control = ControlFile::new(sample_identity(), Some(Lsn::new(42)), 1, false);
        control.finalize_checksum();

        let mut bytes = control.encode();
        bytes[ControlFile::CHECKSUM_FIELD_START + 1] ^= 0xFF;

        let err = ControlFile::decode(&bytes).unwrap_err();

        assert!(matches!(err, WalError::ChecksumMismatch { lsn: None }));
    }

    #[test]
    fn validate_identity_rejects_mismatch() {
        let control = ControlFile::new(sample_identity(), None, 0, false);

        let err = control
            .validate_identity(WalIdentity::new(99, 22, 1))
            .unwrap_err();

        assert!(matches!(err, WalError::IdentityMismatch { .. }));
    }

    #[test]
    fn read_returns_none_when_control_file_is_missing() {
        let test_dir = TestDir::new("missing");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let control = store.read().unwrap();

        assert_eq!(control, None);
    }

    #[test]
    fn publish_then_read_round_trips_control_file() {
        let test_dir = TestDir::new("publish");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let control = ControlFile::new(sample_identity(), Some(Lsn::new(8192)), 3, true);
        store.publish(&control).unwrap();

        let decoded = store.read().unwrap().unwrap();

        assert_eq!(decoded.identity(), sample_identity());
        assert_eq!(decoded.last_checkpoint_lsn, Some(Lsn::new(8192)));
        assert_eq!(decoded.checkpoint_no, 3);
        assert!(decoded.clean_shutdown);
    }

    #[test]
    fn load_for_recovery_falls_back_to_none_on_corrupt_file() {
        let test_dir = TestDir::new("corrupt");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        fs::write(store.path(), b"not-a-valid-control-file").unwrap();

        let control = store.load_for_recovery(sample_identity()).unwrap();

        assert_eq!(control, None);
    }

    #[test]
    fn load_for_recovery_rejects_identity_mismatch() {
        let test_dir = TestDir::new("identity-mismatch");
        let store = FsControlFileStore::new(test_dir.path().to_path_buf());

        let control = ControlFile::new(WalIdentity::new(99, 88, 77), None, 0, false);
        store.publish(&control).unwrap();

        let err = store.load_for_recovery(sample_identity()).unwrap_err();

        assert!(matches!(err, WalError::IdentityMismatch { .. }));
    }
}
