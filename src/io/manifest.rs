//! Durable witness for the newest acknowledged A-WAL history.
//!
//! Segment directory enumeration can prove gaps only when a later segment is
//! still present. `wal.manifest` closes the remaining newest/only-segment gap by
//! recording a lower bound that every recovery must find before it may publish
//! a recovered WAL.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::SEGMENT_HEADER_LEN,
    error::WalError,
    format::codec::{
        copy_with_zeroed_range, crc32c, is_all_zero, put_bytes, put_u16_le, put_u32_le, put_u64_le,
        read_array, read_u16_le, read_u32_le, read_u64_le,
    },
    lsn::Lsn,
    types::{SegmentId, WalIdentity},
};

pub const WAL_MANIFEST_FILE_NAME: &str = "wal.manifest";

/// Last successfully synchronized WAL tail published by the writer.
///
/// This is deliberately a lower-bound witness. Recovery may accept additional
/// fully valid bytes written after this publication, but it must never accept or
/// repair a physical history that ends before this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalManifest {
    pub magic: u32,
    pub version: u16,
    pub header_len: u16,
    pub system_id: u64,
    pub wal_incarnation: u64,
    pub timeline_id: u64,
    pub tail_segment_id: SegmentId,
    pub tail_segment_base_lsn: Lsn,
    pub durable_lsn: Lsn,
    pub synchronized_file_len: u64,
    pub checksum: u32,
    pub reserved: [u8; 12],
}

impl WalManifest {
    pub const MAGIC: u32 = 0x5741_4C4D;
    pub const SUPPORTED_VERSION: u16 = 1;
    pub const ENCODED_LEN: usize = 80;

    const CHECKSUM_FIELD_START: usize = 64;
    const CHECKSUM_FIELD_END: usize = 68;

    pub fn new(
        identity: WalIdentity,
        tail_segment_id: SegmentId,
        tail_segment_base_lsn: Lsn,
        durable_lsn: Lsn,
        synchronized_file_len: u64,
    ) -> Result<Self, WalError> {
        let manifest = Self {
            magic: Self::MAGIC,
            version: Self::SUPPORTED_VERSION,
            header_len: Self::ENCODED_LEN as u16,
            system_id: identity.system_id,
            wal_incarnation: identity.wal_incarnation,
            timeline_id: identity.timeline_id,
            tail_segment_id,
            tail_segment_base_lsn,
            durable_lsn,
            synchronized_file_len,
            checksum: 0,
            reserved: [0; 12],
        };

        manifest.validate()?;
        Ok(manifest)
    }

    pub const fn identity(&self) -> WalIdentity {
        WalIdentity::new(self.system_id, self.wal_incarnation, self.timeline_id)
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_inner(self.checksum)
    }

    pub fn finalize_checksum(&mut self) {
        self.checksum = crc32c(&self.encode_inner(0));
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(WalError::bad_manifest(format!(
                "encoded length is {}, expected {}",
                bytes.len(),
                Self::ENCODED_LEN
            )));
        }

        let mut offset = 0;
        let manifest = Self {
            magic: read_u32_le(bytes, &mut offset)?,
            version: read_u16_le(bytes, &mut offset)?,
            header_len: read_u16_le(bytes, &mut offset)?,
            system_id: read_u64_le(bytes, &mut offset)?,
            wal_incarnation: read_u64_le(bytes, &mut offset)?,
            timeline_id: read_u64_le(bytes, &mut offset)?,
            tail_segment_id: read_u64_le(bytes, &mut offset)?,
            tail_segment_base_lsn: Lsn::new(read_u64_le(bytes, &mut offset)?),
            durable_lsn: Lsn::new(read_u64_le(bytes, &mut offset)?),
            synchronized_file_len: read_u64_le(bytes, &mut offset)?,
            checksum: read_u32_le(bytes, &mut offset)?,
            reserved: read_array::<12>(bytes, &mut offset)?,
        };

        if offset != bytes.len() {
            return Err(WalError::bad_manifest("trailing bytes after fixed header"));
        }

        manifest.validate()?;
        manifest.verify_checksum(bytes)?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), WalError> {
        if self.magic != Self::MAGIC {
            return Err(WalError::bad_manifest(format!(
                "bad magic 0x{:08x}",
                self.magic
            )));
        }
        if self.version != Self::SUPPORTED_VERSION {
            return Err(WalError::bad_manifest(format!(
                "unsupported version {}, expected {}",
                self.version,
                Self::SUPPORTED_VERSION
            )));
        }
        if self.header_len != Self::ENCODED_LEN as u16 {
            return Err(WalError::bad_manifest("invalid header length"));
        }
        if self.tail_segment_id == 0 {
            return Err(WalError::bad_manifest("tail segment ID 0 is reserved"));
        }
        if !is_all_zero(&self.reserved) {
            return Err(WalError::bad_manifest("reserved bytes are non-zero"));
        }

        let logical_len = self
            .synchronized_file_len
            .checked_sub(SEGMENT_HEADER_LEN)
            .ok_or_else(|| WalError::bad_manifest("tail file is shorter than its header"))?;
        let expected_durable_lsn = self
            .tail_segment_base_lsn
            .checked_add_bytes(logical_len)
            .ok_or(WalError::ReservationOverflow)?;

        if expected_durable_lsn != self.durable_lsn {
            return Err(WalError::bad_manifest(
                "durable LSN does not match the synchronized segment length",
            ));
        }

        Ok(())
    }

    pub fn validate_identity(&self, expected: WalIdentity) -> Result<(), WalError> {
        let found = self.identity();
        if found != expected {
            return Err(WalError::IdentityMismatch { expected, found });
        }
        Ok(())
    }

    fn verify_checksum(&self, encoded: &[u8]) -> Result<(), WalError> {
        let zeroed = copy_with_zeroed_range(
            encoded,
            Self::CHECKSUM_FIELD_START..Self::CHECKSUM_FIELD_END,
        );
        if crc32c(&zeroed) != self.checksum {
            return Err(WalError::bad_manifest("checksum mismatch"));
        }
        Ok(())
    }

    fn encode_inner(&self, checksum: u32) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::ENCODED_LEN);
        put_u32_le(&mut bytes, self.magic);
        put_u16_le(&mut bytes, self.version);
        put_u16_le(&mut bytes, self.header_len);
        put_u64_le(&mut bytes, self.system_id);
        put_u64_le(&mut bytes, self.wal_incarnation);
        put_u64_le(&mut bytes, self.timeline_id);
        put_u64_le(&mut bytes, self.tail_segment_id);
        put_u64_le(&mut bytes, self.tail_segment_base_lsn.as_u64());
        put_u64_le(&mut bytes, self.durable_lsn.as_u64());
        put_u64_le(&mut bytes, self.synchronized_file_len);
        put_u32_le(&mut bytes, checksum);
        put_bytes(&mut bytes, &self.reserved);
        debug_assert_eq!(bytes.len(), Self::ENCODED_LEN);
        bytes
    }
}

/// Filesystem publisher for the atomically replaced MANIFEST witness.
#[derive(Debug, Clone)]
pub struct FsWalManifestStore {
    dir: PathBuf,
}

impl FsWalManifestStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(WAL_MANIFEST_FILE_NAME)
    }

    pub fn read(&self) -> Result<Option<WalManifest>, WalError> {
        let bytes = match fs::read(self.path()) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source.into()),
        };
        Ok(Some(WalManifest::decode(&bytes)?))
    }

    pub fn load_for_recovery(
        &self,
        expected_identity: WalIdentity,
    ) -> Result<Option<WalManifest>, WalError> {
        let manifest = self.read()?;
        if let Some(manifest) = &manifest {
            manifest.validate_identity(expected_identity)?;
        }
        Ok(manifest)
    }

    pub fn publish(&self, manifest: &WalManifest) -> Result<(), WalError> {
        let mut manifest = manifest.clone();
        manifest.validate()?;
        manifest.finalize_checksum();

        let temporary_path = self.temporary_path();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&manifest.encode())?;
        file.flush()?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temporary_path, self.path())?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    fn temporary_path(&self) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_nanos();
        self.dir.join(format!(
            ".tmp-{}-{}-{nanos}",
            WAL_MANIFEST_FILE_NAME,
            process::id()
        ))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trips_and_binds_the_durable_tail() {
        let identity = WalIdentity::new(11, 22, 1);
        let mut manifest = WalManifest::new(
            identity,
            7,
            Lsn::new(4_096),
            Lsn::new(4_160),
            SEGMENT_HEADER_LEN + 64,
        )
        .unwrap();
        manifest.finalize_checksum();

        let decoded = WalManifest::decode(&manifest.encode()).unwrap();

        assert_eq!(decoded, manifest);
        assert_eq!(decoded.identity(), identity);
    }

    #[test]
    fn manifest_rejects_a_tail_lsn_inconsistent_with_file_length() {
        let error = WalManifest::new(
            WalIdentity::new(11, 22, 1),
            7,
            Lsn::new(4_096),
            Lsn::new(4_161),
            SEGMENT_HEADER_LEN + 64,
        )
        .unwrap_err();

        assert!(matches!(error, WalError::BadWalManifest { .. }));
    }
}
