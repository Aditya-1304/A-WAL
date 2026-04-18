use crate::{
    error::WalError,
    format::codec::{
        copy_with_zeroed_range, is_all_zero, put_bytes, put_u8, put_u16_le, put_u32_le, put_u64_le,
        read_array, read_u8, read_u16_le, read_u32_le, read_u64_le,
    },
    lsn::Lsn,
    types::{SegmentId, WalIdentity},
};

pub mod checksum_algorithms {
    pub const CRC32C: u8 = 1;
}

pub mod compression_algorithms {
    pub const NONE: u8 = 0;
    pub const LZ4: u8 = 1;
    pub const ZSTD: u8 = 2;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u16,
    pub header_len: u16,
    pub system_id: u64,
    pub wal_incarnation: u64,
    pub timeline_id: u64,
    pub segment_id: SegmentId,
    pub base_lsn: Lsn,
    pub checksum_algorithm: u8,
    pub compression_algorithm: u8,
    pub header_checksum: u32,
    pub reserved: [u8; 14],
}

impl SegmentHeader {
    pub const MAGIC: u32 = 0x5741_4C53;
    pub const SUPPORTED_VERSION: u16 = 1;
    pub const ENCODED_LEN: usize = 68;

    const CHECKSUM_FIELD_START: usize = 50;
    const CHECKSUM_FIELD_END: usize = 54;

    pub fn new(
        identity: WalIdentity,
        segment_id: SegmentId,
        base_lsn: Lsn,
        compression_algorithm: u8,
        version: u16,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            version,
            header_len: Self::ENCODED_LEN as u16,
            system_id: identity.system_id,
            wal_incarnation: identity.wal_incarnation,
            timeline_id: identity.timeline_id,
            segment_id,
            base_lsn,
            checksum_algorithm: checksum_algorithms::CRC32C,
            compression_algorithm,
            header_checksum: 0,
            reserved: [0; 14],
        }
    }

    pub fn identity(&self) -> WalIdentity {
        WalIdentity::new(self.system_id, self.wal_incarnation, self.timeline_id)
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_inner(self.header_checksum)
    }

    pub fn compute_checksum(&self) -> u32 {
        let bytes = self.encode_inner(0);
        crc32c(&bytes)
    }

    pub fn finalize_checksum(&mut self) {
        self.header_checksum = self.compute_checksum();
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
        let segment_id = read_u64_le(bytes, &mut offset)?;
        let base_lsn = Lsn::new(read_u64_le(bytes, &mut offset)?);
        let checksum_algorithm = read_u8(bytes, &mut offset)?;
        let compression_algorithm = read_u8(bytes, &mut offset)?;
        let header_checksum = read_u32_le(bytes, &mut offset)?;
        let reserved = read_array::<14>(bytes, &mut offset)?;

        if offset != bytes.len() {
            return Err(WalError::BadSegmentHeader);
        }

        let header = Self {
            magic,
            version,
            header_len,
            system_id,
            wal_incarnation,
            timeline_id,
            segment_id,
            base_lsn,
            checksum_algorithm,
            compression_algorithm,
            header_checksum,
            reserved,
        };

        header.validate()?;
        header.verify_checksum(bytes)?;

        Ok(header)
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

        if self.checksum_algorithm != checksum_algorithms::CRC32C {
            return Err(WalError::UnsupportedChecksumAlgorithm {
                found: self.checksum_algorithm,
            });
        }

        if !Self::is_supported_compression_algorithm(self.compression_algorithm) {
            return Err(WalError::UnsupportedCompressionAlgorithm {
                found: self.compression_algorithm,
            });
        }

        if !is_all_zero(&self.reserved) {
            return Err(WalError::BadSegmentHeader);
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

        if expected != self.header_checksum {
            return Err(WalError::ChecksumMismatch { lsn: None });
        }

        Ok(())
    }

    fn encode_inner(&self, header_checksum: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);

        put_u32_le(&mut buf, self.magic);
        put_u16_le(&mut buf, self.version);
        put_u16_le(&mut buf, self.header_len);
        put_u64_le(&mut buf, self.system_id);
        put_u64_le(&mut buf, self.wal_incarnation);
        put_u64_le(&mut buf, self.timeline_id);
        put_u64_le(&mut buf, self.segment_id);
        put_u64_le(&mut buf, self.base_lsn.as_u64());
        put_u8(&mut buf, self.checksum_algorithm);
        put_u8(&mut buf, self.compression_algorithm);
        put_u32_le(&mut buf, header_checksum);
        put_bytes(&mut buf, &self.reserved);

        debug_assert_eq!(buf.len(), Self::ENCODED_LEN);
        buf
    }

    fn is_supported_compression_algorithm(algorithm: u8) -> bool {
        matches!(
            algorithm,
            compression_algorithms::NONE
                | compression_algorithms::LZ4
                | compression_algorithms::ZSTD
        )
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    const POLY: u32 = 0x82F6_3B78;

    let mut crc = !0u32;

    for &byte in bytes {
        crc ^= u32::from(byte);

        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
    }

    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> SegmentHeader {
        let mut header = SegmentHeader::new(
            WalIdentity::new(11, 22, 1),
            7,
            Lsn::new(4096),
            compression_algorithms::NONE,
            SegmentHeader::SUPPORTED_VERSION,
        );
        header.finalize_checksum();
        header
    }

    #[test]
    fn encoded_len_matches_field_layout() {
        assert_eq!(SegmentHeader::ENCODED_LEN, 68);
    }

    #[test]
    fn segment_header_round_trips() {
        let header = sample_header();

        let bytes = header.encode();
        let decoded = SegmentHeader::decode(&bytes).unwrap();

        assert_eq!(decoded, header);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[0] ^= 0xFF;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadMagic { .. }));
    }

    #[test]
    fn decode_rejects_bad_header_len() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[6] = 0;
        bytes[7] = 0;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadSegmentHeader));
    }

    #[test]
    fn decode_rejects_unsupported_checksum_algorithm() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[48] = 99;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(
            err,
            WalError::UnsupportedChecksumAlgorithm { found: 99 }
        ));
    }

    #[test]
    fn decode_rejects_unsupported_compression_algorithm() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[49] = 99;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(
            err,
            WalError::UnsupportedCompressionAlgorithm { found: 99 }
        ));
    }

    #[test]
    fn decode_rejects_non_zero_reserved_bytes() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[67] = 1;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadSegmentHeader));
    }

    #[test]
    fn decode_rejects_checksum_mismatch() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[8] ^= 0x01;

        let err = SegmentHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch { lsn: None }));
    }

    #[test]
    fn identity_accessor_rebuilds_identity_tuple() {
        let header = sample_header();

        assert_eq!(header.identity(), WalIdentity::new(11, 22, 1));
    }
}
