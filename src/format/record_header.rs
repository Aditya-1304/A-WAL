use crate::{
    error::WalError,
    format::codec::{
        copy_with_zeroed_range, put_u16_le, put_u32_le, put_u64_le, read_u16_le, read_u32_le,
        read_u64_le,
    },
    lsn::Lsn,
    types::{RecordType, record_flags},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub magic: u32,
    pub version: u16,
    pub record_type: RecordType,
    pub header_len: u16,
    pub flags: u16,
    pub payload_len: u32,
    pub lsn: Lsn,
    pub checksum: u32,
    pub reserved: u32,
}

impl RecordHeader {
    pub const MAGIC: u32 = 0x5741_4C52;
    pub const SUPPORTED_VERSION: u16 = 1;
    pub const ENCODED_LEN: usize = 32;

    const CHECKSUM_FIELD_START: usize = 24;
    const CHECKSUM_FIELD_END: usize = 28;

    pub fn new(
        record_type: RecordType,
        flags: u16,
        payload_len: u32,
        lsn: Lsn,
        version: u16,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            version,
            record_type,
            header_len: Self::ENCODED_LEN as u16,
            flags,
            payload_len,
            lsn,
            checksum: 0,
            reserved: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.encode_inner(self.checksum)
    }

    pub fn total_len(&self) -> u32 {
        u32::from(self.header_len) + self.payload_len
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & record_flags::COMPRESSED != 0
    }

    pub fn compute_checksum(&self, payload: &[u8]) -> Result<u32, WalError> {
        self.validate_payload_slice(payload)?;

        let header_bytes = self.encode_inner(0);
        let mut combined = Vec::with_capacity(Self::ENCODED_LEN + payload.len());
        combined.extend_from_slice(&header_bytes);
        combined.extend_from_slice(payload);

        Ok(crc32c(&combined))
    }

    pub fn finalize_checksum(&mut self, payload: &[u8]) -> Result<(), WalError> {
        self.checksum = self.compute_checksum(payload)?;
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        if bytes.len() < Self::ENCODED_LEN {
            return Err(WalError::ShortRead);
        }

        if bytes.len() != Self::ENCODED_LEN {
            return Err(WalError::BadRecordHeader);
        }

        let mut offset = 0;

        let magic = read_u32_le(bytes, &mut offset)?;
        let version = read_u16_le(bytes, &mut offset)?;
        let record_type = RecordType::new(read_u16_le(bytes, &mut offset)?);
        let header_len = read_u16_le(bytes, &mut offset)?;
        let flags = read_u16_le(bytes, &mut offset)?;
        let payload_len = read_u32_le(bytes, &mut offset)?;
        let lsn = Lsn::new(read_u64_le(bytes, &mut offset)?);
        let checksum = read_u32_le(bytes, &mut offset)?;
        let reserved = read_u32_le(bytes, &mut offset)?;

        if offset != bytes.len() {
            return Err(WalError::BadRecordHeader);
        }

        let header = Self {
            magic,
            version,
            record_type,
            header_len,
            flags,
            payload_len,
            lsn,
            checksum,
            reserved,
        };

        header.validate()?;
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
            return Err(WalError::BadRecordHeader);
        }

        if self.flags & !record_flags::KNOWN_MASK != 0 {
            return Err(WalError::UnsupportedRecordFlags { found: self.flags });
        }

        if self.reserved != 0 {
            return Err(WalError::BadRecordHeader);
        }

        Ok(())
    }

    pub fn verify_checksum(&self, encoded_header: &[u8], payload: &[u8]) -> Result<(), WalError> {
        if encoded_header.len() < Self::ENCODED_LEN {
            return Err(WalError::ShortRead);
        }

        if encoded_header.len() != Self::ENCODED_LEN {
            return Err(WalError::BadRecordHeader);
        }

        self.validate_payload_slice(payload)?;

        let zeroed = copy_with_zeroed_range(
            encoded_header,
            Self::CHECKSUM_FIELD_START..Self::CHECKSUM_FIELD_END,
        );

        let mut combined = Vec::with_capacity(Self::ENCODED_LEN + payload.len());
        combined.extend_from_slice(&zeroed);
        combined.extend_from_slice(payload);

        let expected = crc32c(&combined);

        if expected != self.checksum {
            return Err(WalError::ChecksumMismatch {
                lsn: Some(self.lsn),
            });
        }

        Ok(())
    }

    fn encode_inner(&self, checksum: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);

        put_u32_le(&mut buf, self.magic);
        put_u16_le(&mut buf, self.version);
        put_u16_le(&mut buf, self.record_type.as_u16());
        put_u16_le(&mut buf, self.header_len);
        put_u16_le(&mut buf, self.flags);
        put_u32_le(&mut buf, self.payload_len);
        put_u64_le(&mut buf, self.lsn.as_u64());
        put_u32_le(&mut buf, checksum);
        put_u32_le(&mut buf, self.reserved);

        debug_assert_eq!(buf.len(), Self::ENCODED_LEN);
        buf
    }

    fn validate_payload_slice(&self, payload: &[u8]) -> Result<(), WalError> {
        let expected_len = self.payload_len as usize;

        if payload.len() < expected_len {
            return Err(WalError::ShortRead);
        }

        if payload.len() != expected_len {
            return Err(WalError::BadRecordHeader);
        }

        Ok(())
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
    use crate::types::{record_flags, record_types};

    fn sample_payload() -> &'static [u8] {
        b"hello wal"
    }

    fn sample_header() -> RecordHeader {
        let mut header = RecordHeader::new(
            record_types::SEGMENT_SEAL,
            record_flags::NONE,
            sample_payload().len() as u32,
            Lsn::new(8192),
            RecordHeader::SUPPORTED_VERSION,
        );
        header.finalize_checksum(sample_payload()).unwrap();
        header
    }

    #[test]
    fn encoded_len_matches_field_layout() {
        assert_eq!(RecordHeader::ENCODED_LEN, 32);
    }

    #[test]
    fn record_header_round_trips() {
        let header = sample_header();

        let bytes = header.encode();
        let decoded = RecordHeader::decode(&bytes).unwrap();

        assert_eq!(decoded, header);
    }

    #[test]
    fn total_len_is_header_plus_payload() {
        let header = sample_header();
        assert_eq!(header.total_len(), 32 + sample_payload().len() as u32);
    }

    #[test]
    fn is_compressed_checks_flag_bit() {
        let compressed = RecordHeader::new(
            record_types::SEGMENT_SEAL,
            record_flags::COMPRESSED,
            4,
            Lsn::new(1),
            RecordHeader::SUPPORTED_VERSION,
        );

        assert!(compressed.is_compressed());
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[0] ^= 0xFF;

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadMagic { .. }));
    }

    #[test]
    fn decode_rejects_bad_header_len() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[8] = 0;
        bytes[9] = 0;

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadRecordHeader));
    }

    #[test]
    fn decode_rejects_unknown_flag_bits() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[10] = 0x02;
        bytes[11] = 0x00;

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(
            err,
            WalError::UnsupportedRecordFlags { found: 0x0002 }
        ));
    }

    #[test]
    fn decode_rejects_non_zero_reserved() {
        let header = sample_header();
        let mut bytes = header.encode();

        bytes[28] = 1;

        let err = RecordHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalError::BadRecordHeader));
    }

    #[test]
    fn verify_checksum_accepts_valid_payload() {
        let header = sample_header();
        let bytes = header.encode();

        assert!(header.verify_checksum(&bytes, sample_payload()).is_ok());
    }

    #[test]
    fn verify_checksum_rejects_tampered_payload() {
        let header = sample_header();
        let bytes = header.encode();
        let tampered = b"jello wal";

        let err = header.verify_checksum(&bytes, tampered).unwrap_err();
        assert!(matches!(
            err,
            WalError::ChecksumMismatch { lsn: Some(lsn) } if lsn == Lsn::new(8192)
        ));
    }

    #[test]
    fn verify_checksum_rejects_short_payload() {
        let header = sample_header();
        let bytes = header.encode();

        let err = header.verify_checksum(&bytes, b"short").unwrap_err();
        assert!(matches!(err, WalError::ShortRead));
    }
}
