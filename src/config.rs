use std::path::PathBuf;

use crate::{error::WalError, types::WalIdentity};

pub const DEFAULT_TARGET_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RECORD_SIZE: u32 = 1024 * 1024;
pub const DEFAULT_STORAGE_WRITE_UNIT: u32 = 4096;
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_WRITE_BUFFER_COUNT: usize = 2;
pub const DEFAULT_FORMAT_VERSION: u16 = 1;
pub const DEFAULT_MAX_RECYCLED_SEGMENTS: usize = 8;

pub const SEGMENT_HEADER_LEN: u64 = 68;
pub const RECORD_HEADER_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionPolicy {
    None,
    Lz4,
    Zstd { level: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    OnExplicitSync,
    GroupCommit {
        max_delay: std::time::Duration,
        max_batch: usize,
    },
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub identity: WalIdentity,
    pub target_segment_size: u64,
    pub max_record_size: u32,
    pub storage_write_unit: u32,
    pub write_buffer_size: usize,
    pub write_buffer_count: usize,
    pub format_version: u16,
    pub sync_policy: SyncPolicy,
    pub truncate_tail: bool,
    pub read_only: bool,
    pub record_alignment: u32,
    pub max_recycled_segments: usize,
    pub compression_policy: CompressionPolicy,
    pub max_wal_size: Option<u64>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            identity: WalIdentity::new(0, 0, 1),
            target_segment_size: DEFAULT_TARGET_SEGMENT_SIZE,
            max_record_size: DEFAULT_MAX_RECORD_SIZE,
            storage_write_unit: DEFAULT_STORAGE_WRITE_UNIT,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            write_buffer_count: DEFAULT_WRITE_BUFFER_COUNT,
            format_version: DEFAULT_FORMAT_VERSION,
            sync_policy: SyncPolicy::OnExplicitSync,
            truncate_tail: true,
            read_only: false,
            record_alignment: 0,
            max_recycled_segments: DEFAULT_MAX_RECYCLED_SEGMENTS,
            compression_policy: CompressionPolicy::None,
            max_wal_size: None,
        }
    }
}

impl WalConfig {
    pub fn validate(&self) -> Result<(), WalError> {
        if self.storage_write_unit < 512 {
            return Err(WalError::invalid_config(
                "storage_write_unit must be at least 512",
            ));
        }

        if !self.storage_write_unit.is_power_of_two() {
            return Err(WalError::invalid_config(
                "storage_write_unit must be a power of two",
            ));
        }

        if self.write_buffer_size == 0 {
            return Err(WalError::invalid_config(
                "write_buffer_size must be greater than zero",
            ));
        }

        if self.write_buffer_count == 0 {
            return Err(WalError::invalid_config(
                "write_buffer_count must be at least 1",
            ));
        }

        if self.write_buffer_size % self.storage_write_unit as usize != 0 {
            return Err(WalError::invalid_config(
                "write_buffer_size must be a multiple of storage_write_unit",
            ));
        }

        if !matches!(self.record_alignment, 0 | 512 | 4096) {
            return Err(WalError::invalid_config(
                "record_alignment must be one of: 0, 512, 4096",
            ));
        }

        if self.record_alignment > 0
            && self.target_segment_size % u64::from(self.record_alignment) != 0
        {
            return Err(WalError::invalid_config(
                "target_segment_size must be a multiple of record_alignment when alignment is enabled",
            ));
        }

        let max_record_total_len = u64::try_from(RECORD_HEADER_LEN)
            .expect("record header len fits in u64")
            + u64::from(self.max_record_size);

        if SEGMENT_HEADER_LEN + max_record_total_len > self.target_segment_size {
            return Err(WalError::invalid_config(
                "segment size is too small to hold the segment header plus one maximum-sized record",
            ));
        }

        let min_write_buffer_size =
            RECORD_HEADER_LEN + usize::try_from(self.max_record_size).expect("u32 fits in usize");

        if self.write_buffer_size < min_write_buffer_size {
            return Err(WalError::invalid_config(
                "write_buffer_size must be large enough to hold one record header plus one maximum-sized record",
            ));
        }

        if let Some(limit) = self.max_wal_size {
            if limit == 0 {
                return Err(WalError::invalid_config(
                    "max_wal_size, when set, must be greater than zero",
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::WalError;

    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = WalConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn storage_write_unit_must_be_at_least_512() {
        let mut config = WalConfig::default();
        config.storage_write_unit = 256;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn storage_write_unit_must_be_power_of_two() {
        let mut config = WalConfig::default();
        config.storage_write_unit = 3000;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn write_buffer_size_must_be_non_zero() {
        let mut config = WalConfig::default();
        config.write_buffer_size = 0;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn write_buffer_count_must_be_at_least_one() {
        let mut config = WalConfig::default();
        config.write_buffer_count = 0;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn write_buffer_size_must_align_to_storage_write_unit() {
        let mut config = WalConfig::default();
        config.write_buffer_size = 4097;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn record_alignment_must_be_supported_value() {
        let mut config = WalConfig::default();
        config.record_alignment = 1024;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn target_segment_size_must_match_alignment_when_enabled() {
        let mut config = WalConfig::default();
        config.record_alignment = 4096;
        config.target_segment_size = DEFAULT_TARGET_SEGMENT_SIZE + 1;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn target_segment_size_must_fit_header_and_max_record() {
        let mut config = WalConfig::default();
        config.target_segment_size = SEGMENT_HEADER_LEN + u64::from(DEFAULT_MAX_RECORD_SIZE) - 1;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn write_buffer_size_must_fit_header_and_max_record() {
        let mut config = WalConfig::default();
        config.write_buffer_size = RECORD_HEADER_LEN + DEFAULT_MAX_RECORD_SIZE as usize - 1;

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }

    #[test]
    fn max_wal_size_when_set_must_be_non_zero() {
        let mut config = WalConfig::default();
        config.max_wal_size = Some(0);

        let err = config.validate().expect_err("config should be invalid");
        assert!(matches!(err, WalError::InvalidConfig { .. }));
    }
}
