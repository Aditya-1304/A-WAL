use std::path::PathBuf;

use wal::{
    config::{
        CompressionPolicy, DEFAULT_FORMAT_VERSION, DEFAULT_MAX_RECORD_SIZE,
        DEFAULT_MAX_RECYCLED_SEGMENTS, DEFAULT_STORAGE_WRITE_UNIT, DEFAULT_TARGET_SEGMENT_SIZE,
        DEFAULT_WRITE_BUFFER_COUNT, DEFAULT_WRITE_BUFFER_SIZE, RECORD_HEADER_LEN,
        SEGMENT_HEADER_LEN, SyncPolicy, WalConfig,
    },
    error::WalError,
    types::WalIdentity,
};

fn expect_invalid_config(mutator: impl FnOnce(&mut WalConfig)) -> WalError {
    let mut config = WalConfig::default();
    mutator(&mut config);
    config.validate().expect_err("config should be invalid")
}

#[test]
fn default_config_matches_phase1_spec_defaults() {
    let config = WalConfig::default();

    assert_eq!(config.dir, PathBuf::from("."));
    assert_eq!(config.identity, WalIdentity::new(0, 0, 1));
    assert_eq!(config.target_segment_size, DEFAULT_TARGET_SEGMENT_SIZE);
    assert_eq!(config.max_record_size, DEFAULT_MAX_RECORD_SIZE);
    assert_eq!(config.storage_write_unit, DEFAULT_STORAGE_WRITE_UNIT);
    assert_eq!(config.write_buffer_size, DEFAULT_WRITE_BUFFER_SIZE);
    assert_eq!(config.write_buffer_count, DEFAULT_WRITE_BUFFER_COUNT);
    assert_eq!(config.format_version, DEFAULT_FORMAT_VERSION);
    assert_eq!(config.sync_policy, SyncPolicy::OnExplicitSync);
    assert!(config.truncate_tail);
    assert!(!config.read_only);
    assert_eq!(config.record_alignment, 0);
    assert_eq!(config.max_recycled_segments, DEFAULT_MAX_RECYCLED_SEGMENTS);
    assert_eq!(config.compression_policy, CompressionPolicy::None);
    assert_eq!(config.max_wal_size, None);
}

#[test]
fn default_config_is_valid() {
    let config = WalConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn minimum_valid_storage_write_unit_is_accepted() {
    let mut config = WalConfig::default();
    config.storage_write_unit = 512;

    assert!(config.validate().is_ok());
}

#[test]
fn aligned_config_can_be_valid() {
    let mut config = WalConfig::default();
    config.record_alignment = 4096;
    config.target_segment_size = 64 * 1024 * 1024;
    config.storage_write_unit = 4096;
    config.write_buffer_size = 4 * 1024 * 1024;

    assert!(config.validate().is_ok());
}

#[test]
fn max_wal_size_when_positive_is_accepted() {
    let mut config = WalConfig::default();
    config.max_wal_size = Some(1);

    assert!(config.validate().is_ok());
}

#[test]
fn storage_write_unit_must_be_at_least_512() {
    let err = expect_invalid_config(|config| {
        config.storage_write_unit = 256;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn storage_write_unit_must_be_power_of_two() {
    let err = expect_invalid_config(|config| {
        config.storage_write_unit = 3000;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn write_buffer_size_must_be_non_zero() {
    let err = expect_invalid_config(|config| {
        config.write_buffer_size = 0;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn write_buffer_count_must_be_at_least_one() {
    let err = expect_invalid_config(|config| {
        config.write_buffer_count = 0;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn write_buffer_size_must_be_multiple_of_storage_write_unit() {
    let err = expect_invalid_config(|config| {
        config.write_buffer_size = 4097;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn record_alignment_must_be_supported_value() {
    let err = expect_invalid_config(|config| {
        config.record_alignment = 1024;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn target_segment_size_must_match_record_alignment_when_enabled() {
    let err = expect_invalid_config(|config| {
        config.record_alignment = 4096;
        config.target_segment_size = DEFAULT_TARGET_SEGMENT_SIZE + 1;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn segment_must_fit_segment_header_plus_one_max_sized_record() {
    let err = expect_invalid_config(|config| {
        config.target_segment_size = SEGMENT_HEADER_LEN + u64::from(DEFAULT_MAX_RECORD_SIZE) - 1;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn write_buffer_must_fit_record_header_plus_one_max_sized_record() {
    let err = expect_invalid_config(|config| {
        config.write_buffer_size = RECORD_HEADER_LEN + DEFAULT_MAX_RECORD_SIZE as usize - 1;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn max_wal_size_when_set_must_be_non_zero() {
    let err = expect_invalid_config(|config| {
        config.max_wal_size = Some(0);
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn format_version_must_match_supported_v1_version() {
    let err = expect_invalid_config(|config| {
        config.format_version = 2;
    });

    assert!(matches!(err, WalError::InvalidConfig { .. }));
}

#[test]
fn default_format_version_is_accepted() {
    let config = WalConfig::default();
    assert_eq!(config.format_version, DEFAULT_FORMAT_VERSION);
    assert!(config.validate().is_ok());
}
