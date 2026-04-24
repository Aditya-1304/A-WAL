use std::{fmt, io};

use crate::{lsn::Lsn, types::WalIdentity};

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    InvalidConfig {
        reason: String,
    },

    UnsupportedVersion {
        found: u16,
        expected: u16,
    },
    BadMagic {
        found: u32,
    },
    IdentityMismatch {
        expected: WalIdentity,
        found: WalIdentity,
    },
    BadSegmentHeader,
    BadRecordHeader,
    UnsupportedRecordFlags {
        found: u16,
    },
    PayloadTooLarge {
        len: u32,
        max: u32,
    },
    UnsupportedChecksumAlgorithm {
        found: u8,
    },
    UnsupportedCompressionAlgorithm {
        found: u8,
    },
    ChecksumMismatch {
        lsn: Option<Lsn>,
    },
    ShortRead,

    DiskFull,
    FatalIo {
        operation: &'static str,
        source: io::Error,
    },
    BrokenDurabilityContract,
    NonMonotonicLsn {
        expected: Lsn,
        found: Lsn,
    },

    LsnPruned {
        lsn: Lsn,
    },
    LsnOutOfRange {
        lsn: Lsn,
    },
    ReadOnlyViolation,
    SegmentOrderingViolation,
    FilenameHeaderMismatch,
    ReadOnlyTailCorruption,
    CorruptionInSealedSegment,
    WalSizeLimitExceeded {
        current: u64,
        limit: u64,
    },
    ShutdownInProgress,

    DecompressionError {
        reason: String,
    },
    ReservationOverflow,
}

impl Clone for WalError {
    fn clone(&self) -> Self {
        match self {
            Self::Io(source) => Self::Io(clone_io_error(source)),

            Self::InvalidConfig { reason } => Self::InvalidConfig {
                reason: reason.clone(),
            },

            Self::UnsupportedVersion { found, expected } => Self::UnsupportedVersion {
                found: *found,
                expected: *expected,
            },

            Self::BadMagic { found } => Self::BadMagic { found: *found },

            Self::IdentityMismatch { expected, found } => Self::IdentityMismatch {
                expected: *expected,
                found: *found,
            },

            Self::BadSegmentHeader => Self::BadSegmentHeader,

            Self::BadRecordHeader => Self::BadRecordHeader,

            Self::UnsupportedRecordFlags { found } => {
                Self::UnsupportedRecordFlags { found: *found }
            }

            Self::PayloadTooLarge { len, max } => Self::PayloadTooLarge {
                len: *len,
                max: *max,
            },

            Self::UnsupportedChecksumAlgorithm { found } => {
                Self::UnsupportedChecksumAlgorithm { found: *found }
            }

            Self::UnsupportedCompressionAlgorithm { found } => {
                Self::UnsupportedCompressionAlgorithm { found: *found }
            }

            Self::ChecksumMismatch { lsn } => Self::ChecksumMismatch { lsn: *lsn },

            Self::ShortRead => Self::ShortRead,

            Self::DiskFull => Self::DiskFull,

            Self::FatalIo { operation, source } => Self::FatalIo {
                operation: *operation,
                source: clone_io_error(source),
            },

            Self::BrokenDurabilityContract => Self::BrokenDurabilityContract,

            Self::NonMonotonicLsn { expected, found } => Self::NonMonotonicLsn {
                expected: *expected,
                found: *found,
            },

            Self::LsnPruned { lsn } => Self::LsnPruned { lsn: *lsn },

            Self::LsnOutOfRange { lsn } => Self::LsnOutOfRange { lsn: *lsn },

            Self::ReadOnlyViolation => Self::ReadOnlyViolation,

            Self::SegmentOrderingViolation => Self::SegmentOrderingViolation,

            Self::FilenameHeaderMismatch => Self::FilenameHeaderMismatch,

            Self::ReadOnlyTailCorruption => Self::ReadOnlyTailCorruption,

            Self::CorruptionInSealedSegment => Self::CorruptionInSealedSegment,

            Self::WalSizeLimitExceeded { current, limit } => Self::WalSizeLimitExceeded {
                current: *current,
                limit: *limit,
            },

            Self::ShutdownInProgress => Self::ShutdownInProgress,

            Self::DecompressionError { reason } => Self::DecompressionError {
                reason: reason.clone(),
            },

            Self::ReservationOverflow => Self::ReservationOverflow,
        }
    }
}

fn clone_io_error(source: &io::Error) -> io::Error {
    io::Error::new(source.kind(), source.to_string())
}

impl WalError {
    pub fn invalid_config(reason: impl Into<String>) -> Self {
        Self::InvalidConfig {
            reason: reason.into(),
        }
    }

    pub fn decompression_error(reason: impl Into<String>) -> Self {
        Self::DecompressionError {
            reason: reason.into(),
        }
    }

    pub fn fatal_io(operation: &'static str, source: io::Error) -> Self {
        Self::FatalIo { operation, source }
    }
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(err) => write!(f, "i/o error: {err}"),
            WalError::InvalidConfig { reason } => write!(f, "invalid config: {reason}"),

            WalError::UnsupportedVersion { found, expected } => {
                write!(f, "unsupported version: found {found}, expected {expected}")
            }
            WalError::BadMagic { found } => write!(f, "bad magic: found 0x{found:08x}"),
            WalError::IdentityMismatch { expected, found } => write!(
                f,
                "identity mismatch: expected (system_id={}, wal_incarnation={}, timeline_id={}), found (system_id={}, wal_incarnation={}, timeline_id={})",
                expected.system_id,
                expected.wal_incarnation,
                expected.timeline_id,
                found.system_id,
                found.wal_incarnation,
                found.timeline_id,
            ),
            WalError::BadSegmentHeader => write!(f, "bad segment header"),
            WalError::BadRecordHeader => write!(f, "bad record header"),
            WalError::UnsupportedRecordFlags { found } => {
                write!(f, "unsupported record flags: 0x{found:04x}")
            }
            WalError::PayloadTooLarge { len, max } => {
                write!(f, "payload too large: len={len}, max={max}")
            }
            WalError::UnsupportedChecksumAlgorithm { found } => {
                write!(f, "unsupported checksum algorithm: {found}")
            }
            WalError::UnsupportedCompressionAlgorithm { found } => {
                write!(f, "unsupported compression algorithm: {found}")
            }
            WalError::ChecksumMismatch { lsn } => match lsn {
                Some(lsn) => write!(f, "checksum mismatch at lsn {}", lsn.as_u64()),
                None => write!(f, "checksum mismatch"),
            },
            WalError::ShortRead => write!(f, "short read"),

            WalError::DiskFull => write!(f, "disk full"),
            WalError::FatalIo { operation, source } => {
                write!(f, "fatal i/o during {operation}: {source}")
            }
            WalError::BrokenDurabilityContract => write!(f, "broken durability contract"),
            WalError::NonMonotonicLsn { expected, found } => write!(
                f,
                "non-monotonic lsn: expected {}, found {}",
                expected.as_u64(),
                found.as_u64()
            ),

            WalError::LsnPruned { lsn } => write!(f, "lsn {} has been pruned", lsn.as_u64()),
            WalError::LsnOutOfRange { lsn } => {
                write!(f, "lsn {} is out of range", lsn.as_u64())
            }
            WalError::ReadOnlyViolation => write!(f, "operation is not allowed in read-only mode"),
            WalError::SegmentOrderingViolation => write!(f, "segment ordering violation"),
            WalError::FilenameHeaderMismatch => write!(f, "filename/header mismatch"),
            WalError::ReadOnlyTailCorruption => {
                write!(f, "tail corruption detected in read-only mode")
            }
            WalError::CorruptionInSealedSegment => {
                write!(f, "corruption detected in a sealed segment")
            }
            WalError::WalSizeLimitExceeded { current, limit } => {
                write!(
                    f,
                    "wal size limit exceeded: current={current}, limit={limit}"
                )
            }
            WalError::ShutdownInProgress => write!(f, "shutdown is in progress"),

            WalError::DecompressionError { reason } => {
                write!(f, "decompression error: {reason}")
            }
            WalError::ReservationOverflow => write!(f, "reservation overflow"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(err) => Some(err),
            WalError::FatalIo { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(source: io::Error) -> Self {
        Self::Io(source)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use super::WalError;

    #[test]
    fn invalid_config_helper_builds_variant() {
        let err = WalError::invalid_config("write_buffer_size must be non-zero");

        match err {
            WalError::InvalidConfig { reason } => {
                assert_eq!(reason, "write_buffer_size must be non-zero");
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn from_io_error_wraps_io_variant() {
        let err = WalError::from(io::Error::other("boom"));

        match err {
            WalError::Io(source) => assert_eq!(source.to_string(), "boom"),
            other => panic!("expected Io, got {other:?}"),
        }
    }

    #[test]
    fn fatal_io_exposes_source_error() {
        let err = WalError::fatal_io("sync", io::Error::other("fsync failed"));

        assert_eq!(
            err.source()
                .expect("fatal io should expose source")
                .to_string(),
            "fsync failed"
        );
    }
}
