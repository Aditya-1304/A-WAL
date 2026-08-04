use std::{fmt, io};

use crate::{
    lsn::Lsn,
    types::WalIdentity,
    wal::engine::{AppendResult, BatchAppendResult},
};

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

    /// `wal.manifest` is malformed or internally inconsistent.
    BadWalManifest {
        reason: String,
    },

    /// Physical files no longer cover the last tail acknowledged as durable.
    MissingDurableWalHistory {
        expected_segment_id: u64,
        expected_durable_lsn: Lsn,
        reason: String,
    },

    /// two retained WAL segments do not form one exact physical history
    ///
    /// the first retained segment may begin above segment one after legitimate
    /// retention. Every later segment must have the immediately following
    /// segment ID and must begin at the previous segment's exact logical end
    SegmentContinuityViolation {
        expected_segment_id: u64,
        found_segment_id: u64,
        expected_base_lsn: Lsn,
        found_base_lsn: Lsn,
    },

    /// historical segment ended without its required `SEGMENT_SEAL`
    MissingSegmentSeal {
        segment_id: u64,
    },

    /// `SEGMENT_SEAL` payload does not describe the bytes preceding it
    InvalidSegmentSeal {
        segment_id: u64,
        reason: String,
    },

    /// physical record was found after a segment's terminal seal
    RecordAfterSegmentSeal {
        segment_id: u64,
        lsn: Lsn,
    },

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
    EmptyBatch,
}

/// failure classification for a single record WAL append
///
/// this type preserves whether the caller can safely conclude that the user
/// record was not appended or whether recovery is required to determine its
/// durable outcome
#[derive(Debug, Clone)]
pub enum AppendFailure {
    /// the user record did not acquire a logical WAL extent
    ///
    /// the underlying error may still have placed the WAL in sticky-fatal
    /// state. For example, rollover metadata can encounter mutating I/O before
    /// the user record itself is staged
    NotStaged(WalError),

    /// the user record acquired an extent, but the process cannot prove whether
    /// that complete extent reached stable storage
    ///
    /// callers must not treat this result as an ordinary abort or automatically
    /// retry the logical operation. Recovery must first determine the durable
    /// prefix
    OutcomeUnknown {
        /// exact logical interval assigned to the staged user record
        extent: AppendResult,

        /// failure that prevented the WAL from proving durability
        source: WalError,
    },
}

/// Failure classification for an ordered multi-record append.
///
/// `OutcomeUnknown` carries every extent assigned before the failure. Callers
/// must recover the WAL before retrying the logical batch because any prefix,
/// including the complete batch, may be present after restart.
#[derive(Debug, Clone)]
pub enum BatchAppendFailure {
    NotStaged(WalError),
    OutcomeUnknown {
        result: BatchAppendResult,
        source: WalError,
    },
}

impl BatchAppendFailure {
    pub fn into_source(self) -> WalError {
        match self {
            Self::NotStaged(source) | Self::OutcomeUnknown { source, .. } => source,
        }
    }

    pub fn extents(&self) -> &[AppendResult] {
        match self {
            Self::NotStaged(_) => &[],
            Self::OutcomeUnknown { result, .. } => &result.record_extents,
        }
    }

    pub const fn wal_error(&self) -> &WalError {
        match self {
            Self::NotStaged(source) | Self::OutcomeUnknown { source, .. } => source,
        }
    }
}

impl fmt::Display for BatchAppendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStaged(source) => write!(formatter, "WAL batch was not staged: {source}"),
            Self::OutcomeUnknown { result, source } => write!(
                formatter,
                "WAL batch outcome is unknown after assigning {} record extents: {source}",
                result.record_extents.len()
            ),
        }
    }
}

impl std::error::Error for BatchAppendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.wal_error())
    }
}

impl AppendFailure {
    /// this consumes the classification and return its underlying WAL error
    ///
    /// this is useful for legacy diagnostics and tests Transactional
    /// callers must inspect the classification before discarding it
    pub fn into_source(self) -> WalError {
        match self {
            Self::NotStaged(source) | Self::OutcomeUnknown { source, .. } => source,
        }
    }

    /// return the assigned extent when the append outcome is unknown
    pub const fn extent(&self) -> Option<AppendResult> {
        match self {
            Self::NotStaged(_) => None,
            Self::OutcomeUnknown { extent, .. } => Some(*extent),
        }
    }

    /// borrow the underlying WAL error without discarding classification
    pub const fn wal_error(&self) -> &WalError {
        match self {
            Self::NotStaged(source) | Self::OutcomeUnknown { source, .. } => source,
        }
    }
}

impl fmt::Display for AppendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStaged(source) => {
                write!(formatter, "WAL append was not staged: {source}")
            }

            Self::OutcomeUnknown { extent, source } => write!(
                formatter,
                "WAL append outcome is unknown for logical extent [{}, {}): {}",
                extent.start_lsn.as_u64(),
                extent.end_lsn.as_u64(),
                source
            ),
        }
    }
}

impl std::error::Error for AppendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.wal_error())
    }
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
                operation,
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

            Self::BadWalManifest { reason } => Self::BadWalManifest {
                reason: reason.clone(),
            },

            Self::MissingDurableWalHistory {
                expected_segment_id,
                expected_durable_lsn,
                reason,
            } => Self::MissingDurableWalHistory {
                expected_segment_id: *expected_segment_id,
                expected_durable_lsn: *expected_durable_lsn,
                reason: reason.clone(),
            },

            Self::SegmentContinuityViolation {
                expected_segment_id,
                found_segment_id,
                expected_base_lsn,
                found_base_lsn,
            } => Self::SegmentContinuityViolation {
                expected_segment_id: *expected_segment_id,
                found_segment_id: *found_segment_id,
                expected_base_lsn: *expected_base_lsn,
                found_base_lsn: *found_base_lsn,
            },

            Self::MissingSegmentSeal { segment_id } => Self::MissingSegmentSeal {
                segment_id: *segment_id,
            },

            Self::InvalidSegmentSeal { segment_id, reason } => Self::InvalidSegmentSeal {
                segment_id: *segment_id,
                reason: reason.clone(),
            },

            Self::RecordAfterSegmentSeal { segment_id, lsn } => Self::RecordAfterSegmentSeal {
                segment_id: *segment_id,
                lsn: *lsn,
            },

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
            Self::EmptyBatch => Self::EmptyBatch,
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

    pub fn bad_manifest(reason: impl Into<String>) -> Self {
        Self::BadWalManifest {
            reason: reason.into(),
        }
    }

    /// Return whether this error proves the live writer is fail-stopped.
    ///
    /// A `NotStaged` append remains a definite rejection for the user record,
    /// but a fatal source means the shared WAL handle still requires reopen and
    /// recovery before any later database operation can be trusted.
    pub const fn requires_recovery(&self) -> bool {
        matches!(self, Self::FatalIo { .. } | Self::BrokenDurabilityContract)
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

            WalError::BadWalManifest { reason } => {
                write!(f, "invalid WAL manifest: {reason}")
            }

            WalError::MissingDurableWalHistory {
                expected_segment_id,
                expected_durable_lsn,
                reason,
            } => write!(
                f,
                "durable WAL history is missing: expected segment {} through LSN {}: {}",
                expected_segment_id,
                expected_durable_lsn.as_u64(),
                reason,
            ),

            WalError::SegmentContinuityViolation {
                expected_segment_id,
                found_segment_id,
                expected_base_lsn,
                found_base_lsn,
            } => write!(
                f,
                "WAL segment continuity violation: expected segment {} at base LSN {}, \
                 found segment {} at base LSN {}",
                expected_segment_id,
                expected_base_lsn.as_u64(),
                found_segment_id,
                found_base_lsn.as_u64(),
            ),

            WalError::MissingSegmentSeal { segment_id } => {
                write!(
                    f,
                    "historical WAL segment {segment_id} is missing its terminal segment seal"
                )
            }

            WalError::InvalidSegmentSeal { segment_id, reason } => {
                write!(
                    f,
                    "WAL segment {segment_id} contains an invalid segment seal: {reason}"
                )
            }

            WalError::RecordAfterSegmentSeal { segment_id, lsn } => {
                write!(
                    f,
                    "WAL segment {segment_id} contains a record at LSN {} after its terminal seal",
                    lsn.as_u64(),
                )
            }

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
            WalError::EmptyBatch => write!(f, "WAL append batch must not be empty"),
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
