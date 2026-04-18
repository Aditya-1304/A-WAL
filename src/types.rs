pub type SegmentId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordType(pub u16);

impl RecordType {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }

    pub const fn is_internal(self) -> bool {
        self.0 < record_types::USER_MIN
    }

    pub const fn is_user_defined(self) -> bool {
        self.0 >= record_types::USER_MIN
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalIdentity {
    pub system_id: u64,
    pub wal_incarnation: u64,
    pub timeline_id: u64,
}

impl WalIdentity {
    pub const fn new(system_id: u64, wal_incarnation: u64, timeline_id: u64) -> Self {
        Self {
            system_id,
            wal_incarnation,
            timeline_id,
        }
    }
}

pub mod record_types {
    use super::RecordType;

    pub const USER_MIN: u16 = 1024;

    pub const SEGMENT_MARKER: RecordType = RecordType::new(1);
    pub const BEGIN_CHECKPOINT: RecordType = RecordType::new(2);
    pub const END_CHECKPOINT: RecordType = RecordType::new(3);
    pub const CHECKPOINT_CHUNK: RecordType = RecordType::new(4);
    pub const SEGMENT_SEAL: RecordType = RecordType::new(5);
    pub const SHUTDOWN: RecordType = RecordType::new(6);
}

pub mod record_flags {
    pub const NONE: u16 = 0x0000;
    pub const COMPRESSED: u16 = 0x0001;
    pub const KNOWN_MASK: u16 = COMPRESSED;
}

#[cfg(test)]
mod tests {
    use super::{RecordType, WalIdentity, record_flags, record_types};

    #[test]
    fn record_type_new_and_as_u16_round_trip() {
        let record_type = RecordType::new(42);
        assert_eq!(record_type.as_u16(), 42);
    }

    #[test]
    fn internal_record_types_are_below_user_min() {
        assert!(record_types::SEGMENT_MARKER.is_internal());
        assert!(record_types::BEGIN_CHECKPOINT.is_internal());
        assert!(record_types::END_CHECKPOINT.is_internal());
        assert!(record_types::CHECKPOINT_CHUNK.is_internal());
        assert!(record_types::SEGMENT_SEAL.is_internal());
        assert!(record_types::SHUTDOWN.is_internal());
    }

    #[test]
    fn user_defined_record_type_starts_at_user_min() {
        let user_type = RecordType::new(record_types::USER_MIN);
        assert!(user_type.is_user_defined());
        assert!(!user_type.is_internal());
    }

    #[test]
    fn known_mask_matches_current_flag_set() {
        assert_eq!(record_flags::KNOWN_MASK, record_flags::COMPRESSED);
    }

    #[test]
    fn wal_identity_constructor_sets_all_fields() {
        let identity = WalIdentity::new(11, 22, 33);

        assert_eq!(identity.system_id, 11);
        assert_eq!(identity.wal_incarnation, 22);
        assert_eq!(identity.timeline_id, 33);
    }
}
