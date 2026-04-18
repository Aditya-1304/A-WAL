#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add_bytes(self, bytes: u64) -> Option<Self> {
        self.0.checked_add(bytes).map(Self)
    }

    pub fn checked_sub_bytes(self, bytes: u64) -> Option<Self> {
        self.0.checked_sub(bytes).map(Self)
    }

    pub fn checked_distance_from(self, earlier: Self) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }
}

#[cfg(test)]
mod tests {
    use super::Lsn;

    #[test]
    fn zero_constant_is_zero() {
        assert_eq!(Lsn::ZERO, Lsn(0));
    }

    #[test]
    fn new_and_as_u64_round_trip() {
        let lsn = Lsn::new(4096);
        assert_eq!(lsn.as_u64(), 4096);
    }

    #[test]
    fn checked_add_bytes_success() {
        let lsn = Lsn(100);
        assert_eq!(lsn.checked_add_bytes(24), Some(Lsn(124)));
    }

    #[test]
    fn checked_add_bytes_overflow_returns_none() {
        let lsn = Lsn(u64::MAX);
        assert_eq!(lsn.checked_add_bytes(1), None);
    }

    #[test]
    fn checked_sub_bytes_success() {
        let lsn = Lsn(100);
        assert_eq!(lsn.checked_sub_bytes(40), Some(Lsn(60)));
    }

    #[test]
    fn checked_sub_bytes_underflow_returns_none() {
        let lsn = Lsn(10);
        assert_eq!(lsn.checked_sub_bytes(11), None);
    }

    #[test]
    fn checked_distance_from_success() {
        let later = Lsn(8192);
        let earlier = Lsn(4096);
        assert_eq!(later.checked_distance_from(earlier), Some(4096));
    }

    #[test]
    fn checked_distance_from_underflow_returns_none() {
        let earlier = Lsn(100);
        let later = Lsn(200);
        assert_eq!(earlier.checked_distance_from(later), None);
    }
}
