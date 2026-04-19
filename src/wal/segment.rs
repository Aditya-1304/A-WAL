use crate::{
    error::WalError, format::segment_header::SegmentHeader, io::segment_file::SegmentFile,
    lsn::Lsn, types::SegmentId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub base_lsn: Lsn,
    pub header_len: u64,
    pub file_len: u64,
    pub sealed: bool,
}

impl SegmentDescriptor {
    pub fn from_header(header: &SegmentHeader, file_len: u64) -> Result<Self, WalError> {
        Self::from_header_with_sealed(header, file_len, false)
    }

    pub fn from_header_with_sealed(
        header: &SegmentHeader,
        file_len: u64,
        sealed: bool,
    ) -> Result<Self, WalError> {
        header.validate()?;

        let header_len = u64::from(header.header_len);
        if file_len < header_len {
            return Err(WalError::BadSegmentHeader);
        }

        let descriptor = Self {
            segment_id: header.segment_id,
            base_lsn: header.base_lsn,
            header_len,
            file_len,
            sealed,
        };

        let _ = descriptor.written_end_lsn()?;
        Ok(descriptor)
    }

    pub fn data_start_offset(&self) -> u64 {
        self.header_len
    }

    pub fn tail_file_offset(&self) -> u64 {
        self.file_len
    }

    pub fn written_logical_len(&self) -> u64 {
        self.file_len - self.header_len
    }

    pub fn written_end_lsn(&self) -> Result<Lsn, WalError> {
        self.base_lsn
            .checked_add_bytes(self.written_logical_len())
            .ok_or(WalError::BadSegmentHeader)
    }

    pub fn contains_written_lsn(&self, lsn: Lsn) -> Result<bool, WalError> {
        Ok(lsn >= self.base_lsn && lsn < self.written_end_lsn()?)
    }

    pub fn checked_lsn_to_file_offset(&self, lsn: Lsn) -> Result<Option<u64>, WalError> {
        if !self.contains_written_lsn(lsn)? {
            return Ok(None);
        }

        let logical_offset = lsn
            .checked_distance_from(self.base_lsn)
            .ok_or(WalError::LsnOutOfRange { lsn })?;

        Ok(Some(self.header_len + logical_offset))
    }

    pub fn lsn_to_file_offset(&self, lsn: Lsn) -> Result<u64, WalError> {
        self.checked_lsn_to_file_offset(lsn)?
            .ok_or(WalError::LsnOutOfRange { lsn })
    }

    pub fn checked_file_offset_to_lsn(&self, file_offset: u64) -> Option<Lsn> {
        if file_offset < self.header_len || file_offset > self.file_len {
            return None;
        }

        let logical_offset = file_offset - self.header_len;
        self.base_lsn.checked_add_bytes(logical_offset)
    }

    pub fn remaining_file_capacity(&self, target_segment_size: u64) -> u64 {
        target_segment_size.saturating_sub(self.file_len)
    }

    pub fn can_fit_bytes(&self, additional_bytes: u64, target_segment_size: u64) -> bool {
        additional_bytes <= self.remaining_file_capacity(target_segment_size)
    }

    pub fn set_file_len(&mut self, file_len: u64) -> Result<(), WalError> {
        if file_len < self.header_len {
            return Err(WalError::BadSegmentHeader);
        }

        self.file_len = file_len;
        let _ = self.written_end_lsn()?;
        Ok(())
    }

    pub fn note_bytes_written(&mut self, bytes: u64) -> Result<(), WalError> {
        let new_file_len = self
            .file_len
            .checked_add(bytes)
            .ok_or(WalError::BadSegmentHeader)?;
        self.set_file_len(new_file_len)
    }

    pub fn mark_sealed(&mut self) {
        self.sealed = true;
    }

    pub fn mark_active(&mut self) {
        self.sealed = false;
    }
}

#[derive(Debug)]
pub struct ActiveSegment<F> {
    file: F,
    header: SegmentHeader,
    descriptor: SegmentDescriptor,
}

impl<F: SegmentFile> ActiveSegment<F> {
    pub fn open(file: F, header: SegmentHeader) -> Result<Self, WalError> {
        Self::open_with_sealed(file, header, false)
    }

    pub fn open_with_sealed(
        file: F,
        header: SegmentHeader,
        sealed: bool,
    ) -> Result<Self, WalError> {
        let file_len = file.len()?;
        let descriptor = SegmentDescriptor::from_header_with_sealed(&header, file_len, sealed)?;

        Ok(Self {
            file,
            header,
            descriptor,
        })
    }

    pub fn file(&self) -> &F {
        &self.file
    }

    pub fn file_mut(&mut self) -> &mut F {
        &mut self.file
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    pub fn segment_id(&self) -> SegmentId {
        self.descriptor.segment_id
    }

    pub fn base_lsn(&self) -> Lsn {
        self.descriptor.base_lsn
    }

    pub fn header_len(&self) -> u64 {
        self.descriptor.header_len
    }

    pub fn file_len(&self) -> u64 {
        self.descriptor.file_len
    }

    pub fn written_logical_len(&self) -> u64 {
        self.descriptor.written_logical_len()
    }

    pub fn written_end_lsn(&self) -> Result<Lsn, WalError> {
        self.descriptor.written_end_lsn()
    }

    pub fn is_sealed(&self) -> bool {
        self.descriptor.sealed
    }

    pub fn contains_written_lsn(&self, lsn: Lsn) -> Result<bool, WalError> {
        self.descriptor.contains_written_lsn(lsn)
    }

    pub fn lsn_to_file_offset(&self, lsn: Lsn) -> Result<u64, WalError> {
        self.descriptor.lsn_to_file_offset(lsn)
    }

    pub fn checked_file_offset_to_lsn(&self, file_offset: u64) -> Option<Lsn> {
        self.descriptor.checked_file_offset_to_lsn(file_offset)
    }

    pub fn remaining_file_capacity(&self, target_segment_size: u64) -> u64 {
        self.descriptor.remaining_file_capacity(target_segment_size)
    }

    pub fn can_fit_bytes(&self, additional_bytes: u64, target_segment_size: u64) -> bool {
        self.descriptor
            .can_fit_bytes(additional_bytes, target_segment_size)
    }

    pub fn refresh_len(&mut self) -> Result<u64, WalError> {
        let file_len = self.file.len()?;
        self.descriptor.set_file_len(file_len)?;
        Ok(file_len)
    }

    pub fn note_bytes_written(&mut self, bytes: u64) -> Result<(), WalError> {
        self.descriptor.note_bytes_written(bytes)
    }

    pub fn truncate_file(&mut self, file_len: u64) -> Result<(), WalError> {
        self.file.truncate(file_len)?;
        self.descriptor.set_file_len(file_len)?;
        Ok(())
    }

    pub fn mark_sealed(&mut self) {
        self.descriptor.mark_sealed();
    }

    pub fn mark_active(&mut self) {
        self.descriptor.mark_active();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        format::segment_header::{SegmentHeader, compression_algorithms},
        io::segment_file::{FsSegmentFile, SegmentFile},
        types::WalIdentity,
    };

    struct TestFile {
        path: PathBuf,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos();

            let path = std::env::temp_dir().join(format!(
                "wal-active-segment-{name}-{}-{nanos}",
                process::id()
            ));

            Self { path }
        }

        fn open(&self) -> FsSegmentFile {
            FsSegmentFile::open_append(&self.path).expect("failed to open test file")
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn sample_header(segment_id: SegmentId, base_lsn: Lsn) -> SegmentHeader {
        let mut header = SegmentHeader::new(
            WalIdentity::new(11, 22, 1),
            segment_id,
            base_lsn,
            compression_algorithms::NONE,
            SegmentHeader::SUPPORTED_VERSION,
        );
        header.finalize_checksum();
        header
    }

    fn seed_segment_file(file: &mut FsSegmentFile, header: &SegmentHeader, bytes: &[u8]) {
        file.append_all(&header.encode()).unwrap();
        file.append_all(bytes).unwrap();
    }

    #[test]
    fn descriptor_uses_file_len_minus_header_len_for_written_bytes() {
        let header = sample_header(7, Lsn::new(4096));
        let descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64 + 12)
                .unwrap();

        assert_eq!(descriptor.segment_id, 7);
        assert_eq!(descriptor.base_lsn, Lsn::new(4096));
        assert_eq!(descriptor.header_len, SegmentHeader::ENCODED_LEN as u64);
        assert_eq!(descriptor.file_len, SegmentHeader::ENCODED_LEN as u64 + 12);
        assert_eq!(descriptor.written_logical_len(), 12);
        assert_eq!(descriptor.written_end_lsn().unwrap(), Lsn::new(4108));
    }

    #[test]
    fn lsn_to_file_offset_maps_base_lsn_to_first_data_byte() {
        let header = sample_header(1, Lsn::new(1000));
        let descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64 + 32)
                .unwrap();

        assert_eq!(
            descriptor.lsn_to_file_offset(Lsn::new(1000)).unwrap(),
            SegmentHeader::ENCODED_LEN as u64
        );
        assert_eq!(
            descriptor.lsn_to_file_offset(Lsn::new(1016)).unwrap(),
            SegmentHeader::ENCODED_LEN as u64 + 16
        );
    }

    #[test]
    fn lsn_to_file_offset_rejects_lsn_outside_written_range() {
        let header = sample_header(1, Lsn::new(1000));
        let descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64 + 8).unwrap();

        let before_base = descriptor.lsn_to_file_offset(Lsn::new(999)).unwrap_err();
        let at_tail = descriptor.lsn_to_file_offset(Lsn::new(1008)).unwrap_err();

        assert!(matches!(
            before_base,
            WalError::LsnOutOfRange { lsn } if lsn == Lsn::new(999)
        ));
        assert!(matches!(
            at_tail,
            WalError::LsnOutOfRange { lsn } if lsn == Lsn::new(1008)
        ));
    }

    #[test]
    fn checked_file_offset_to_lsn_inverts_mapping_and_allows_tail_offset() {
        let header = sample_header(5, Lsn::new(8192));
        let descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64 + 20)
                .unwrap();

        assert_eq!(
            descriptor.checked_file_offset_to_lsn(SegmentHeader::ENCODED_LEN as u64),
            Some(Lsn::new(8192))
        );
        assert_eq!(
            descriptor.checked_file_offset_to_lsn(SegmentHeader::ENCODED_LEN as u64 + 20),
            Some(Lsn::new(8212))
        );
        assert_eq!(descriptor.checked_file_offset_to_lsn(0), None);
    }

    #[test]
    fn note_bytes_written_advances_file_len_and_written_end_lsn() {
        let header = sample_header(2, Lsn::new(500));
        let mut descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64).unwrap();

        descriptor.note_bytes_written(9).unwrap();

        assert_eq!(descriptor.file_len, SegmentHeader::ENCODED_LEN as u64 + 9);
        assert_eq!(descriptor.written_end_lsn().unwrap(), Lsn::new(509));
    }

    #[test]
    fn set_file_len_rejects_lengths_shorter_than_header() {
        let header = sample_header(2, Lsn::new(0));
        let mut descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64).unwrap();

        let err = descriptor.set_file_len(10).unwrap_err();
        assert!(matches!(err, WalError::BadSegmentHeader));
    }

    #[test]
    fn can_fit_bytes_checks_target_segment_size_against_current_file_len() {
        let header = sample_header(3, Lsn::new(0));
        let descriptor =
            SegmentDescriptor::from_header(&header, SegmentHeader::ENCODED_LEN as u64 + 100)
                .unwrap();

        assert_eq!(descriptor.remaining_file_capacity(200), 32);
        assert!(descriptor.can_fit_bytes(32, 200));
        assert!(!descriptor.can_fit_bytes(33, 200));
    }

    #[test]
    fn active_segment_open_reads_existing_file_len() {
        let test_file = TestFile::new("open");
        let mut file = test_file.open();
        let header = sample_header(7, Lsn::new(4096));

        seed_segment_file(&mut file, &header, b"hello world");

        let segment = ActiveSegment::open(file, header).unwrap();

        assert_eq!(segment.segment_id(), 7);
        assert_eq!(segment.base_lsn(), Lsn::new(4096));
        assert_eq!(segment.file_len(), SegmentHeader::ENCODED_LEN as u64 + 11);
        assert_eq!(segment.written_logical_len(), 11);
        assert_eq!(segment.written_end_lsn().unwrap(), Lsn::new(4107));
    }

    #[test]
    fn active_segment_refresh_len_observes_file_growth() {
        let test_file = TestFile::new("refresh");
        let mut file = test_file.open();
        let header = sample_header(9, Lsn::new(100));

        seed_segment_file(&mut file, &header, b"abc");

        let mut segment = ActiveSegment::open(file, header).unwrap();
        segment.file_mut().append_all(b"defgh").unwrap();

        assert_eq!(segment.file_len(), SegmentHeader::ENCODED_LEN as u64 + 3);

        segment.refresh_len().unwrap();

        assert_eq!(segment.file_len(), SegmentHeader::ENCODED_LEN as u64 + 8);
        assert_eq!(segment.written_end_lsn().unwrap(), Lsn::new(108));
    }

    #[test]
    fn active_segment_truncate_updates_cached_mapping() {
        let test_file = TestFile::new("truncate");
        let mut file = test_file.open();
        let header = sample_header(11, Lsn::new(2048));

        seed_segment_file(&mut file, &header, b"abcdef");

        let mut segment = ActiveSegment::open(file, header).unwrap();
        segment
            .truncate_file(SegmentHeader::ENCODED_LEN as u64 + 2)
            .unwrap();

        assert_eq!(segment.file_len(), SegmentHeader::ENCODED_LEN as u64 + 2);
        assert_eq!(segment.written_logical_len(), 2);
        assert_eq!(segment.written_end_lsn().unwrap(), Lsn::new(2050));
    }

    #[test]
    fn active_segment_sealed_flag_can_be_toggled() {
        let test_file = TestFile::new("sealed");
        let mut file = test_file.open();
        let header = sample_header(13, Lsn::new(0));

        seed_segment_file(&mut file, &header, b"");

        let mut segment = ActiveSegment::open(file, header).unwrap();

        assert!(!segment.is_sealed());

        segment.mark_sealed();
        assert!(segment.is_sealed());

        segment.mark_active();
        assert!(!segment.is_sealed());
    }
}
