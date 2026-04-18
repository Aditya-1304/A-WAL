use std::ops::Range;

pub use crc32c::crc32c;

use crate::error::WalError;

pub fn put_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

pub fn put_u16_le(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn put_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn put_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(bytes);
}

pub fn read_u8(input: &[u8], offset: &mut usize) -> Result<u8, WalError> {
    let bytes = read_bytes(input, offset, 1)?;
    Ok(bytes[0])
}

pub fn read_u16_le(input: &[u8], offset: &mut usize) -> Result<u16, WalError> {
    let bytes = read_array::<2>(input, offset)?;
    Ok(u16::from_le_bytes(bytes))
}

pub fn read_u32_le(input: &[u8], offset: &mut usize) -> Result<u32, WalError> {
    let bytes = read_array::<4>(input, offset)?;
    Ok(u32::from_le_bytes(bytes))
}

pub fn read_u64_le(input: &[u8], offset: &mut usize) -> Result<u64, WalError> {
    let bytes = read_array::<8>(input, offset)?;
    Ok(u64::from_le_bytes(bytes))
}

pub fn read_array<const N: usize>(input: &[u8], offset: &mut usize) -> Result<[u8; N], WalError> {
    let bytes = read_bytes(input, offset, N)?;
    let mut array = [0u8; N];
    array.copy_from_slice(bytes);
    Ok(array)
}

pub fn read_bytes<'a>(
    input: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], WalError> {
    let end = offset.checked_add(len).ok_or(WalError::ShortRead)?;

    if end > input.len() {
        return Err(WalError::ShortRead);
    }

    let bytes = &input[*offset..end];
    *offset = end;
    Ok(bytes)
}

pub fn remaining_bytes(input: &[u8], offset: usize) -> usize {
    input.len().saturating_sub(offset)
}

pub fn is_all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|&byte| byte == 0)
}

pub fn copy_with_zeroed_range(input: &[u8], range: Range<usize>) -> Vec<u8> {
    let start = range.start;
    let end = range.end;

    assert!(start <= end, "invalid zero range: start > end");
    assert!(
        end <= input.len(),
        "invalid zero range: end exceeds input length"
    );

    let mut copy = input.to_vec();
    copy[start..end].fill(0);
    copy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::WalError;

    #[test]
    fn put_and_read_primitives_round_trip() {
        let mut buf = Vec::new();

        put_u8(&mut buf, 0xAB);
        put_u16_le(&mut buf, 0x1122);
        put_u32_le(&mut buf, 0x33445566);
        put_u64_le(&mut buf, 0x778899AABBCCDDEE);

        let mut offset = 0;

        assert_eq!(read_u8(&buf, &mut offset).unwrap(), 0xAB);
        assert_eq!(read_u16_le(&buf, &mut offset).unwrap(), 0x1122);
        assert_eq!(read_u32_le(&buf, &mut offset).unwrap(), 0x33445566);
        assert_eq!(read_u64_le(&buf, &mut offset).unwrap(), 0x778899AABBCCDDEE);
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn read_bytes_returns_requested_slice_and_advances_offset() {
        let input = [10, 20, 30, 40, 50];
        let mut offset = 1;

        let bytes = read_bytes(&input, &mut offset, 3).unwrap();

        assert_eq!(bytes, &[20, 30, 40]);
        assert_eq!(offset, 4);
    }

    #[test]
    fn read_array_returns_fixed_size_array() {
        let input = [1, 2, 3, 4, 5, 6];
        let mut offset = 2;

        let array = read_array::<3>(&input, &mut offset).unwrap();

        assert_eq!(array, [3, 4, 5]);
        assert_eq!(offset, 5);
    }

    #[test]
    fn short_read_returns_error() {
        let input = [0x11];
        let mut offset = 0;

        let result = read_u16_le(&input, &mut offset);

        assert!(matches!(result, Err(WalError::ShortRead)));
    }

    #[test]
    fn remaining_bytes_reports_unread_tail() {
        let input = [1, 2, 3, 4, 5];
        assert_eq!(remaining_bytes(&input, 0), 5);
        assert_eq!(remaining_bytes(&input, 2), 3);
        assert_eq!(remaining_bytes(&input, 5), 0);
        assert_eq!(remaining_bytes(&input, 10), 0);
    }

    #[test]
    fn is_all_zero_detects_zeroed_and_non_zeroed_slices() {
        assert!(is_all_zero(&[0, 0, 0, 0]));
        assert!(!is_all_zero(&[0, 1, 0, 0]));
    }

    #[test]
    fn copy_with_zeroed_range_zeros_only_selected_bytes() {
        let input = [1, 2, 3, 4, 5, 6];

        let output = copy_with_zeroed_range(&input, 2..5);

        assert_eq!(output, vec![1, 2, 0, 0, 0, 6]);
        assert_eq!(input, [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn crc32c_matches_known_vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}
