use std::{
    error::Error,
    fmt::{self},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBuffer {
    bytes: Vec<u8>,
    capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendBufferError {
    Overflow { attempted: usize, remaining: usize },
}

impl fmt::Display for AppendBufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendBufferError::Overflow {
                attempted,
                remaining,
            } => write!(
                f,
                "append buffer overflow: attempted to append {attempted} bytes with only {remaining} bytes remaning"
            ),
        }
    }
}

impl Error for AppendBufferError {}

impl AppendBuffer {
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "append buffer capacity must be greater than zero"
        );

        Self {
            bytes: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.bytes.len()
    }

    pub fn can_fit(&self, additional_bytes: usize) -> bool {
        additional_bytes <= self.remaining_capacity()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<(), AppendBufferError> {
        if !self.can_fit(bytes.len()) {
            return Err(AppendBufferError::Overflow {
                attempted: bytes.len(),
                remaining: self.remaining_capacity(),
            });
        }

        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub fn append_parts(&mut self, parts: &[&[u8]]) -> Result<(), AppendBufferError> {
        let additional_bytes: usize = parts.iter().map(|part| part.len()).sum();

        if !self.can_fit(additional_bytes) {
            return Err(AppendBufferError::Overflow {
                attempted: additional_bytes,
                remaining: self.remaining_capacity(),
            });
        }

        for part in parts {
            self.bytes.extend_from_slice(part);
        }

        Ok(())
    }

    pub fn preferred_drain_len(&self, storage_write_unit: usize) -> usize {
        if self.bytes.is_empty() {
            return 0;
        }

        if storage_write_unit == 0 {
            return self.bytes.len();
        }

        (self.bytes.len() / storage_write_unit) * storage_write_unit
    }

    pub fn drain_preferred_chunk(&mut self, storage_write_unit: usize) -> Vec<u8> {
        let drain_len = self.preferred_drain_len(storage_write_unit);

        if drain_len == 0 {
            return Vec::new();
        }

        self.bytes.drain(..drain_len).collect()
    }

    pub fn drain_all(&mut self) -> Vec<u8> {
        self.bytes.drain(..).collect()
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_starts_empty() {
        let buffer = AppendBuffer::new(1024);

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 1024);
        assert_eq!(buffer.remaining_capacity(), 1024);
    }

    #[test]
    fn append_grows_buffer() {
        let mut buffer = AppendBuffer::new(16);

        buffer.append(b"hello").unwrap();

        assert_eq!(buffer.len(), 5);
        assert_eq!(buffer.as_slice(), b"hello");
        assert_eq!(buffer.remaining_capacity(), 11);
    }

    #[test]
    fn append_parts_appends_multiple_slices_in_order() {
        let mut buffer = AppendBuffer::new(16);

        buffer.append_parts(&[b"head", b"-", b"tail"]).unwrap();

        assert_eq!(buffer.as_slice(), b"head-tail");
        assert_eq!(buffer.len(), 9);
    }

    #[test]
    fn append_rejects_overflow_without_mutating_buffer() {
        let mut buffer = AppendBuffer::new(8);
        buffer.append(b"hello").unwrap();

        let err = buffer.append(b"world").unwrap_err();

        assert_eq!(
            err,
            AppendBufferError::Overflow {
                attempted: 5,
                remaining: 3,
            }
        );
        assert_eq!(buffer.as_slice(), b"hello");
        assert_eq!(buffer.len(), 5);
    }

    #[test]
    fn append_parts_rejects_overflow_without_partial_append() {
        let mut buffer = AppendBuffer::new(8);
        buffer.append(b"abc").unwrap();

        let err = buffer.append_parts(&[b"de", b"fghi"]).unwrap_err();

        assert_eq!(
            err,
            AppendBufferError::Overflow {
                attempted: 6,
                remaining: 5,
            }
        );
        assert_eq!(buffer.as_slice(), b"abc");
    }

    #[test]
    fn preferred_drain_len_rounds_down_to_write_unit_multiple() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"abcdefghijklmnopqr").unwrap();

        assert_eq!(buffer.len(), 18);
        assert_eq!(buffer.preferred_drain_len(8), 16);
    }

    #[test]
    fn drain_preferred_chunk_returns_largest_full_write_unit_prefix() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"abcdefghijklmnopqr").unwrap();

        let drained = buffer.drain_preferred_chunk(8);

        assert_eq!(drained, b"abcdefghijklmnop".to_vec());
        assert_eq!(buffer.as_slice(), b"qr");
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn drain_preferred_chunk_returns_empty_when_less_than_one_write_unit_is_buffered() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"hello").unwrap();

        let drained = buffer.drain_preferred_chunk(8);

        assert!(drained.is_empty());
        assert_eq!(buffer.as_slice(), b"hello");
    }

    #[test]
    fn drain_preferred_chunk_with_zero_write_unit_drains_everything() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"hello world").unwrap();

        let drained = buffer.drain_preferred_chunk(0);

        assert_eq!(drained, b"hello world".to_vec());
        assert!(buffer.is_empty());
    }

    #[test]
    fn drain_all_returns_all_bytes_and_empties_buffer() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"abcdef").unwrap();

        let drained = buffer.drain_all();

        assert_eq!(drained, b"abcdef".to_vec());
        assert!(buffer.is_empty());
        assert_eq!(buffer.remaining_capacity(), 32);
    }

    #[test]
    fn clear_empties_buffer_without_returning_bytes() {
        let mut buffer = AppendBuffer::new(32);
        buffer.append(b"abcdef").unwrap();

        buffer.clear();

        assert!(buffer.is_empty());
        assert_eq!(buffer.remaining_capacity(), 32);
    }
}
