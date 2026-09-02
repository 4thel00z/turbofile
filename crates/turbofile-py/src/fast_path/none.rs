//! Platforms with no way to ask about page-cache residency without blocking:
//! every read takes the async path.

pub(super) struct Inner;

impl Inner {
    pub(super) fn new(_fd: i32) -> Self {
        Inner
    }

    pub(super) fn size(&self) -> Option<u64> {
        None
    }

    /// # Safety
    /// Never touches `base`; the signature matches the other platforms.
    pub(super) unsafe fn fill(&mut self, _pos: u64, _base: *mut u8, _len: usize) -> Option<usize> {
        None
    }

    pub(super) fn at_eof(&self, _size: u64) -> bool {
        false
    }

    pub(super) fn supported(&mut self) -> bool {
        false
    }
}
