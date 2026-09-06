//! Helpers shared by the Unix fast paths.

pub(super) use turbofile_core::regular_file_size;

/// Kernel offset for `pos + filled`, or `None` when the caller-supplied
/// position does not fit `off_t`. A plain cast would wrap negative and lean on
/// the kernel's `EINVAL` to reach the fallback; declining here keeps it explicit.
pub(super) fn kernel_offset(pos: u64, filled: usize) -> Option<libc::off_t> {
    libc::off_t::try_from(pos.checked_add(filled as u64)?).ok()
}
