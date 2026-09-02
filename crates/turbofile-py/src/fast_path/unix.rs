//! Helpers shared by the Unix fast paths.

/// Size of the regular file behind `fd`, or `None` for anything that is not a
/// regular file: only those have a size worth trusting for a read-to-end.
pub(super) fn regular_file_size(fd: i32) -> Option<u64> {
    if fd < 0 {
        return None;
    }
    // Safety: `st` is a live, correctly sized `struct stat` that fstat fills.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return None;
    }
    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        return None;
    }
    Some(st.st_size.max(0) as u64)
}

/// Kernel offset for `pos + filled`, or `None` when the caller-supplied
/// position does not fit `off_t`. A plain cast would wrap negative and lean on
/// the kernel's `EINVAL` to reach the fallback; declining here keeps it explicit.
pub(super) fn kernel_offset(pos: u64, filled: usize) -> Option<libc::off_t> {
    libc::off_t::try_from(pos.checked_add(filled as u64)?).ok()
}
