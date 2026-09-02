//! Linux page-cache fast path: `preadv2(RWF_NOWAIT)`. The kernel itself
//! refuses to block, answering `EAGAIN` when the data is not resident and
//! `EOPNOTSUPP` on filesystems without `FMODE_NOWAIT` (tmpfs, notably), so the
//! guarantee that nothing here stalls the event loop is the kernel's, not ours.

use std::io;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::unix::{kernel_offset, regular_file_size};

/// `preadv2` flag: serve only from page cache, return EAGAIN rather than
/// blocking on the device. Linux 4.14+; not exposed by the libc crate.
pub(crate) const RWF_NOWAIT: libc::c_int = 0x0000_0008;

/// `openat2` resolve flag: fail with `EAGAIN` rather than perform any I/O to
/// resolve the path. Linux 5.12+.
const RESOLVE_CACHED: u64 = 0x20;

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

pub(super) struct Inner {
    fd: i32,
}

impl Inner {
    pub(super) fn new(fd: i32) -> Self {
        Inner { fd }
    }

    pub(super) fn size(&self) -> Option<u64> {
        regular_file_size(self.fd)
    }

    /// Fill `[base, base + len)` from `pos` with page-cache-only reads; the
    /// count stops short at EOF. `None` when some part would have blocked, the
    /// filesystem refuses `RWF_NOWAIT`, or the offset does not fit `off_t`.
    ///
    /// # Safety
    /// `base` must own at least `len` writable bytes for the whole call.
    pub(super) unsafe fn fill(&mut self, pos: u64, base: *mut u8, len: usize) -> Option<usize> {
        if self.fd < 0 {
            return None;
        }
        let mut filled = 0usize;
        while filled < len {
            let iov = libc::iovec {
                iov_base: unsafe { base.add(filled) } as *mut libc::c_void,
                iov_len: len - filled,
            };
            let off = kernel_offset(pos, filled)?;
            let n = unsafe { libc::preadv2(self.fd, &iov, 1, off, RWF_NOWAIT) };
            if n < 0 {
                if io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return None;
            }
            if n == 0 {
                break;
            }
            filled += n as usize;
        }
        Some(filled)
    }

    /// Whether the file still ends at `size`: one page-cache-only byte past it
    /// must read as EOF. Anything else, a cold page included, is "unknown".
    pub(super) fn at_eof(&self, size: u64) -> bool {
        let Some(off) = kernel_offset(size, 0) else {
            return false;
        };
        let mut tail = 0u8;
        let probe = libc::iovec {
            iov_base: &mut tail as *mut u8 as *mut libc::c_void,
            iov_len: 1,
        };
        // Safety: `probe` describes one live stack byte for the duration of the call.
        unsafe { libc::preadv2(self.fd, &probe, 1, off, RWF_NOWAIT) == 0 }
    }

    /// Whether a `RWF_NOWAIT` read can ever succeed on this file. Only a
    /// completed probe (a byte, or EOF) or `EAGAIN` proves it; filesystems that
    /// set no `FMODE_NOWAIT` answer `EOPNOTSUPP`, and any other error belongs
    /// to the async path, which is authoritative for reporting it.
    pub(super) fn supported(&mut self) -> bool {
        if self.fd < 0 {
            return false;
        }
        let mut byte = 0u8;
        let iov = libc::iovec {
            iov_base: &mut byte as *mut u8 as *mut libc::c_void,
            iov_len: 1,
        };
        loop {
            // Safety: `iov` describes one live stack byte for the duration of the call.
            let n = unsafe { libc::preadv2(self.fd, &iov, 1, 0, RWF_NOWAIT) };
            if n >= 0 {
                return true;
            }
            match io::Error::last_os_error().raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) => return true,
                _ => return false,
            }
        }
    }
}

/// Closes its fd on every exit path, including the early returns below.
struct OwnedFd(i32);

impl Drop for OwnedFd {
    fn drop(&mut self) {
        // Safety: we own this descriptor and drop runs exactly once.
        unsafe { libc::close(self.0) };
    }
}

/// Whole-file fast path: open, size, read and close entirely on the calling
/// thread, with every step refusing to block. `openat2(RESOLVE_CACHED)` fails
/// rather than walk a path that needs I/O, and `preadv2(RWF_NOWAIT)` fails
/// rather than wait on the device, so nothing here can stall the event loop.
pub(super) fn read_file(py: Python<'_>, path: PathBuf) -> PyResult<Option<PyObject>> {
    use std::os::unix::ffi::OsStrExt;
    let Ok(cpath) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return Ok(None);
    };
    let how = OpenHow {
        flags: (libc::O_RDONLY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_CACHED,
    };
    // Safety: `cpath` and `how` outlive the call; openat2 only reads them.
    let raw = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            libc::AT_FDCWD,
            cpath.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if raw < 0 {
        // EAGAIN (uncached path), ENOSYS (pre-5.12), ENOENT, EPERM: in every
        // case let the async path produce the authoritative result or error.
        return Ok(None);
    }
    let fd = OwnedFd(raw as i32);
    let mut inner = Inner::new(fd.0);
    let Some(size) = inner.size() else {
        return Ok(None);
    };
    let Ok(len) = usize::try_from(size) else {
        return Ok(None);
    };
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let base = bytes.as_bytes().as_ptr() as *mut u8;
    // Safety: `bytes` owns `len` writable bytes for the duration of the fill.
    if unsafe { inner.fill(0, base, len) } != Some(len) {
        return Ok(None); // cold, or shorter than fstat claimed: the async path settles it
    }
    if !inner.at_eof(size) {
        return Ok(None); // grew since fstat, or could not be confirmed
    }
    Ok(Some(bytes.into_any().unbind()))
}
