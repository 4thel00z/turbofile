//! Darwin page-cache fast path. There is no `RWF_NOWAIT` here, but `mincore`
//! on a `PROT_NONE` mapping of the file reports which pages the unified buffer
//! cache holds, so a read is served inline only when every page it touches is
//! resident. The bytes are copied by `pread`, never by dereferencing the
//! mapping, so a truncate from elsewhere cannot fault this process. Check and
//! copy are two syscalls, so a page evicted between them makes that one
//! `pread` wait for the disk: rare, bounded, and never wrong.

use std::io;
use std::sync::OnceLock;

use super::unix::{kernel_offset, regular_file_size};

/// Address space reserved past the file's current end, so a file that is
/// being appended to keeps being served without a fresh mapping per step.
const HEADROOM: u64 = 1 << 20;

/// `mincore` vector flag: the page is resident in the buffer cache.
const MINCORE_INCORE: u8 = 0x1;

/// Pages examined per `mincore` call; larger ranges loop.
const FLAGS_PER_CALL: usize = 256;

pub(super) struct Inner {
    fd: i32,
    view: Option<View>,
}

/// A `PROT_NONE`, `MAP_SHARED` mapping of the file's first `len` bytes, held
/// only so `mincore` has an address range to describe. Never dereferenced.
struct View {
    addr: usize,
    len: usize,
}

impl Drop for View {
    fn drop(&mut self) {
        // Safety: `addr`/`len` came from a successful mmap and are unmapped exactly once.
        unsafe { libc::munmap(self.addr as *mut libc::c_void, self.len) };
    }
}

fn page_size() -> usize {
    static PAGE: OnceLock<usize> = OnceLock::new();
    // Safety: sysconf has no preconditions.
    *PAGE.get_or_init(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as usize)
}

/// Mapping length that covers a file of `size` bytes plus headroom.
fn view_len(size: u64) -> Option<usize> {
    let page = page_size() as u64;
    let rounded = size.checked_add(page - 1)? / page * page;
    usize::try_from(rounded.checked_add(HEADROOM)?).ok()
}

impl Inner {
    pub(super) fn new(fd: i32) -> Self {
        Inner { fd, view: None }
    }

    pub(super) fn size(&self) -> Option<u64> {
        regular_file_size(self.fd)
    }

    fn view_covers(&self, end: u64) -> bool {
        self.view.as_ref().is_some_and(|v| end <= v.len as u64)
    }

    /// Make the view reach `end`, remapping only when the file itself has
    /// grown past the current view. `false` when `end` lies beyond the file
    /// plus headroom, or the descriptor cannot be mapped at all.
    fn ensure_view(&mut self, end: u64) -> bool {
        if self.view_covers(end) {
            return true;
        }
        let Some(size) = regular_file_size(self.fd) else {
            return false;
        };
        let Some(len) = view_len(size) else {
            return false;
        };
        if self.view.as_ref().is_some_and(|v| v.len >= len) {
            return false;
        }
        // Safety: a PROT_NONE shared mapping of `fd`; nothing ever dereferences it.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_SHARED,
                self.fd,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return false;
        }
        self.view = Some(View {
            addr: addr as usize,
            len,
        });
        self.view_covers(end)
    }

    /// Whether every page of `[pos, pos + len)` that holds file data is
    /// resident right now. Pages at or past EOF need not be: `pread` stops
    /// there on its own, so a read straddling the end is served short, and the
    /// size is consulted only once a non-resident page has already ruled out
    /// the pure fast path.
    fn resident(&mut self, pos: u64, len: usize) -> bool {
        let Some(end) = pos.checked_add(len as u64) else {
            return false;
        };
        if !self.ensure_view(end) {
            return false;
        }
        let view = self.view.as_ref().expect("ensure_view succeeded");
        let page = page_size();
        let first = pos as usize / page * page;
        let last = (end as usize).div_ceil(page) * page;
        let mut flags = [0u8; FLAGS_PER_CALL];
        let mut off = first;
        while off < last {
            let chunk = (last - off).min(FLAGS_PER_CALL * page);
            // Safety: `[addr + off, addr + off + chunk)` lies inside the live
            // mapping and `flags` has one byte for every page of `chunk`.
            let rc = unsafe {
                libc::mincore(
                    (view.addr + off) as *const libc::c_void,
                    chunk,
                    flags.as_mut_ptr() as *mut libc::c_char,
                )
            };
            if rc != 0 {
                return false;
            }
            let cold = flags[..chunk / page]
                .iter()
                .position(|f| f & MINCORE_INCORE == 0);
            if let Some(index) = cold {
                let cold_page = (off + index * page) as u64;
                return regular_file_size(self.fd).is_some_and(|size| size <= cold_page);
            }
            off += chunk;
        }
        true
    }

    /// Fill `[base, base + len)` from `pos`, only when every page involved is
    /// resident; the count stops short at EOF. `None` means take the async path.
    ///
    /// # Safety
    /// `base` must own at least `len` writable bytes for the whole call.
    pub(super) unsafe fn fill(&mut self, pos: u64, base: *mut u8, len: usize) -> Option<usize> {
        if !self.resident(pos, len) {
            return None;
        }
        let mut filled = 0usize;
        while filled < len {
            let off = kernel_offset(pos, filled)?;
            let n = unsafe {
                libc::pread(
                    self.fd,
                    base.add(filled) as *mut libc::c_void,
                    len - filled,
                    off,
                )
            };
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

    /// Whether the file still ends at `size`: a second `fstat` agreeing with
    /// the first means nothing was appended while the bytes were copied.
    pub(super) fn at_eof(&self, size: u64) -> bool {
        regular_file_size(self.fd) == Some(size)
    }

    /// Whether this descriptor can ever be served: a regular file that mmap
    /// accepts. Directories, pipes and ttys are declined here once, so the
    /// caller stops probing them.
    pub(super) fn supported(&mut self) -> bool {
        self.view.is_some() || self.ensure_view(0)
    }
}
