//! Page-cache fast path: reads served inline on the calling thread when every
//! byte is already resident, so hot data never pays the submit -> driver
//! thread -> doorbell -> drain round trip. Each platform supplies an `Inner`
//! with the same operations; `None` from any of them means "take the async
//! path", never an error, and the async path stays authoritative for errors.

use std::path::PathBuf;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[cfg(unix)]
mod unix;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::Inner;
#[cfg(target_os = "linux")]
pub(crate) use linux::RWF_NOWAIT;

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "macos")]
use darwin::Inner;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod none;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use none::Inner;

/// Per-file fast-path state, owned by the Python `BinaryFile` for as long as
/// the file is open.
#[pyclass(name = "FastPath")]
pub(crate) struct FastPath {
    inner: Inner,
}

#[pymethods]
impl FastPath {
    #[new]
    fn new(fd: i32) -> Self {
        FastPath {
            inner: Inner::new(fd),
        }
    }

    /// Up to `len` bytes at `pos` as `bytes`, short only at EOF, or `None`
    /// when the fast path does not apply.
    fn read(&mut self, py: Python<'_>, pos: u64, len: usize) -> PyResult<Option<PyObject>> {
        if len == 0 {
            return Ok(None);
        }
        let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
        let base = bytes.as_bytes().as_ptr() as *mut u8;
        // Safety: `bytes` owns `len` writable bytes for the whole call.
        let Some(filled) = (unsafe { self.inner.fill(pos, base, len) }) else {
            return Ok(None);
        };
        if filled == len {
            return Ok(Some(bytes.into_any().unbind()));
        }
        Ok(Some(
            PyBytes::new(py, &bytes.as_bytes()[..filled])
                .into_any()
                .unbind(),
        ))
    }

    /// `readinto` counterpart of `read`: fills a caller-owned buffer and
    /// returns the byte count, or `None` when the fast path does not apply.
    fn readinto(&mut self, pos: u64, buffer: Bound<'_, PyAny>) -> PyResult<Option<usize>> {
        let buffer = PyBuffer::<u8>::get(&buffer)?;
        if buffer.readonly() {
            return Err(PyValueError::new_err("readinto needs a writable buffer"));
        }
        if !buffer.is_c_contiguous() {
            return Err(PyValueError::new_err("buffer must be C-contiguous"));
        }
        let len = buffer.len_bytes();
        if len == 0 {
            return Ok(None);
        }
        // Safety: the PyBuffer pins `len` bytes for the lifetime of this call.
        Ok(unsafe { self.inner.fill(pos, buffer.buf_ptr() as *mut u8, len) })
    }

    /// Everything from `pos` to EOF -- the `f.read()` case, where the size
    /// comes from `fstat` -- or `None` when any part is not resident or the
    /// file changed size while it was being copied.
    fn read_all(&mut self, py: Python<'_>, pos: u64) -> PyResult<Option<PyObject>> {
        let Some(size) = self.inner.size() else {
            return Ok(None);
        };
        let Ok(len) = usize::try_from(size.saturating_sub(pos)) else {
            return Ok(None);
        };
        let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
        let base = bytes.as_bytes().as_ptr() as *mut u8;
        // Safety: `bytes` owns `len` writable bytes for the duration of the fill.
        if len > 0 && unsafe { self.inner.fill(pos, base, len) } != Some(len) {
            return Ok(None);
        }
        if !self.inner.at_eof(size) {
            return Ok(None);
        }
        Ok(Some(bytes.into_any().unbind()))
    }

    /// The file's current size from an inline `fstat`, or `None` for anything
    /// that is not a regular file. Lets a large read-all skip the driver round
    /// trips for its size snapshot and its end-of-file check.
    fn size(&self) -> Option<u64> {
        self.inner.size()
    }

    /// Whether this file can ever take the fast path. The caller asks once
    /// after a `None` and drops the object when the answer is no, so a file
    /// that can never be served pays one probe rather than one per read.
    fn supported(&mut self) -> bool {
        self.inner.supported()
    }
}

/// Whole-file fast path by path: open, size, read and close on the calling
/// thread with every step refusing to block. Returns `None` whenever any step
/// would have blocked, leaving the caller to submit the ordinary async read.
#[cfg(target_os = "linux")]
#[pyfunction]
pub(crate) fn try_read_file(py: Python<'_>, path: PathBuf) -> PyResult<Option<PyObject>> {
    linux::read_file(py, path)
}

/// Only Linux has a cached-only `open` (`openat2(RESOLVE_CACHED)`). On Darwin
/// an inline `open` stalls on cold paths and endpoint-security scans, so
/// `read_bytes` keeps the async path there.
#[cfg(not(target_os = "linux"))]
#[pyfunction]
pub(crate) fn try_read_file(_py: Python<'_>, _path: PathBuf) -> PyResult<Option<PyObject>> {
    Ok(None)
}
