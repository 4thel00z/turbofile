//! Extension module `turbofile._turbofile`: submits ops to the turbofile-core
//! driver and completes asyncio futures through per-loop completion queues.
//! Each burst of completions costs one `call_soon_threadsafe` (the doorbell);
//! a drain callback on the loop thread delivers every queued completion and is
//! the only place Python-owned buffers are dropped.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use pyo3::BoundObject;

use turbofile_core::{BackendKind, Dest, Driver, Op, OpenSpec, Payload, Reply};

/// Cleared by `shutdown()` (registered with atexit): once the interpreter is
/// finalizing, driver-thread completions must never attach to Python again.
static ALIVE: AtomicBool = AtomicBool::new(true);

static GET_RUNNING_LOOP: GILOnceCell<PyObject> = GILOnceCell::new();

#[cfg(target_os = "linux")]
thread_local! {
    /// Private ring owned by `probe_inline_read`, never shared with the driver.
    static PROBE_RING: std::cell::RefCell<Option<io_uring::IoUring>> =
        const { std::cell::RefCell::new(None) };
}

struct Globals {
    driver: Mutex<Option<(u32, Arc<Driver>)>>,
    bridges: Mutex<HashMap<usize, Arc<LoopBridge>>>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

fn globals() -> &'static Globals {
    GLOBALS.get_or_init(|| Globals {
        driver: Mutex::new(None),
        bridges: Mutex::new(HashMap::new()),
    })
}

fn backend_kind() -> PyResult<BackendKind> {
    let Ok(value) = std::env::var("TURBOFILE_BACKEND") else {
        return Ok(BackendKind::default_for_platform());
    };
    match value.as_str() {
        "compio" => Ok(BackendKind::Compio),
        #[cfg(target_os = "macos")]
        "aio" | "darwin-aio" => Ok(BackendKind::DarwinAio),
        other => Err(PyValueError::new_err(format!(
            "unknown TURBOFILE_BACKEND: {other:?}"
        ))),
    }
}

/// The driver is keyed by pid so a forked child rebuilds its own driver
/// thread instead of submitting into the parent's dead one.
fn driver() -> PyResult<Arc<Driver>> {
    let pid = std::process::id();
    let mut guard = globals().driver.lock().unwrap();
    if let Some((stored, driver)) = guard.as_ref() {
        if *stored == pid {
            return Ok(driver.clone());
        }
        globals().bridges.lock().unwrap().clear();
    }
    let driver = Arc::new(Driver::new(backend_kind()?).map_err(io_err_to_pyerr)?);
    *guard = Some((pid, driver.clone()));
    Ok(driver)
}

/// Python-owned memory held only so it stays alive and pinned until the
/// drain, which runs with the GIL, drops it.
enum Owner {
    Buffer(#[allow(dead_code)] PyBuffer<u8>),
}

enum Output {
    Plain,
    FillBytes { bytes: Py<PyBytes> },
    ReadInto,
}

struct Completion {
    fut: PyObject,
    result: io::Result<Reply>,
    output: Output,
    owner: Option<Owner>,
}

struct LoopBridge {
    key: usize,
    queue: Mutex<Vec<Completion>>,
    armed: AtomicBool,
    event_loop: PyObject,
    call_soon_threadsafe: PyObject,
    drain: OnceLock<PyObject>,
}

impl LoopBridge {
    /// Driver-thread side of the doorbell: push, then ring only on the
    /// idle→armed transition.
    fn complete(&self, completion: Completion) {
        self.queue.lock().unwrap().push(completion);
        if self.armed.swap(true, Ordering::AcqRel) {
            return;
        }
        if !ALIVE.load(Ordering::Acquire) {
            return;
        }
        Python::with_gil(|py| {
            let drain = self.drain.get().expect("drain set at construction");
            let rung = self.call_soon_threadsafe.bind(py).call1((drain.bind(py),));
            if rung.is_ok() {
                return;
            }
            // The loop is closed: nothing will ever drain, so drop the
            // queued completions while the GIL is held.
            let stale: Vec<Completion> = std::mem::take(&mut *self.queue.lock().unwrap());
            drop(stale);
            self.armed.store(false, Ordering::Release);
            globals().bridges.lock().unwrap().remove(&self.key);
        });
    }
}

#[pyclass]
struct DrainHandle {
    bridge: Weak<LoopBridge>,
}

#[pymethods]
impl DrainHandle {
    /// Loop-thread side of the doorbell: deliver until the queue stays empty,
    /// re-checking after disarming so a push racing the disarm is never lost.
    fn __call__(&self, py: Python<'_>) {
        let Some(bridge) = self.bridge.upgrade() else {
            return;
        };
        loop {
            let batch: Vec<Completion> = std::mem::take(&mut *bridge.queue.lock().unwrap());
            if batch.is_empty() {
                bridge.armed.store(false, Ordering::Release);
                if bridge.queue.lock().unwrap().is_empty() {
                    return;
                }
                if bridge.armed.swap(true, Ordering::AcqRel) {
                    return;
                }
                continue;
            }
            for completion in batch {
                deliver(py, completion);
            }
        }
    }
}

fn deliver(py: Python<'_>, completion: Completion) {
    let Completion {
        fut,
        result,
        output,
        owner,
    } = completion;
    let fut = fut.bind(py);
    let done = fut
        .call_method0("done")
        .and_then(|flag| flag.extract::<bool>())
        .unwrap_or(true);
    if !done {
        match result {
            Err(e) => {
                let exc = io_err_to_pyerr(e).into_value(py);
                fut.call_method1("set_exception", (exc,)).ok();
            }
            Ok(reply) => match build_value(py, reply, output) {
                Ok(value) => {
                    fut.call_method1("set_result", (value,)).ok();
                }
                Err(e) => {
                    fut.call_method1("set_exception", (e.into_value(py),)).ok();
                }
            },
        }
    }
    drop(owner);
}

fn build_value(py: Python<'_>, reply: Reply, output: Output) -> PyResult<PyObject> {
    match (output, reply) {
        (Output::FillBytes { bytes }, Reply::Read { n }) => {
            let bound = bytes.bind(py);
            if bound.len()? == n {
                return Ok(bytes.into_any());
            }
            let short = PyBytes::new(py, &bound.as_bytes()[..n]);
            Ok(short.into_any().unbind())
        }
        (Output::ReadInto, Reply::Read { n }) => any(py, n),
        (_, Reply::Bytes(data)) => Ok(PyBytes::new(py, &data).into_any().unbind()),
        (_, Reply::Handle { id, size, fd }) => any(py, (id, size, fd)),
        (_, Reply::Written { n, end }) => any(py, (n, end)),
        (_, Reply::Size(size)) => any(py, size),
        (_, Reply::Unit) => Ok(py.None()),
        (_, reply) => Err(PyRuntimeError::new_err(format!(
            "mismatched turbofile reply: {reply:?}"
        ))),
    }
}

fn any<'py, T: IntoPyObject<'py>>(py: Python<'py>, value: T) -> PyResult<PyObject> {
    Ok(value
        .into_pyobject(py)
        .map_err(Into::into)?
        .into_any()
        .unbind())
}

fn io_err_to_pyerr(e: io::Error) -> PyErr {
    match e.raw_os_error() {
        Some(code) => PyOSError::new_err((code, e.to_string())),
        None => PyOSError::new_err(e.to_string()),
    }
}

fn bridge_for_running_loop(py: Python<'_>) -> PyResult<Arc<LoopBridge>> {
    let get_running_loop = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<PyObject> {
        Ok(py.import("asyncio")?.getattr("get_running_loop")?.unbind())
    })?;
    let event_loop = get_running_loop.bind(py).call0()?;
    let key = event_loop.as_ptr() as usize;
    let mut map = globals().bridges.lock().unwrap();
    if let Some(bridge) = map.get(&key) {
        return Ok(bridge.clone());
    }
    map.retain(|_, bridge| {
        bridge
            .event_loop
            .bind(py)
            .call_method0("is_closed")
            .and_then(|closed| closed.extract::<bool>())
            .map(|closed| !closed)
            .unwrap_or(false)
    });
    let bridge = Arc::new(LoopBridge {
        key,
        queue: Mutex::new(Vec::new()),
        armed: AtomicBool::new(false),
        call_soon_threadsafe: event_loop.getattr("call_soon_threadsafe")?.unbind(),
        event_loop: event_loop.unbind(),
        drain: OnceLock::new(),
    });
    let drain = Py::new(
        py,
        DrainHandle {
            bridge: Arc::downgrade(&bridge),
        },
    )?;
    bridge
        .drain
        .set(drain.into_any())
        .unwrap_or_else(|_| unreachable!("drain set once"));
    map.insert(key, bridge.clone());
    Ok(bridge)
}

fn submit(py: Python<'_>, op: Op, output: Output, owner: Option<Owner>) -> PyResult<PyObject> {
    let driver = driver()?;
    let bridge = bridge_for_running_loop(py)?;
    let fut = bridge
        .event_loop
        .bind(py)
        .call_method0("create_future")?
        .unbind();
    let result_fut = fut.clone_ref(py);
    driver.submit(
        op,
        Box::new(move |result| {
            bridge.complete(Completion {
                fut,
                result,
                output,
                owner,
            });
        }),
    );
    Ok(result_fut)
}

/// Shared state of one multi-chunk read: the last chunk to finish pushes the
/// single completion, so the buffer stays alive until every kernel op is done.
struct ReadGroup {
    bridge: Arc<LoopBridge>,
    remaining: AtomicUsize,
    state: Mutex<GroupState>,
}

struct GroupState {
    fut: Option<PyObject>,
    bytes: Option<Py<PyBytes>>,
    n_limit: usize,
    error: Option<io::Error>,
}

impl ReadGroup {
    fn chunk_done(&self, chunk_start: usize, chunk_len: usize, result: io::Result<Reply>) {
        {
            let mut state = self.state.lock().unwrap();
            match result {
                Ok(Reply::Read { n }) => {
                    if n < chunk_len {
                        state.n_limit = state.n_limit.min(chunk_start + n);
                    }
                }
                Ok(_) => {
                    state.error.get_or_insert_with(|| {
                        io::Error::other("mismatched turbofile reply in read group")
                    });
                }
                Err(e) => {
                    state.error.get_or_insert(e);
                }
            }
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let fut = state.fut.take().expect("group completes once");
        let bytes = state.bytes.take().expect("group completes once");
        let result = match state.error.take() {
            Some(e) => Err(e),
            None => Ok(Reply::Read { n: state.n_limit }),
        };
        drop(state);
        self.bridge.complete(Completion {
            fut,
            result,
            output: Output::FillBytes { bytes },
            owner: None,
        });
    }
}

fn write_payload(data: &Bound<'_, PyAny>) -> PyResult<(Payload, Owner)> {
    let buffer = PyBuffer::<u8>::get(data)?;
    if !buffer.is_c_contiguous() {
        return Err(PyValueError::new_err("buffer must be C-contiguous"));
    }
    let payload = Payload::Borrowed {
        ptr: buffer.buf_ptr() as *const u8,
        len: buffer.len_bytes(),
    };
    Ok((payload, Owner::Buffer(buffer)))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn open(
    py: Python<'_>,
    path: PathBuf,
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
) -> PyResult<PyObject> {
    let spec = OpenSpec {
        read,
        write,
        append,
        truncate,
        create,
        create_new,
    };
    submit(py, Op::Open { path, spec }, Output::Plain, None)
}

#[pyfunction]
fn read(py: Python<'_>, handle: u64, pos: u64, len: usize) -> PyResult<PyObject> {
    // The bytes object is created uninitialized and filled by the kernel; it
    // is never visible to Python before its completion delivers it.
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let ptr = bytes.as_bytes().as_ptr() as *mut u8;
    submit(
        py,
        Op::ReadAt {
            handle,
            pos,
            dest: Dest::Into { ptr, len },
        },
        Output::FillBytes {
            bytes: bytes.unbind(),
        },
        None,
    )
}

#[pyfunction]
fn read_parallel(
    py: Python<'_>,
    handle: u64,
    pos: u64,
    len: usize,
    chunk: usize,
) -> PyResult<PyObject> {
    if chunk == 0 {
        return Err(PyValueError::new_err("chunk must be positive"));
    }
    let driver = driver()?;
    let bridge = bridge_for_running_loop(py)?;
    let fut = bridge
        .event_loop
        .bind(py)
        .call_method0("create_future")?
        .unbind();
    let result_fut = fut.clone_ref(py);

    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let base = bytes.as_bytes().as_ptr() as *mut u8;
    let chunk_count = len.div_ceil(chunk).max(1);
    let group = Arc::new(ReadGroup {
        bridge,
        remaining: AtomicUsize::new(chunk_count),
        state: Mutex::new(GroupState {
            fut: Some(fut),
            bytes: Some(bytes.unbind()),
            n_limit: len,
            error: None,
        }),
    });
    if len == 0 {
        group.chunk_done(0, 0, Ok(Reply::Read { n: 0 }));
        return Ok(result_fut);
    }
    for start in (0..len).step_by(chunk) {
        let chunk_len = chunk.min(len - start);
        let group = group.clone();
        driver.submit(
            Op::ReadAt {
                handle,
                pos: pos + start as u64,
                dest: Dest::Into {
                    ptr: unsafe { base.add(start) },
                    len: chunk_len,
                },
            },
            Box::new(move |result| group.chunk_done(start, chunk_len, result)),
        );
    }
    Ok(result_fut)
}

#[pyfunction]
fn read_to_end(py: Python<'_>, handle: u64, pos: u64) -> PyResult<PyObject> {
    submit(py, Op::ReadToEnd { handle, pos }, Output::Plain, None)
}

#[pyfunction]
fn readinto(py: Python<'_>, handle: u64, pos: u64, buffer: Bound<'_, PyAny>) -> PyResult<PyObject> {
    let buffer = PyBuffer::<u8>::get(&buffer)?;
    if buffer.readonly() {
        return Err(PyValueError::new_err("readinto needs a writable buffer"));
    }
    if !buffer.is_c_contiguous() {
        return Err(PyValueError::new_err("buffer must be C-contiguous"));
    }
    let dest = Dest::Into {
        ptr: buffer.buf_ptr() as *mut u8,
        len: buffer.len_bytes(),
    };
    submit(
        py,
        Op::ReadAt { handle, pos, dest },
        Output::ReadInto,
        Some(Owner::Buffer(buffer)),
    )
}

#[pyfunction]
fn write(
    py: Python<'_>,
    handle: u64,
    pos: u64,
    data: Bound<'_, PyAny>,
    append: bool,
) -> PyResult<PyObject> {
    let (payload, owner) = write_payload(&data)?;
    submit(
        py,
        Op::WriteAt {
            handle,
            pos,
            data: payload,
            append,
        },
        Output::Plain,
        Some(owner),
    )
}

#[pyfunction]
fn close(py: Python<'_>, handle: u64) -> PyResult<PyObject> {
    submit(py, Op::Close { handle }, Output::Plain, None)
}

#[pyfunction]
fn sync(py: Python<'_>, handle: u64, data_only: bool) -> PyResult<PyObject> {
    submit(py, Op::Sync { handle, data_only }, Output::Plain, None)
}

#[pyfunction]
fn set_len(py: Python<'_>, handle: u64, size: u64) -> PyResult<PyObject> {
    submit(py, Op::SetLen { handle, size }, Output::Plain, None)
}

#[pyfunction]
fn size(py: Python<'_>, handle: u64) -> PyResult<PyObject> {
    submit(py, Op::Size { handle }, Output::Plain, None)
}

#[pyfunction]
fn read_file(py: Python<'_>, path: PathBuf) -> PyResult<PyObject> {
    submit(py, Op::ReadFile { path }, Output::Plain, None)
}

#[pyfunction]
fn write_file(py: Python<'_>, path: PathBuf, data: Bound<'_, PyAny>) -> PyResult<PyObject> {
    let (payload, owner) = write_payload(&data)?;
    submit(
        py,
        Op::WriteFile {
            path,
            data: payload,
        },
        Output::Plain,
        Some(owner),
    )
}

/// Page-cache fast path for positional reads.
///
/// Serves the whole request from resident pages on the *calling* thread and
/// hands back `bytes` directly: no future, no submit channel, no driver-thread
/// hop and no event-loop wakeup. The ladder in `perf/` measures this at ~1.1 us
/// against ~45 us for the bridge path, because a cross-thread completion on a
/// machine with an 18 us C-state exit latency costs two wakes.
///
/// Returns `None` — never an error — whenever the fast path does not apply, so
/// the caller falls back to ordinary async submission: `EAGAIN` when the data
/// is not resident (this is exactly what keeps the loop unblocked), and
/// `EOPNOTSUPP` when the filesystem sets no `FMODE_NOWAIT` (tmpfs, notably).
#[cfg(target_os = "linux")]
#[pyfunction]
fn try_read(py: Python<'_>, fd: i32, pos: u64, len: usize) -> PyResult<Option<PyObject>> {
    if fd < 0 || len == 0 {
        return Ok(None);
    }
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let base = bytes.as_bytes().as_ptr() as *mut u8;
    let mut filled: usize = 0;
    while filled < len {
        let iov = libc::iovec {
            // Safety: `base` owns `len` bytes and `filled < len`.
            iov_base: unsafe { base.add(filled) } as *mut libc::c_void,
            iov_len: len - filled,
        };
        let off = (pos + filled as u64) as libc::off_t;
        // Safety: `iov` describes live memory owned by `bytes` for this call.
        let n = unsafe { libc::preadv2(fd, &iov, 1, off, RWF_NOWAIT) };
        if n < 0 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                // Not resident, or no FMODE_NOWAIT: defer to the async path.
                Some(libc::EAGAIN) | Some(libc::EOPNOTSUPP) | Some(libc::EINVAL) => Ok(None),
                _ => Err(io_err_to_pyerr(e)),
            };
        }
        if n == 0 {
            break; // EOF, matching the backend's read-up-to semantics.
        }
        filled += n as usize;
    }
    if filled == len {
        return Ok(Some(bytes.into_any().unbind()));
    }
    Ok(Some(
        PyBytes::new(py, &bytes.as_bytes()[..filled])
            .into_any()
            .unbind(),
    ))
}

/// `readinto` counterpart of [`try_read`]: fills a caller-owned buffer and
/// returns the byte count, or `None` when the fast path does not apply.
#[cfg(target_os = "linux")]
#[pyfunction]
fn try_readinto(fd: i32, pos: u64, buffer: Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    let buffer = PyBuffer::<u8>::get(&buffer)?;
    if buffer.readonly() {
        return Err(PyValueError::new_err("readinto needs a writable buffer"));
    }
    if !buffer.is_c_contiguous() {
        return Err(PyValueError::new_err("buffer must be C-contiguous"));
    }
    let len = buffer.len_bytes();
    if fd < 0 || len == 0 {
        return Ok(None);
    }
    let base = buffer.buf_ptr() as *mut u8;
    let mut filled: usize = 0;
    while filled < len {
        let iov = libc::iovec {
            // Safety: the PyBuffer pins `len` bytes for the lifetime of this call.
            iov_base: unsafe { base.add(filled) } as *mut libc::c_void,
            iov_len: len - filled,
        };
        let off = (pos + filled as u64) as libc::off_t;
        // Safety: as above; `preadv2` returns before `buffer` is released.
        let n = unsafe { libc::preadv2(fd, &iov, 1, off, RWF_NOWAIT) };
        if n < 0 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) | Some(libc::EOPNOTSUPP) | Some(libc::EINVAL) => Ok(None),
                _ => Err(io_err_to_pyerr(e)),
            };
        }
        if n == 0 {
            break;
        }
        filled += n as usize;
    }
    Ok(Some(filled))
}

/// Whether this file supports `RWF_NOWAIT` at all. Filesystems that set no
/// `FMODE_NOWAIT` (tmpfs among them) fail every attempt with `EOPNOTSUPP`, so
/// the caller latches this once instead of paying a doomed syscall per read.
#[cfg(target_os = "linux")]
#[pyfunction]
fn fast_read_supported(fd: i32) -> bool {
    if fd < 0 {
        return false;
    }
    let mut byte = 0u8;
    let iov = libc::iovec {
        iov_base: &mut byte as *mut u8 as *mut libc::c_void,
        iov_len: 1,
    };
    // Safety: `iov` describes one live stack byte for the duration of the call.
    let n = unsafe { libc::preadv2(fd, &iov, 1, 0, RWF_NOWAIT) };
    // EAGAIN means "supported, just not resident"; only EOPNOTSUPP rules it out.
    n >= 0 || io::Error::last_os_error().raw_os_error() != Some(libc::EOPNOTSUPP)
}

/// `openat2` resolve flag: fail with `EAGAIN` rather than perform any I/O to
/// resolve the path. Linux 5.12+.
#[cfg(target_os = "linux")]
const RESOLVE_CACHED: u64 = 0x20;

#[cfg(target_os = "linux")]
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

/// Closes its fd on every exit path, including the early returns below.
#[cfg(target_os = "linux")]
struct OwnedFd(i32);

#[cfg(target_os = "linux")]
impl Drop for OwnedFd {
    fn drop(&mut self) {
        // Safety: we own this descriptor and drop runs exactly once.
        unsafe { libc::close(self.0) };
    }
}

/// Read-to-end fast path on an already-open descriptor: the `f.read()` case,
/// where the size comes from `fstat` rather than from the caller. Same
/// non-blocking guarantee as [`try_read`], and the same `None` contract --
/// anything unexpected defers to the async path, which is authoritative for
/// both the result and any error.
#[cfg(target_os = "linux")]
#[pyfunction]
fn try_read_all(py: Python<'_>, fd: i32, pos: u64) -> PyResult<Option<PyObject>> {
    if fd < 0 {
        return Ok(None);
    }
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: `st` is a live, correctly sized `struct stat`.
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        return Ok(None);
    }
    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Ok(None);
    }
    let size = st.st_size.max(0) as u64;
    let len = size.saturating_sub(pos) as usize;
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    if len > 0 {
        let base = bytes.as_bytes().as_ptr() as *mut u8;
        // Safety: `base` owns `len` bytes for the duration of the fill.
        match unsafe { nowait_fill(fd, pos, base, len) } {
            Some(filled) if filled == len => {}
            _ => return Ok(None),
        }
    }
    // Confirm we really are at EOF and the file did not grow under us.
    let mut tail = 0u8;
    let probe = libc::iovec {
        iov_base: &mut tail as *mut u8 as *mut libc::c_void,
        iov_len: 1,
    };
    // Safety: `probe` describes one live stack byte.
    let n = unsafe { libc::preadv2(fd, &probe, 1, size as libc::off_t, RWF_NOWAIT) };
    if n != 0 {
        return Ok(None);
    }
    Ok(Some(bytes.into_any().unbind()))
}

/// Fill `[base, base + len)` from `fd` at `pos` using page-cache-only reads.
/// `None` means some part would have blocked, or the file ended early.
///
/// # Safety
/// `base` must own at least `len` writable bytes for the whole call.
#[cfg(target_os = "linux")]
unsafe fn nowait_fill(fd: i32, pos: u64, base: *mut u8, len: usize) -> Option<usize> {
    let mut filled: usize = 0;
    while filled < len {
        let iov = libc::iovec {
            iov_base: base.add(filled) as *mut libc::c_void,
            iov_len: len - filled,
        };
        let off = (pos + filled as u64) as libc::off_t;
        let n = libc::preadv2(fd, &iov, 1, off, RWF_NOWAIT);
        if n < 0 {
            match io::Error::last_os_error().raw_os_error() {
                Some(libc::EINTR) => continue,
                _ => return None,
            }
        }
        if n == 0 {
            break;
        }
        filled += n as usize;
    }
    Some(filled)
}

#[cfg(not(target_os = "linux"))]
#[pyfunction]
fn try_read_all(_py: Python<'_>, _fd: i32, _pos: u64) -> PyResult<Option<PyObject>> {
    Ok(None)
}

/// Whole-file fast path: open, size, read and close entirely on the calling
/// thread, with every step refusing to block. `openat2(RESOLVE_CACHED)` fails
/// rather than walk a path that needs I/O, and `preadv2(RWF_NOWAIT)` fails
/// rather than wait on the device, so nothing here can stall the event loop.
///
/// Returns `None` whenever any step would have blocked, leaving the caller to
/// submit the ordinary async `ReadFile`.
#[cfg(target_os = "linux")]
#[pyfunction]
fn try_read_file(py: Python<'_>, path: PathBuf) -> PyResult<Option<PyObject>> {
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

    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // Safety: `st` is a live, correctly sized `struct stat`.
    if unsafe { libc::fstat(fd.0, &mut st) } < 0 {
        return Ok(None);
    }
    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Ok(None); // only regular files have a trustworthy size
    }
    let len = st.st_size.max(0) as usize;

    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let base = bytes.as_bytes().as_ptr() as *mut u8;
    let mut filled: usize = 0;
    while filled < len {
        let iov = libc::iovec {
            // Safety: `base` owns `len` bytes and `filled < len`.
            iov_base: unsafe { base.add(filled) } as *mut libc::c_void,
            iov_len: len - filled,
        };
        // Safety: `iov` describes live memory owned by `bytes`.
        let n = unsafe { libc::preadv2(fd.0, &iov, 1, filled as libc::off_t, RWF_NOWAIT) };
        if n < 0 {
            return match io::Error::last_os_error().raw_os_error() {
                Some(libc::EINTR) => continue,
                _ => Ok(None),
            };
        }
        if n == 0 {
            // Shorter than fstat claimed: let the async path settle it.
            return Ok(None);
        }
        filled += n as usize;
    }

    // The file may have grown since fstat; one byte past the end tells us.
    let mut tail = 0u8;
    let probe = libc::iovec {
        iov_base: &mut tail as *mut u8 as *mut libc::c_void,
        iov_len: 1,
    };
    // Safety: `probe` describes one live stack byte.
    let n = unsafe { libc::preadv2(fd.0, &probe, 1, len as libc::off_t, RWF_NOWAIT) };
    if n != 0 {
        return Ok(None); // grew, or could not be confirmed: fall back
    }
    Ok(Some(bytes.into_any().unbind()))
}

#[cfg(not(target_os = "linux"))]
#[pyfunction]
fn fast_read_supported(_fd: i32) -> bool {
    false
}

#[cfg(not(target_os = "linux"))]
#[pyfunction]
fn try_read_file(_py: Python<'_>, _path: PathBuf) -> PyResult<Option<PyObject>> {
    Ok(None)
}

/// Platforms without `preadv2(RWF_NOWAIT)` have no page-cache fast path; the
/// caller always takes the async submission route.
#[cfg(not(target_os = "linux"))]
#[pyfunction]
fn try_read(_py: Python<'_>, _fd: i32, _pos: u64, _len: usize) -> PyResult<Option<PyObject>> {
    Ok(None)
}

#[cfg(not(target_os = "linux"))]
#[pyfunction]
fn try_readinto(_fd: i32, _pos: u64, _buffer: Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    Ok(None)
}

/// `preadv2` flag: serve only from page cache, return EAGAIN rather than
/// blocking on the device. Linux 4.14+; not exposed by the libc crate.
#[cfg(target_os = "linux")]
const RWF_NOWAIT: libc::c_int = 0x0000_0008;

/// Calibration probe: a page-cache-only positional read on the *calling*
/// thread. Returns EAGAIN instead of blocking when the data is not resident,
/// which is what makes it safe to attempt from an event loop. This is the
/// floor of the fast path the ladder argues for.
#[cfg(target_os = "linux")]
#[pyfunction]
fn probe_pread_nowait(py: Python<'_>, fd: i32, len: usize) -> PyResult<PyObject> {
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let iov = libc::iovec {
        iov_base: bytes.as_bytes().as_ptr() as *mut libc::c_void,
        iov_len: len,
    };
    // Safety: `bytes` owns `len` bytes and outlives this synchronous call.
    let n = unsafe { libc::preadv2(fd, &iov, 1, 0, RWF_NOWAIT) };
    if n < 0 {
        return Err(io_err_to_pyerr(io::Error::last_os_error()));
    }
    Ok(bytes.into_any().unbind())
}

/// Calibration probe: one io_uring read submitted and reaped on the *calling*
/// thread inside a single `io_uring_enter`. No channel, no driver thread, no
/// doorbell. This is what the ladder's `read` rung would cost if the thread
/// hop were removed, so it is the floor any redesign is measured against.
#[cfg(target_os = "linux")]
#[pyfunction]
fn probe_inline_read(py: Python<'_>, fd: i32, len: usize) -> PyResult<PyObject> {
    use io_uring::{opcode, types};
    let bytes = PyBytes::new_with(py, len, |_| Ok(()))?;
    let ptr = bytes.as_bytes().as_ptr() as *mut u8;
    PROBE_RING.with(|cell| -> PyResult<()> {
        let mut guard = cell.borrow_mut();
        if guard.is_none() {
            *guard = Some(io_uring::IoUring::new(64).map_err(io_err_to_pyerr)?);
        }
        let ring = guard.as_mut().expect("ring initialised above");
        let sqe = opcode::Read::new(types::Fd(fd), ptr, len as u32)
            .offset(0)
            .build()
            .user_data(1);
        // Safety: `bytes` owns the buffer and outlives the wait below, which
        // does not return until the kernel has finished with it.
        unsafe {
            ring.submission()
                .push(&sqe)
                .map_err(|_| PyRuntimeError::new_err("probe submission queue full"))?;
        }
        ring.submit_and_wait(1).map_err(io_err_to_pyerr)?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| PyRuntimeError::new_err("probe completion missing"))?;
        match cqe.result() {
            n if n < 0 => Err(io_err_to_pyerr(io::Error::from_raw_os_error(-n))),
            _ => Ok(()),
        }
    })?;
    Ok(bytes.into_any().unbind())
}

/// Calibration probe: pyo3 entry and return, nothing else. Establishes the
/// FFI floor for the latency ladder.
#[pyfunction]
fn probe_ffi() -> u64 {
    0
}

/// Calibration probe: an asyncio future created and resolved on the calling
/// thread, never touching the driver. Isolates future machinery from the
/// completion bridge.
#[pyfunction]
fn probe_resolved_future(py: Python<'_>) -> PyResult<PyObject> {
    let bridge = bridge_for_running_loop(py)?;
    let fut = bridge.event_loop.bind(py).call_method0("create_future")?;
    fut.call_method1("set_result", (py.None(),))?;
    Ok(fut.unbind())
}

/// Calibration probe: a full submit -> driver thread -> doorbell -> drain
/// round trip that performs no kernel work. Measured against
/// `probe_resolved_future` this is the bridge's own per-op cost.
#[pyfunction]
fn probe_nop(py: Python<'_>) -> PyResult<PyObject> {
    submit(py, Op::Nop, Output::Plain, None)
}

#[pyfunction]
fn backend_name() -> PyResult<&'static str> {
    Ok(driver()?.backend_label())
}

#[pyfunction]
fn shutdown() {
    ALIVE.store(false, Ordering::Release);
}

#[pymodule]
fn _turbofile(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(try_read, m)?)?;
    m.add_function(wrap_pyfunction!(try_read_file, m)?)?;
    m.add_function(wrap_pyfunction!(try_read_all, m)?)?;
    m.add_function(wrap_pyfunction!(fast_read_supported, m)?)?;
    m.add_function(wrap_pyfunction!(try_readinto, m)?)?;
    m.add_function(wrap_pyfunction!(read_parallel, m)?)?;
    m.add_function(wrap_pyfunction!(read_to_end, m)?)?;
    m.add_function(wrap_pyfunction!(readinto, m)?)?;
    m.add_function(wrap_pyfunction!(write, m)?)?;
    m.add_function(wrap_pyfunction!(close, m)?)?;
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    m.add_function(wrap_pyfunction!(set_len, m)?)?;
    m.add_function(wrap_pyfunction!(size, m)?)?;
    m.add_function(wrap_pyfunction!(read_file, m)?)?;
    m.add_function(wrap_pyfunction!(write_file, m)?)?;
    #[cfg(target_os = "linux")]
    m.add_function(wrap_pyfunction!(probe_inline_read, m)?)?;
    #[cfg(target_os = "linux")]
    m.add_function(wrap_pyfunction!(probe_pread_nowait, m)?)?;
    m.add_function(wrap_pyfunction!(probe_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(probe_resolved_future, m)?)?;
    m.add_function(wrap_pyfunction!(probe_nop, m)?)?;
    m.add_function(wrap_pyfunction!(backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    Ok(())
}
