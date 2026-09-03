//! Extension module `turbofile._turbofile`: submits ops to the turbofile-core
//! driver and completes asyncio futures through per-loop completion queues.
//! Each burst of completions costs one doorbell: a byte written to a pipe the
//! loop watches through `add_reader`, so the driver thread never takes the GIL
//! to wake the loop (`call_soon_threadsafe` only for loops without readers). A
//! drain callback on the loop thread delivers every queued completion and is
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
use pyo3::types::{PyBytes, PyDict};
use pyo3::BoundObject;

use turbofile_core::{is_cancelled, BackendKind, Dest, Driver, Op, OpenSpec, Payload, Reply};

mod fast_path;

#[cfg(target_os = "linux")]
use fast_path::RWF_NOWAIT;

/// Cleared by `shutdown()` (registered with atexit): once the interpreter is
/// finalizing, driver-thread completions must never attach to Python again.
static ALIVE: AtomicBool = AtomicBool::new(true);

static GET_RUNNING_LOOP: GILOnceCell<PyObject> = GILOnceCell::new();

static KERNEL_FUTURE: GILOnceCell<PyObject> = GILOnceCell::new();

#[cfg(target_os = "linux")]
thread_local! {
    /// Private ring owned by `probe_inline_read`, never shared with the driver.
    static PROBE_RING: std::cell::RefCell<Option<io_uring::IoUring>> =
        const { std::cell::RefCell::new(None) };
}

/// `cancel()` forwards an abort request for the future's kernel ops (their
/// ids sit in `op_ids`) and returns False: the future settles when the ops
/// do, promptly on abort, so caller buffers are never touched after the
/// `await` raises. `deliver` turns an ECANCELED settle into a real
/// cancellation via `settle_cancelled`.
fn kernel_future_class(py: Python<'_>) -> PyResult<&'static PyObject> {
    KERNEL_FUTURE.get_or_try_init(py, || {
        let ns = PyDict::new(py);
        ns.set_item("cancel_kernel_ops", wrap_pyfunction!(cancel_ops, py)?)?;
        py.run(
            c"import asyncio\n\nclass KernelFuture(asyncio.Future):\n    \"\"\"Completion of in-flight kernel ops; cancel() requests their abort.\"\"\"\n\n    def cancel(self, msg: object = None) -> bool:\n        if self.done():\n            return False\n        cancel_kernel_ops(self.op_ids)\n        return False\n\n    def settle_cancelled(self) -> bool:\n        return super().cancel()\n",
            Some(&ns),
            Some(&ns),
        )?;
        let class = ns
            .get_item("KernelFuture")?
            .ok_or_else(|| PyRuntimeError::new_err("KernelFuture class not defined"))?;
        Ok(class.unbind())
    })
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
    /// `functools.partial(KernelFuture, loop=event_loop)`.
    create_future: PyObject,
    /// Both set right after construction: the drain needs a weak reference to
    /// the bridge, and the doorbell needs the drain registered with the loop.
    doorbell: OnceLock<Doorbell>,
    drain: OnceLock<PyObject>,
}

/// How the driver thread wakes the loop thread once per completion burst.
enum Doorbell {
    /// The loop watches `read_fd` through `add_reader`; one byte written to
    /// `write_fd` wakes it. A plain syscall, so the driver thread never needs
    /// the GIL to deliver.
    #[cfg(unix)]
    Pipe { read_fd: i32, write_fd: i32 },
    /// Loops without `add_reader` (a proactor): `call_soon_threadsafe(drain)`,
    /// which takes the GIL and fails once the loop is closed.
    CallSoon(PyObject),
}

#[cfg(unix)]
impl Drop for Doorbell {
    fn drop(&mut self) {
        let Doorbell::Pipe { read_fd, write_fd } = self else {
            return;
        };
        // Safety: both descriptors came from `pipe` and are closed exactly once.
        unsafe {
            libc::close(*read_fd);
            libc::close(*write_fd);
        }
    }
}

/// A pipe with both ends non-blocking and close-on-exec.
#[cfg(unix)]
fn nonblocking_pipe() -> io::Result<(i32, i32)> {
    let mut fds = [0i32; 2];
    // Safety: `fds` has room for the two descriptors `pipe` writes.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    for fd in fds {
        // Safety: `fd` is a descriptor this function owns.
        let flagged = unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) == 0
                && libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) == 0
        };
        if flagged {
            continue;
        }
        let err = io::Error::last_os_error();
        // Safety: both descriptors are this function's and are closed exactly once.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        return Err(err);
    }
    Ok((fds[0], fds[1]))
}

/// The pipe doorbell for `event_loop`, or `None` when the loop has no working
/// `add_reader` or no pipe can be made; the caller falls back to
/// `call_soon_threadsafe`. A pipe the loop refuses is closed here.
#[cfg(unix)]
fn pipe_doorbell(event_loop: &Bound<'_, PyAny>, drain: &Bound<'_, PyAny>) -> Option<Doorbell> {
    let (read_fd, write_fd) = nonblocking_pipe().ok()?;
    let doorbell = Doorbell::Pipe { read_fd, write_fd };
    event_loop
        .call_method1("add_reader", (read_fd, drain))
        .ok()?;
    Some(doorbell)
}

#[cfg(not(unix))]
fn pipe_doorbell(_event_loop: &Bound<'_, PyAny>, _drain: &Bound<'_, PyAny>) -> Option<Doorbell> {
    None
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
        match self.doorbell.get().expect("doorbell set at construction") {
            #[cfg(unix)]
            Doorbell::Pipe { write_fd, .. } => ring_pipe(*write_fd),
            Doorbell::CallSoon(call_soon_threadsafe) => self.ring_call_soon(call_soon_threadsafe),
        }
    }

    fn ring_call_soon(&self, call_soon_threadsafe: &PyObject) {
        Python::with_gil(|py| {
            let drain = self.drain.get().expect("drain set at construction");
            let rung = call_soon_threadsafe.bind(py).call1((drain.bind(py),));
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

    /// Loop-thread side: swallow every pending wake byte before the queue is
    /// read, so a byte written after this read is a wake for later work.
    fn quiet_doorbell(&self) {
        match self.doorbell.get().expect("doorbell set at construction") {
            #[cfg(unix)]
            Doorbell::Pipe { read_fd, .. } => quiet_pipe(*read_fd),
            Doorbell::CallSoon(_) => {}
        }
    }
}

#[cfg(unix)]
fn interrupted() -> bool {
    io::Error::last_os_error().raw_os_error() == Some(libc::EINTR)
}

/// Write the one wake byte. An `EINTR` before it is written is retried, since
/// the bridge stays armed and nothing else would ring; `EAGAIN` means the pipe
/// already holds a wake; any other failure means the bridge is gone.
#[cfg(unix)]
fn ring_pipe(write_fd: i32) {
    loop {
        // Safety: `write_fd` stays open for as long as its bridge lives.
        let n = unsafe { libc::write(write_fd, [1u8].as_ptr().cast(), 1) };
        if n >= 0 || !interrupted() {
            return;
        }
    }
}

/// Read the pipe dry: stop when it is empty (`EAGAIN`) or closed, retry `EINTR`.
#[cfg(unix)]
fn quiet_pipe(read_fd: i32) {
    let mut sink = [0u8; 64];
    loop {
        // Safety: `sink` has `sink.len()` writable bytes; the read end is non-blocking.
        let n = unsafe { libc::read(read_fd, sink.as_mut_ptr().cast(), sink.len()) };
        if n > 0 {
            continue;
        }
        if n < 0 && interrupted() {
            continue;
        }
        return;
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
        bridge.quiet_doorbell();
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
            Err(e) if is_cancelled(&e) => {
                fut.call_method0("settle_cancelled").ok();
            }
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
    let class = kernel_future_class(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("loop", &event_loop)?;
    let create_future = py
        .import("functools")?
        .getattr("partial")?
        .call((class.bind(py),), Some(&kwargs))?
        .unbind();
    let bridge = Arc::new(LoopBridge {
        key,
        queue: Mutex::new(Vec::new()),
        armed: AtomicBool::new(false),
        create_future,
        event_loop: event_loop.clone().unbind(),
        doorbell: OnceLock::new(),
        drain: OnceLock::new(),
    });
    let drain = Py::new(
        py,
        DrainHandle {
            bridge: Arc::downgrade(&bridge),
        },
    )?
    .into_any();
    let doorbell = match pipe_doorbell(&event_loop, drain.bind(py)) {
        Some(doorbell) => doorbell,
        None => Doorbell::CallSoon(event_loop.getattr("call_soon_threadsafe")?.unbind()),
    };
    bridge
        .drain
        .set(drain)
        .unwrap_or_else(|_| unreachable!("drain set once"));
    bridge
        .doorbell
        .set(doorbell)
        .unwrap_or_else(|_| unreachable!("doorbell set once"));
    map.insert(key, bridge.clone());
    Ok(bridge)
}

fn submit(py: Python<'_>, op: Op, output: Output, owner: Option<Owner>) -> PyResult<PyObject> {
    let driver = driver()?;
    let bridge = bridge_for_running_loop(py)?;
    let fut = bridge.create_future.bind(py).call0()?.unbind();
    let result_fut = fut.clone_ref(py);
    let id = driver.submit(
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
    // Same-thread with the GIL held: nothing can read op_ids before this.
    result_fut.bind(py).setattr("op_ids", (id,))?;
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
    let fut = bridge.create_future.bind(py).call0()?.unbind();
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
        result_fut.bind(py).setattr("op_ids", Vec::<u64>::new())?;
        group.chunk_done(0, 0, Ok(Reply::Read { n: 0 }));
        return Ok(result_fut);
    }
    let mut ids = Vec::with_capacity(chunk_count);
    for start in (0..len).step_by(chunk) {
        let chunk_len = chunk.min(len - start);
        let group = group.clone();
        ids.push(driver.submit(
            Op::ReadAt {
                handle,
                pos: pos + start as u64,
                dest: Dest::Into {
                    ptr: unsafe { base.add(start) },
                    len: chunk_len,
                },
            },
            Box::new(move |result| group.chunk_done(start, chunk_len, result)),
        ));
    }
    result_fut.bind(py).setattr("op_ids", ids)?;
    Ok(result_fut)
}

/// Best-effort abort of the kernel ops behind one future; ops the kernel
/// already completed deliver their result instead.
#[pyfunction]
fn cancel_ops(ops: Vec<u64>) -> PyResult<()> {
    let driver = driver()?;
    for id in ops {
        driver.cancel(id);
    }
    Ok(())
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

/// Whole file as `bytes` in one submission when it is at most `inline_max`
/// bytes; a larger file resolves to an open `(handle, size, fd)` instead, for
/// the caller to fill with `read_parallel` and close.
#[pyfunction]
fn read_file(py: Python<'_>, path: PathBuf, inline_max: u64) -> PyResult<PyObject> {
    submit(py, Op::ReadFile { path, inline_max }, Output::Plain, None)
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
    m.add_class::<fast_path::FastPath>()?;
    m.add_function(wrap_pyfunction!(fast_path::try_read_file, m)?)?;
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
    m.add_function(wrap_pyfunction!(cancel_ops, m)?)?;
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
