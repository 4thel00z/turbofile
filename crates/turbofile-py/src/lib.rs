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
use pyo3::types::{PyBytes, PyDict};
use pyo3::BoundObject;

use turbofile_core::{BackendKind, Dest, Driver, Op, OpenSpec, Payload, Reply};

/// Cleared by `shutdown()` (registered with atexit): once the interpreter is
/// finalizing, driver-thread completions must never attach to Python again.
static ALIVE: AtomicBool = AtomicBool::new(true);

static GET_RUNNING_LOOP: GILOnceCell<PyObject> = GILOnceCell::new();

static KERNEL_FUTURE: GILOnceCell<PyObject> = GILOnceCell::new();

/// A submitted kernel op cannot be recalled, so its future refuses
/// cancellation: `Task.cancel` falls back to `_must_cancel`, the
/// `CancelledError` is delivered when the op completes, and caller buffers are
/// never touched after the `await` raises.
fn kernel_future_class(py: Python<'_>) -> PyResult<&'static PyObject> {
    KERNEL_FUTURE.get_or_try_init(py, || {
        let ns = PyDict::new(py);
        py.run(
            c"import asyncio\n\nclass KernelFuture(asyncio.Future):\n    \"\"\"Completion of an in-flight kernel op; refuses cancellation.\"\"\"\n\n    def cancel(self, msg: object = None) -> bool:\n        return False\n",
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
    call_soon_threadsafe: PyObject,
    /// `functools.partial(KernelFuture, loop=event_loop)`.
    create_future: PyObject,
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
        call_soon_threadsafe: event_loop.getattr("call_soon_threadsafe")?.unbind(),
        create_future,
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
    let fut = bridge.create_future.bind(py).call0()?.unbind();
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
    m.add_function(wrap_pyfunction!(backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    Ok(())
}
