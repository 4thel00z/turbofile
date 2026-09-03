//! Backend drivers for turbofile: op submission over a channel to a
//! dedicated I/O thread, completion via callback.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

mod compio_backend;
#[cfg(target_os = "macos")]
mod darwin_aio;

pub type Callback = Box<dyn FnOnce(io::Result<Reply>) + Send + 'static>;

/// The error an op settles with when its cancel request won.
#[cfg(unix)]
pub fn cancelled_error() -> io::Error {
    io::Error::from_raw_os_error(libc::ECANCELED)
}

#[cfg(unix)]
pub fn is_cancelled(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::ECANCELED)
}

pub(crate) enum Msg {
    Submit { id: u64, op: Op, cb: Callback },
    Cancel { id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Compio,
    #[cfg(target_os = "macos")]
    DarwinAio,
}

impl BackendKind {
    pub fn default_for_platform() -> Self {
        #[cfg(target_os = "macos")]
        return BackendKind::DarwinAio;
        #[cfg(not(target_os = "macos"))]
        BackendKind::Compio
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenSpec {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub create_new: bool,
}

/// Bytes to write. `Borrowed` carries a raw pointer whose allocation the
/// submitter must keep alive until the op's callback has been invoked.
#[derive(Debug)]
pub enum Payload {
    Owned(Vec<u8>),
    Borrowed { ptr: *const u8, len: usize },
}

unsafe impl Send for Payload {}

impl Payload {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Payload::Owned(data) => data,
            Payload::Borrowed { ptr, len } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Where read bytes go. `Into` points at caller-owned memory with the same
/// validity rule as [`Payload::Borrowed`]; the backend replies [`Reply::Read`]
/// for it and [`Reply::Bytes`] for `Alloc`.
#[derive(Debug)]
pub enum Dest {
    Alloc { len: usize },
    Into { ptr: *mut u8, len: usize },
}

unsafe impl Send for Dest {}

#[derive(Debug)]
pub enum Op {
    Open {
        path: PathBuf,
        spec: OpenSpec,
    },
    ReadAt {
        handle: u64,
        pos: u64,
        dest: Dest,
    },
    ReadToEnd {
        handle: u64,
        pos: u64,
    },
    WriteAt {
        handle: u64,
        pos: u64,
        data: Payload,
        append: bool,
    },
    Close {
        handle: u64,
    },
    Sync {
        handle: u64,
        data_only: bool,
    },
    SetLen {
        handle: u64,
        size: u64,
    },
    Size {
        handle: u64,
    },
    /// Open, read and close in one submission when the file is at most
    /// `inline_max` bytes. A larger file comes back as an open read handle
    /// (`Reply::Handle`) instead, so the caller can fill its own buffer with a
    /// parallel read and close it: one copy less, and chunks in flight.
    ReadFile {
        path: PathBuf,
        inline_max: u64,
    },
    WriteFile {
        path: PathBuf,
        data: Payload,
    },
    /// Calibration probe: no kernel work and no file-table access. Isolates
    /// the submit channel, the driver-thread hop and the completion doorbell
    /// from the cost of the I/O itself.
    Nop,
}

#[derive(Debug)]
pub enum Reply {
    Handle { id: u64, size: u64, fd: i64 },
    Bytes(Vec<u8>),
    Read { n: usize },
    Written { n: usize, end: u64 },
    Size(u64),
    Unit,
}

pub struct Driver {
    tx: flume::Sender<Msg>,
    next_op: AtomicU64,
    label: &'static str,
}

impl Driver {
    pub fn new(kind: BackendKind) -> io::Result<Self> {
        let (tx, rx) = flume::unbounded();
        let label = match kind {
            BackendKind::Compio => compio_backend::spawn(rx)?,
            #[cfg(target_os = "macos")]
            BackendKind::DarwinAio => {
                darwin_aio::spawn(rx)?;
                "darwin-aio"
            }
        };
        Ok(Self {
            tx,
            next_op: AtomicU64::new(1),
            label,
        })
    }

    /// The I/O mechanism actually driving this backend (e.g. the compio
    /// fusion driver may have fallen back from io_uring to polling).
    pub fn backend_label(&self) -> &'static str {
        self.label
    }

    /// Returns the op's id, the token [`Driver::cancel`] takes.
    pub fn submit(&self, op: Op, cb: Callback) -> u64 {
        let id = self.next_op.fetch_add(1, Ordering::Relaxed);
        if let Err(flume::SendError(Msg::Submit { cb, .. })) =
            self.tx.send(Msg::Submit { id, op, cb })
        {
            cb(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "turbofile driver thread is gone",
            )));
        }
        id
    }

    /// Best-effort abort of an in-flight op: a queued op settles ECANCELED,
    /// a submitted one is cancelled where the backend can (aio_cancel on
    /// macOS; compio aborts before the next chunk). An op the kernel already
    /// completed delivers its result.
    pub fn cancel(&self, id: u64) {
        self.tx.send(Msg::Cancel { id }).ok();
    }
}

pub(crate) fn bad_handle() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "unknown or closed turbofile handle",
    )
}
