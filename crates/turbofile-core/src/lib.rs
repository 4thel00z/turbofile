//! Backend drivers for turbofile: op submission over a channel to a
//! dedicated I/O thread, completion via callback.

use std::io;
use std::path::PathBuf;

mod compio_backend;
#[cfg(target_os = "macos")]
mod darwin_aio;

pub type Callback = Box<dyn FnOnce(io::Result<Reply>) + Send + 'static>;

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
    ReadFile {
        path: PathBuf,
    },
    WriteFile {
        path: PathBuf,
        data: Payload,
    },
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
    tx: flume::Sender<(Op, Callback)>,
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
        Ok(Self { tx, label })
    }

    /// The I/O mechanism actually driving this backend (e.g. the compio
    /// fusion driver may have fallen back from io_uring to polling).
    pub fn backend_label(&self) -> &'static str {
        self.label
    }

    pub fn submit(&self, op: Op, cb: Callback) {
        if let Err(flume::SendError((_, cb))) = self.tx.send((op, cb)) {
            cb(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "turbofile driver thread is gone",
            )));
        }
    }
}

pub(crate) fn bad_handle() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "unknown or closed turbofile handle",
    )
}
