use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::mem::MaybeUninit;
use std::rc::Rc;

use compio::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use compio::driver::AsRawFd;
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAt, AsyncReadAtExt, AsyncWriteAtExt};

use crate::{bad_handle, cancelled_error, Dest, Msg, Op, OpenSpec, Payload, Reply};

type Files = Rc<RefCell<HashMap<u64, File>>>;

/// Cancel flags of ops still executing, keyed by op id. compio owns each
/// submitted kernel op's buffer through its own cancellation, so aborting
/// mid-op cannot release the caller's memory safely; the flag is honored
/// before the op starts and between chunks instead.
type CancelFlags = Rc<RefCell<HashMap<u64, Rc<Cell<bool>>>>>;

impl IoBuf for Payload {
    fn as_init(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Caller-owned memory handed to the kernel; validity is guaranteed by the
/// submitter until the op's callback runs (see [`Dest::Into`]).
struct RawBufMut {
    ptr: *mut u8,
    cap: usize,
    len: usize,
}

unsafe impl Send for RawBufMut {}

impl IoBuf for RawBufMut {
    fn as_init(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
}

impl SetLen for RawBufMut {
    unsafe fn set_len(&mut self, len: usize) {
        self.len = len.min(self.cap);
    }
}

impl IoBufMut for RawBufMut {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut MaybeUninit<u8>, self.cap) }
    }
}

pub(crate) fn spawn(rx: flume::Receiver<Msg>) -> io::Result<&'static str> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("turbofile-compio".into())
        .spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Ok(runtime) => {
                    ready_tx.send(Ok(runtime.driver_type())).ok();
                    runtime
                }
                Err(e) => {
                    ready_tx.send(Err(e)).ok();
                    return;
                }
            };
            runtime.block_on(run(rx));
        })?;
    match ready_rx.recv() {
        Ok(Ok(driver_type)) => Ok(match driver_type {
            compio::driver::DriverType::IoUring => "compio-io-uring",
            compio::driver::DriverType::Poll => "compio-polling",
            compio::driver::DriverType::IOCP => "compio-iocp",
        }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "compio driver thread exited during startup",
        )),
    }
}

async fn run(rx: flume::Receiver<Msg>) {
    let files: Files = Rc::new(RefCell::new(HashMap::new()));
    let cancel_flags: CancelFlags = Rc::new(RefCell::new(HashMap::new()));
    let mut next_id: u64 = 1;
    while let Ok(msg) = rx.recv_async().await {
        let (id, op, cb) = match msg {
            Msg::Cancel { id } => {
                if let Some(flag) = cancel_flags.borrow().get(&id) {
                    flag.set(true);
                }
                continue;
            }
            Msg::Submit { id, op, cb } => (id, op, cb),
        };
        let open_id = match &op {
            Op::Open { .. } | Op::ReadFile { .. } => {
                next_id += 1;
                Some(next_id - 1)
            }
            _ => None,
        };
        let cancelled = Rc::new(Cell::new(false));
        cancel_flags.borrow_mut().insert(id, cancelled.clone());
        let files = files.clone();
        let cancel_flags = cancel_flags.clone();
        compio::runtime::spawn(async move {
            let result = execute(op, open_id, files, &cancelled).await;
            cancel_flags.borrow_mut().remove(&id);
            cb(result);
        })
        .detach();
    }
}

async fn execute(
    op: Op,
    open_id: Option<u64>,
    files: Files,
    cancelled: &Cell<bool>,
) -> io::Result<Reply> {
    if cancelled.get() {
        return Err(cancelled_error());
    }
    match op {
        Op::Open { path, spec } => {
            let file = open_options(&spec).open(&path).await?;
            let size = file.metadata().await?.len();
            let fd = file.as_raw_fd() as i64;
            let id = open_id.expect("open allocates an id");
            files.borrow_mut().insert(id, file);
            Ok(Reply::Handle { id, size, fd })
        }
        Op::ReadAt { handle, pos, dest } => {
            let file = lookup(&files, handle)?;
            match dest {
                Dest::Alloc { len } => {
                    let (_, buf) =
                        read_up_to(&file, pos, len, Vec::with_capacity(len), cancelled).await?;
                    Ok(Reply::Bytes(buf))
                }
                Dest::Into { ptr, len } => {
                    let raw = RawBufMut {
                        ptr,
                        cap: len,
                        len: 0,
                    };
                    let (n, _) = read_up_to(&file, pos, len, raw, cancelled).await?;
                    Ok(Reply::Read { n })
                }
            }
        }
        Op::ReadToEnd { handle, pos } => {
            let file = lookup(&files, handle)?;
            let size = file.metadata().await?.len().saturating_sub(pos) as usize;
            let BufResult(res, buf) = file.read_to_end_at(Vec::with_capacity(size + 1), pos).await;
            res?;
            Ok(Reply::Bytes(buf))
        }
        Op::WriteAt {
            handle,
            pos,
            data,
            append,
        } => {
            let file = lookup(&files, handle)?;
            let n = data.len();
            if append {
                append_all(&file, data).await?;
                let end = file.metadata().await?.len();
                return Ok(Reply::Written { n, end });
            }
            let mut target = &file;
            let BufResult(res, _) = target.write_all_at(data, pos).await;
            res?;
            Ok(Reply::Written {
                n,
                end: pos + n as u64,
            })
        }
        Op::Close { handle } => {
            let file = files.borrow_mut().remove(&handle).ok_or_else(bad_handle)?;
            file.close().await?;
            Ok(Reply::Unit)
        }
        Op::Sync { handle, data_only } => {
            let file = lookup(&files, handle)?;
            match data_only {
                true => file.sync_data().await?,
                false => file.sync_all().await?,
            }
            Ok(Reply::Unit)
        }
        Op::SetLen { handle, size } => {
            let file = lookup(&files, handle)?;
            file.set_len(size).await?;
            Ok(Reply::Unit)
        }
        Op::Size { handle } => {
            let file = lookup(&files, handle)?;
            Ok(Reply::Size(file.metadata().await?.len()))
        }
        Op::ReadFile { path, inline_max } => {
            let file = OpenOptions::new().read(true).open(&path).await?;
            let size = file.metadata().await?.len();
            if size > inline_max {
                let fd = file.as_raw_fd() as i64;
                let id = open_id.expect("read_file allocates an id");
                files.borrow_mut().insert(id, file);
                return Ok(Reply::Handle { id, size, fd });
            }
            let BufResult(res, buf) = file
                .read_to_end_at(Vec::with_capacity(size as usize + 1), 0)
                .await;
            res?;
            file.close().await?;
            Ok(Reply::Bytes(buf))
        }
        Op::WriteFile { path, data } => {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await?;
            let n = data.len();
            let mut target = &file;
            let BufResult(res, _) = target.write_all_at(data, 0).await;
            res?;
            file.close().await?;
            Ok(Reply::Written { n, end: n as u64 })
        }
        Op::Nop => Ok(Reply::Unit),
    }
}

/// Append via `write(2)`: positional writes on an `O_APPEND` fd append on
/// Linux but honor the offset on macOS, so the only portable append is the
/// plain write path.
#[cfg(unix)]
async fn append_all(file: &File, data: Payload) -> io::Result<()> {
    let fd = file.as_raw_fd();
    compio::runtime::spawn_blocking(move || {
        let slice = data.as_slice();
        let mut written = 0;
        while written < slice.len() {
            let r = unsafe {
                libc::write(
                    fd,
                    slice.as_ptr().add(written) as *const libc::c_void,
                    slice.len() - written,
                )
            };
            if r < 0 {
                return Err(io::Error::last_os_error());
            }
            written += r as usize;
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::other("append task cancelled"))?
}

#[cfg(not(unix))]
async fn append_all(file: &File, data: Payload) -> io::Result<()> {
    let end = file.metadata().await?.len();
    let mut target = file;
    let BufResult(res, _) = target.write_all_at(data, end).await;
    res
}

/// Read up to `len` bytes from `pos`, stopping early only at EOF or a cancel
/// request between chunks. The buffer fills from its current initialized
/// length.
async fn read_up_to<B: IoBufMut + 'static>(
    file: &File,
    pos: u64,
    len: usize,
    mut buf: B,
    cancelled: &Cell<bool>,
) -> io::Result<(usize, B)> {
    loop {
        if cancelled.get() {
            return Err(cancelled_error());
        }
        let filled = buf.buf_len();
        if filled >= len {
            return Ok((filled, buf));
        }
        let BufResult(res, ret) = file.read_at(buf, pos + filled as u64).await;
        buf = ret;
        if res? == 0 {
            return Ok((buf.buf_len(), buf));
        }
    }
}

fn lookup(files: &Files, handle: u64) -> io::Result<File> {
    files.borrow().get(&handle).cloned().ok_or_else(bad_handle)
}

fn open_options(spec: &OpenSpec) -> OpenOptions {
    let mut opts = OpenOptions::new();
    opts.read(spec.read)
        .write(spec.write || spec.append)
        .truncate(spec.truncate)
        .create(spec.create)
        .create_new(spec.create_new);
    #[cfg(unix)]
    if spec.append {
        opts.custom_flags(libc::O_APPEND);
    }
    opts
}
