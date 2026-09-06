//! POSIX AIO backend for macOS. One driver thread submits aio ops and reaps
//! completions with a timed `aio_suspend` loop; XNU has no kqueue completion
//! delivery, so the timeout doubles as the new-submission latency bound.

use std::collections::{HashMap, VecDeque};
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::{
    bad_handle, cancelled_error, regular_file_size, Callback, Dest, Msg, Op, OpenSpec, Payload,
    Reply,
};

/// Upper bound for the `aio_suspend` wait while ops are in flight; newly
/// submitted ops wait at most this long before the driver notices them.
const SUSPEND_TIMEOUT_NS: i64 = 1_000_000;

/// XNU rejects submissions beyond kern.aioprocmax with EAGAIN; staying under
/// it keeps the pending queue in userspace where it is observable.
const FALLBACK_MAX_INFLIGHT: usize = 16;

pub(crate) fn spawn(rx: flume::Receiver<Msg>) -> io::Result<()> {
    std::thread::Builder::new()
        .name("turbofile-aio".into())
        .spawn(move || Driver::new(rx).run())?;
    Ok(())
}

struct FileEntry {
    fd: i32,
    ops: usize,
    closing: Option<Callback>,
}

enum Want {
    Exact(usize),
    ToEnd,
}

enum ReadBuf {
    Owned(Vec<u8>),
    External { ptr: *mut u8, len: usize },
}

impl ReadBuf {
    fn chunk(&mut self, filled: usize) -> (*mut u8, usize) {
        match self {
            ReadBuf::Owned(vec) => unsafe {
                (vec.as_mut_ptr().add(filled), vec.capacity() - filled)
            },
            ReadBuf::External { ptr, len } => unsafe { (ptr.add(filled), *len - filled) },
        }
    }
}

struct ReadJob {
    handle: Option<u64>,
    fd: i32,
    owned_fd: bool,
    pos: u64,
    want: Want,
    buf: ReadBuf,
    filled: usize,
}

impl ReadJob {
    /// Whether the bytes read so far are all the op asked for: the requested
    /// count for a sized read, the file's current end for a read to end. Only
    /// a regular file's size is trusted; anything else reads on until a
    /// zero-length result.
    fn complete(&self) -> bool {
        match self.want {
            Want::Exact(len) => self.filled >= len,
            Want::ToEnd => {
                regular_file_size(self.fd).is_some_and(|size| self.pos + self.filled as u64 >= size)
            }
        }
    }
}

struct WriteJob {
    handle: Option<u64>,
    fd: i32,
    owned_fd: bool,
    pos: u64,
    append: bool,
    data: Payload,
    filled: usize,
}

struct FsyncJob {
    handle: u64,
    fd: i32,
}

enum JobKind {
    Read(ReadJob),
    Write(WriteJob),
    Fsync(FsyncJob),
}

struct Job {
    id: u64,
    cancelled: bool,
    kind: JobKind,
    cb: Callback,
}

struct Inflight {
    aiocb: Box<libc::aiocb>,
    job: Job,
}

struct Driver {
    rx: flume::Receiver<Msg>,
    files: HashMap<u64, FileEntry>,
    next_id: u64,
    inflight: Vec<Inflight>,
    queue: VecDeque<Job>,
    max_inflight: usize,
    disconnected: bool,
}

impl Driver {
    fn new(rx: flume::Receiver<Msg>) -> Self {
        Self {
            rx,
            files: HashMap::new(),
            next_id: 1,
            inflight: Vec::new(),
            queue: VecDeque::new(),
            max_inflight: aio_proc_max(),
            disconnected: false,
        }
    }

    fn run(mut self) {
        loop {
            if self.inflight.is_empty() && self.queue.is_empty() {
                if self.disconnected {
                    return;
                }
                match self.rx.recv() {
                    Ok(msg) => self.handle_msg(msg),
                    Err(_) => return,
                }
            }
            if !self.inflight.is_empty() {
                self.suspend();
                self.reap();
            }
            self.drain_channel();
            self.submit_ready();
        }
    }

    fn suspend(&self) {
        let list: Vec<*const libc::aiocb> = self
            .inflight
            .iter()
            .map(|entry| &*entry.aiocb as *const libc::aiocb)
            .collect();
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: SUSPEND_TIMEOUT_NS,
        };
        unsafe {
            libc::aio_suspend(list.as_ptr(), list.len() as libc::c_int, &timeout);
        }
    }

    fn reap(&mut self) {
        let mut index = 0;
        while index < self.inflight.len() {
            let err = unsafe { libc::aio_error(&*self.inflight[index].aiocb) };
            if err == libc::EINPROGRESS {
                index += 1;
                continue;
            }
            let mut entry = self.inflight.swap_remove(index);
            let n = unsafe { libc::aio_return(&mut *entry.aiocb) };
            match err {
                0 => self.advance(entry.job, n as usize),
                e => self.finish(entry.job, Err(io::Error::from_raw_os_error(e))),
            }
        }
    }

    fn drain_channel(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => self.handle_msg(msg),
                Err(flume::TryRecvError::Empty) => return,
                Err(flume::TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    return;
                }
            }
        }
    }

    fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Submit { id, op, cb } => self.handle_op(id, op, cb),
            Msg::Cancel { id } => self.cancel(id),
        }
    }

    /// A queued job settles ECANCELED immediately; an inflight one gets
    /// `aio_cancel` and is flagged so a surviving chunk is not resubmitted.
    /// An unknown id already finished: nothing to do.
    fn cancel(&mut self, id: u64) {
        if let Some(pos) = self.queue.iter().position(|job| job.id == id) {
            let job = self.queue.remove(pos).expect("position is in bounds");
            self.finish(job, Err(cancelled_error()));
            return;
        }
        let Some(entry) = self.inflight.iter_mut().find(|entry| entry.job.id == id) else {
            return;
        };
        entry.job.cancelled = true;
        unsafe { libc::aio_cancel(entry.aiocb.aio_fildes, &mut *entry.aiocb) };
    }

    fn handle_op(&mut self, id: u64, op: Op, cb: Callback) {
        match op {
            Op::Nop => cb(Ok(Reply::Unit)),
            Op::Open { path, spec } => cb(self.open(&path, &spec)),
            Op::Close { handle } => self.close(handle, cb),
            Op::Size { handle } => cb(self
                .with_fd(handle)
                .and_then(|fd| fd_size(fd).map(Reply::Size))),
            Op::SetLen { handle, size } => cb(self.with_fd(handle).and_then(|fd| {
                match unsafe { libc::ftruncate(fd, size as libc::off_t) } {
                    0 => Ok(Reply::Unit),
                    _ => Err(io::Error::last_os_error()),
                }
            })),
            Op::Sync { handle, .. } => match self.with_fd(handle) {
                Ok(fd) => self.enqueue(
                    id,
                    JobKind::Fsync(FsyncJob { handle, fd }),
                    Some(handle),
                    cb,
                ),
                Err(e) => cb(Err(e)),
            },
            Op::ReadAt { handle, pos, dest } => match self.with_fd(handle) {
                Ok(fd) => {
                    // XNU may reject zero-length aio submissions; answer
                    // directly instead of finding out.
                    match &dest {
                        Dest::Alloc { len: 0 } => {
                            cb(Ok(Reply::Bytes(Vec::new())));
                            return;
                        }
                        Dest::Into { len: 0, .. } => {
                            cb(Ok(Reply::Read { n: 0 }));
                            return;
                        }
                        _ => {}
                    }
                    let (want, buf) = match dest {
                        Dest::Alloc { len } => {
                            (Want::Exact(len), ReadBuf::Owned(Vec::with_capacity(len)))
                        }
                        Dest::Into { ptr, len } => {
                            (Want::Exact(len), ReadBuf::External { ptr, len })
                        }
                    };
                    self.enqueue(
                        id,
                        JobKind::Read(ReadJob {
                            handle: Some(handle),
                            fd,
                            owned_fd: false,
                            pos,
                            want,
                            buf,
                            filled: 0,
                        }),
                        Some(handle),
                        cb,
                    );
                }
                Err(e) => cb(Err(e)),
            },
            Op::ReadToEnd { handle, pos } => match self.with_fd(handle) {
                Ok(fd) => {
                    let hint = fd_size(fd)
                        .map(|size| size.saturating_sub(pos) as usize)
                        .unwrap_or(0);
                    self.enqueue(
                        id,
                        JobKind::Read(ReadJob {
                            handle: Some(handle),
                            fd,
                            owned_fd: false,
                            pos,
                            want: Want::ToEnd,
                            buf: ReadBuf::Owned(Vec::with_capacity(hint.max(1))),
                            filled: 0,
                        }),
                        Some(handle),
                        cb,
                    );
                }
                Err(e) => cb(Err(e)),
            },
            Op::WriteAt {
                handle,
                pos,
                data,
                append,
            } => match self.with_fd(handle) {
                Ok(fd) if data.is_empty() => {
                    let end = match append {
                        true => fd_size(fd),
                        false => Ok(pos),
                    };
                    cb(end.map(|end| Reply::Written { n: 0, end }));
                }
                Ok(fd) => self.enqueue(
                    id,
                    JobKind::Write(WriteJob {
                        handle: Some(handle),
                        fd,
                        owned_fd: false,
                        pos,
                        append,
                        data,
                        filled: 0,
                    }),
                    Some(handle),
                    cb,
                ),
                Err(e) => cb(Err(e)),
            },
            Op::ReadFile { path, inline_max } => {
                let opened = open_sized(
                    &path,
                    &OpenSpec {
                        read: true,
                        ..OpenSpec::default()
                    },
                );
                let (fd, size) = match opened {
                    Ok(opened) => opened,
                    Err(e) => {
                        cb(Err(e));
                        return;
                    }
                };
                if size > inline_max {
                    cb(Ok(self.register(fd, size)));
                    return;
                }
                self.enqueue(
                    id,
                    JobKind::Read(ReadJob {
                        handle: None,
                        fd,
                        owned_fd: true,
                        pos: 0,
                        want: Want::ToEnd,
                        buf: ReadBuf::Owned(Vec::with_capacity((size as usize).max(1))),
                        filled: 0,
                    }),
                    None,
                    cb,
                );
            }
            Op::WriteFile { path, data } => {
                let opened = open_raw(
                    &path,
                    &OpenSpec {
                        write: true,
                        create: true,
                        truncate: true,
                        ..OpenSpec::default()
                    },
                );
                match opened {
                    Ok(fd) => self.enqueue(
                        id,
                        JobKind::Write(WriteJob {
                            handle: None,
                            fd,
                            owned_fd: true,
                            pos: 0,
                            append: false,
                            data,
                            filled: 0,
                        }),
                        None,
                        cb,
                    ),
                    Err(e) => cb(Err(e)),
                }
            }
        }
    }

    fn open(&mut self, path: &Path, spec: &OpenSpec) -> io::Result<Reply> {
        let (fd, size) = open_sized(path, spec)?;
        Ok(self.register(fd, size))
    }

    /// Take ownership of an open descriptor as a new handle.
    fn register(&mut self, fd: i32, size: u64) -> Reply {
        let id = self.next_id;
        self.next_id += 1;
        self.files.insert(
            id,
            FileEntry {
                fd,
                ops: 0,
                closing: None,
            },
        );
        Reply::Handle {
            id,
            size,
            fd: fd as i64,
        }
    }

    fn close(&mut self, handle: u64, cb: Callback) {
        let Some(entry) = self.files.get_mut(&handle) else {
            cb(Err(bad_handle()));
            return;
        };
        if entry.closing.is_some() {
            cb(Err(bad_handle()));
            return;
        }
        if entry.ops > 0 {
            entry.closing = Some(cb);
            return;
        }
        let fd = entry.fd;
        self.files.remove(&handle);
        cb(close_raw(fd).map(|_| Reply::Unit));
    }

    fn with_fd(&self, handle: u64) -> io::Result<i32> {
        let entry = self.files.get(&handle).ok_or_else(bad_handle)?;
        if entry.closing.is_some() {
            return Err(bad_handle());
        }
        Ok(entry.fd)
    }

    fn enqueue(&mut self, id: u64, kind: JobKind, handle: Option<u64>, cb: Callback) {
        if let Some(handle) = handle {
            if let Some(entry) = self.files.get_mut(&handle) {
                entry.ops += 1;
            }
        }
        self.queue.push_back(Job {
            id,
            cancelled: false,
            kind,
            cb,
        });
        self.submit_ready();
    }

    fn submit_ready(&mut self) {
        while self.inflight.len() < self.max_inflight {
            let Some(job) = self.queue.pop_front() else {
                return;
            };
            match self.submit(job) {
                Submitted::Inflight => {}
                Submitted::Full(job) => {
                    self.queue.push_front(job);
                    return;
                }
            }
        }
    }

    fn submit(&mut self, mut job: Job) -> Submitted {
        let mut aiocb: Box<libc::aiocb> = Box::new(unsafe { std::mem::zeroed() });
        aiocb.aio_sigevent.sigev_notify = libc::SIGEV_NONE;

        let submitted = match &mut job.kind {
            JobKind::Read(read) => {
                let (ptr, remaining) = read.buf.chunk(read.filled);
                aiocb.aio_fildes = read.fd;
                aiocb.aio_offset = (read.pos + read.filled as u64) as libc::off_t;
                aiocb.aio_buf = ptr as *mut libc::c_void;
                aiocb.aio_nbytes = remaining;
                unsafe { libc::aio_read(&mut *aiocb) }
            }
            JobKind::Write(write) => {
                let slice = write.data.as_slice();
                aiocb.aio_fildes = write.fd;
                aiocb.aio_offset = (write.pos + write.filled as u64) as libc::off_t;
                aiocb.aio_buf = unsafe { slice.as_ptr().add(write.filled) } as *mut libc::c_void;
                aiocb.aio_nbytes = slice.len() - write.filled;
                unsafe { libc::aio_write(&mut *aiocb) }
            }
            JobKind::Fsync(fsync) => {
                aiocb.aio_fildes = fsync.fd;
                unsafe { libc::aio_fsync(libc::O_SYNC, &mut *aiocb) }
            }
        };

        if submitted == 0 {
            self.inflight.push(Inflight { aiocb, job });
            return Submitted::Inflight;
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EAGAIN) {
            if self.inflight.is_empty() {
                self.execute_sync(job);
                return Submitted::Inflight;
            }
            return Submitted::Full(job);
        }
        self.finish(job, Err(err));
        Submitted::Inflight
    }

    /// Progress fallback when kern.aiomax is exhausted by other processes:
    /// run the whole job with plain syscalls so nothing waits on slots this
    /// process cannot get.
    fn execute_sync(&mut self, mut job: Job) {
        loop {
            let result = match &mut job.kind {
                JobKind::Read(read) => {
                    let (ptr, remaining) = read.buf.chunk(read.filled);
                    let n = unsafe {
                        libc::pread(
                            read.fd,
                            ptr as *mut libc::c_void,
                            remaining,
                            (read.pos + read.filled as u64) as libc::off_t,
                        )
                    };
                    match n {
                        -1 => Err(io::Error::last_os_error()),
                        n => Ok(n as usize),
                    }
                }
                JobKind::Write(write) => {
                    let slice = write.data.as_slice();
                    let remaining = slice.len() - write.filled;
                    let n = match write.append {
                        true => unsafe {
                            libc::write(
                                write.fd,
                                slice.as_ptr().add(write.filled) as *const libc::c_void,
                                remaining,
                            )
                        },
                        false => unsafe {
                            libc::pwrite(
                                write.fd,
                                slice.as_ptr().add(write.filled) as *const libc::c_void,
                                remaining,
                                (write.pos + write.filled as u64) as libc::off_t,
                            )
                        },
                    };
                    match n {
                        -1 => Err(io::Error::last_os_error()),
                        n => Ok(n as usize),
                    }
                }
                JobKind::Fsync(fsync) => match unsafe { libc::fsync(fsync.fd) } {
                    0 => Ok(0),
                    _ => Err(io::Error::last_os_error()),
                },
            };
            let n = match result {
                Ok(n) => n,
                Err(e) => {
                    self.finish(job, Err(e));
                    return;
                }
            };
            match step(&mut job, n) {
                Step::Done(reply) => {
                    self.finish(job, reply);
                    return;
                }
                Step::More => {}
            }
        }
    }

    /// One chunk completed with `n` bytes; either resubmit the remainder or
    /// finish the job. A job whose chunk survived its cancel request settles
    /// ECANCELED instead of resubmitting; a fully completed one keeps its
    /// result (the op won the race).
    fn advance(&mut self, mut job: Job, n: usize) {
        match step(&mut job, n) {
            Step::Done(reply) => self.finish(job, reply),
            Step::More if job.cancelled => self.finish(job, Err(cancelled_error())),
            Step::More => {
                self.queue.push_front(job);
                self.submit_ready();
            }
        }
    }

    fn finish(&mut self, job: Job, result: io::Result<Reply>) {
        let (handle, owned_fd, fd) = match &job.kind {
            JobKind::Read(read) => (read.handle, read.owned_fd, read.fd),
            JobKind::Write(write) => (write.handle, write.owned_fd, write.fd),
            JobKind::Fsync(fsync) => (Some(fsync.handle), false, fsync.fd),
        };
        if owned_fd {
            close_raw(fd).ok();
        }
        (job.cb)(result);
        let Some(handle) = handle else {
            return;
        };
        let Some(entry) = self.files.get_mut(&handle) else {
            return;
        };
        entry.ops -= 1;
        if entry.ops > 0 || entry.closing.is_none() {
            return;
        }
        let entry = self.files.remove(&handle).expect("entry present");
        let cb = entry.closing.expect("closing set");
        cb(close_raw(entry.fd).map(|_| Reply::Unit));
    }
}

enum Submitted {
    Inflight,
    Full(Job),
}

enum Step {
    Done(io::Result<Reply>),
    More,
}

fn step(job: &mut Job, n: usize) -> Step {
    match &mut job.kind {
        JobKind::Read(read) => {
            read.filled += n;
            if n == 0 || read.complete() {
                return Step::Done(read_reply(read));
            }
            if let ReadBuf::Owned(vec) = &mut read.buf {
                if read.filled == vec.capacity() {
                    vec.reserve(vec.capacity().max(65536));
                }
            }
            Step::More
        }
        JobKind::Write(write) => {
            write.filled += n;
            if write.filled < write.data.len() {
                return Step::More;
            }
            let end = match write.append {
                true => fd_size(write.fd),
                false => Ok(write.pos + write.data.len() as u64),
            };
            Step::Done(end.map(|end| Reply::Written {
                n: write.data.len(),
                end,
            }))
        }
        JobKind::Fsync(_) => Step::Done(Ok(Reply::Unit)),
    }
}

fn read_reply(read: &mut ReadJob) -> io::Result<Reply> {
    match &mut read.buf {
        ReadBuf::Owned(vec) => {
            unsafe { vec.set_len(read.filled) };
            Ok(Reply::Bytes(std::mem::take(vec)))
        }
        ReadBuf::External { .. } => Ok(Reply::Read { n: read.filled }),
    }
}

fn aio_proc_max() -> usize {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    let name = c"kern.aioprocmax";
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut value as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    match rc {
        0 if value > 0 => value as usize,
        _ => FALLBACK_MAX_INFLIGHT,
    }
}

fn open_raw(path: &Path, spec: &OpenSpec) -> io::Result<i32> {
    let cpath = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let access = match (spec.read, spec.write || spec.append) {
        (true, true) => libc::O_RDWR,
        (true, false) => libc::O_RDONLY,
        (false, true) => libc::O_WRONLY,
        (false, false) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "open needs read or write access",
            ));
        }
    };
    let mut flags = access | libc::O_CLOEXEC;
    if spec.append {
        flags |= libc::O_APPEND;
    }
    if spec.truncate {
        flags |= libc::O_TRUNC;
    }
    if spec.create {
        flags |= libc::O_CREAT;
    }
    if spec.create_new {
        flags |= libc::O_CREAT | libc::O_EXCL;
    }
    match unsafe { libc::open(cpath.as_ptr(), flags, 0o666 as libc::c_uint) } {
        -1 => Err(io::Error::last_os_error()),
        fd => Ok(fd),
    }
}

/// Open plus the size an open handle reports; the descriptor is closed again
/// if the size cannot be read.
fn open_sized(path: &Path, spec: &OpenSpec) -> io::Result<(i32, u64)> {
    let fd = open_raw(path, spec)?;
    match fd_size(fd) {
        Ok(size) => Ok((fd, size)),
        Err(e) => {
            close_raw(fd).ok();
            Err(e)
        }
    }
}

fn close_raw(fd: i32) -> io::Result<()> {
    match unsafe { libc::close(fd) } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

fn fd_size(fd: i32) -> io::Result<u64> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    match unsafe { libc::fstat(fd, &mut stat) } {
        0 => Ok(stat.st_size as u64),
        _ => Err(io::Error::last_os_error()),
    }
}
