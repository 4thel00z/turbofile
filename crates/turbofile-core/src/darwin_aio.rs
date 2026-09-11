//! POSIX AIO backend for macOS. One driver thread submits aio ops and reaps
//! completions with a timed `aio_suspend` loop; XNU has no kqueue completion
//! delivery, so the timeout doubles as the new-submission latency bound. The
//! timeout adapts: short after a pass that reaped a completion or handled a
//! message, doubling toward a cap while nothing moves. Opens run on the driver
//! thread while it has nothing else waiting; under a backlog they go to a few
//! helper threads and the descriptors come back as messages, so a burst of
//! whole-file ops is not serialized behind one thread's `open` calls.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use crate::{
    bad_handle, cancelled_error, regular_file_size, Callback, Dest, Msg, Op, OpenSpec, Payload,
    Reply,
};

/// The `aio_suspend` wait right after a pass that made progress. A submission
/// arriving while ops are in flight waits at most this long to be noticed, as
/// long as traffic keeps flowing.
const SUSPEND_MIN_NS: i64 = 20_000;

/// Cap on the wait once passes stop making progress: the driver doubles from
/// the minimum up to here, so a lull costs at most this much notice latency
/// while a wake every cap-length period keeps the polling cost near zero.
const SUSPEND_MAX_NS: i64 = 160_000;

/// XNU rejects submissions beyond kern.aioprocmax with EAGAIN; staying under
/// it keeps the pending queue in userspace where it is observable.
const FALLBACK_MAX_INFLIGHT: usize = 16;

/// Bounds on the threads that run `open` (and the close of a descriptor the
/// driver owns) while the driver has a backlog. An open costs several
/// microseconds of kernel and endpoint-security work per file, about a third
/// of it serialized system-wide, so it scales on a few threads and no more;
/// the completions still go through the single `aio_suspend` loop. One helper
/// per three hardware threads: on a 12-thread machine (six performance and
/// six efficiency cores) four helpers beat three by a tenth on a 200-file
/// burst and five and six lose, since the driver, the event loop and the
/// kernel's AIO threads want cores too.
const MIN_OPEN_THREADS: usize = 2;
const MAX_OPEN_THREADS: usize = 4;

pub(crate) fn spawn(rx: flume::Receiver<Msg>) -> io::Result<()> {
    let (helper_tx, helper_rx) = flume::unbounded();
    let (back_tx, back_rx) = flume::unbounded();
    for _ in 0..open_threads() {
        let jobs = helper_rx.clone();
        let back = back_tx.clone();
        std::thread::Builder::new()
            .name("turbofile-open".into())
            .spawn(move || helper_loop(jobs, back))?;
    }
    std::thread::Builder::new()
        .name("turbofile-aio".into())
        .spawn(move || Driver::new(rx, helper_tx, back_rx).run())?;
    Ok(())
}

fn open_threads() -> usize {
    let hardware = std::thread::available_parallelism().map_or(1, |n| n.get());
    helpers_for(hardware)
}

fn helpers_for(hardware: usize) -> usize {
    (hardware / 3).clamp(MIN_OPEN_THREADS, MAX_OPEN_THREADS)
}

/// Work handed to a helper thread: an open whose descriptor comes back to the
/// driver as an [`Opened`] message, or a close nothing waits for.
enum HelperJob {
    Open {
        path: PathBuf,
        spec: OpenSpec,
        pending: PendingOpen,
    },
    Close(i32),
}

/// An op between its submission and its open: what the driver does with the
/// descriptor once it exists.
struct PendingOpen {
    id: u64,
    then: AfterOpen,
    cb: Callback,
}

enum AfterOpen {
    Handle,
    ReadFile { inline_max: u64 },
    WriteFile { data: Payload },
}

struct Opened {
    pending: PendingOpen,
    result: io::Result<(i32, u64)>,
}

fn helper_loop(jobs: flume::Receiver<HelperJob>, back: flume::Sender<Opened>) {
    for job in jobs.iter() {
        match job {
            HelperJob::Close(fd) => {
                close_raw(fd).ok();
            }
            HelperJob::Open {
                path,
                spec,
                pending,
            } => {
                let result = open_for(&path, &spec, &pending.then);
                let Err(flume::SendError(opened)) = back.send(Opened { pending, result }) else {
                    continue;
                };
                if let Ok((fd, _)) = opened.result {
                    close_raw(fd).ok();
                }
                (opened.pending.cb)(Err(driver_gone()));
            }
        }
    }
}

/// The open an op needs: a write to a fresh file has no size worth asking for.
fn open_for(path: &Path, spec: &OpenSpec, then: &AfterOpen) -> io::Result<(i32, u64)> {
    match then {
        AfterOpen::WriteFile { .. } => open_raw(path, spec).map(|fd| (fd, 0)),
        AfterOpen::Handle | AfterOpen::ReadFile { .. } => open_sized(path, spec),
    }
}

fn driver_gone() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "turbofile driver thread is gone")
}

enum Wake {
    Msg(Msg),
    Opened(Opened),
    SubmittersGone,
    HelpersGone,
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
    helper_tx: flume::Sender<HelperJob>,
    back_rx: flume::Receiver<Opened>,
    pending_opens: HashSet<u64>,
    cancelled_opens: HashSet<u64>,
    helpers_gone: bool,
    files: HashMap<u64, FileEntry>,
    next_id: u64,
    inflight: Vec<Inflight>,
    queue: VecDeque<Job>,
    max_inflight: usize,
    disconnected: bool,
    suspend_ns: i64,
    progress: bool,
}

impl Driver {
    fn new(
        rx: flume::Receiver<Msg>,
        helper_tx: flume::Sender<HelperJob>,
        back_rx: flume::Receiver<Opened>,
    ) -> Self {
        Self {
            rx,
            helper_tx,
            back_rx,
            pending_opens: HashSet::new(),
            cancelled_opens: HashSet::new(),
            helpers_gone: false,
            files: HashMap::new(),
            next_id: 1,
            inflight: Vec::new(),
            queue: VecDeque::new(),
            max_inflight: aio_proc_max(),
            disconnected: false,
            suspend_ns: SUSPEND_MIN_NS,
            progress: false,
        }
    }

    fn run(mut self) {
        loop {
            if self.inflight.is_empty() && self.queue.is_empty() {
                if self.disconnected && self.pending_opens.is_empty() {
                    return;
                }
                self.wait_idle();
            }
            if !self.inflight.is_empty() {
                self.suspend();
                self.reap();
            }
            self.drain_channel();
            self.submit_ready();
            // Progress covers everything since the last wait was chosen: a
            // message taken while idle above, and the completions and messages
            // of this pass.
            self.suspend_ns = match std::mem::take(&mut self.progress) {
                true => SUSPEND_MIN_NS,
                false => (self.suspend_ns * 2).min(SUSPEND_MAX_NS),
            };
        }
    }

    /// Block until a submission or an opened descriptor arrives. Once the
    /// submitters are gone only the helpers can still have something to say;
    /// once the helpers are gone only the submitters can.
    fn wait_idle(&mut self) {
        if self.disconnected {
            match self.back_rx.recv() {
                Ok(opened) => self.handle_opened(opened),
                Err(_) => self.lose_helpers(),
            }
            return;
        }
        if self.helpers_gone {
            match self.rx.recv() {
                Ok(msg) => self.handle_msg(msg),
                Err(_) => self.disconnected = true,
            }
            return;
        }
        let wake = flume::Selector::new()
            .recv(&self.rx, |msg| {
                msg.map(Wake::Msg).unwrap_or(Wake::SubmittersGone)
            })
            .recv(&self.back_rx, |opened| {
                opened.map(Wake::Opened).unwrap_or(Wake::HelpersGone)
            })
            .wait();
        match wake {
            Wake::Msg(msg) => self.handle_msg(msg),
            Wake::Opened(opened) => self.handle_opened(opened),
            Wake::SubmittersGone => self.disconnected = true,
            Wake::HelpersGone => self.lose_helpers(),
        }
    }

    /// Every helper thread has exited, which nothing in `helper_loop` can
    /// cause today. Opens still out never come back; from here every job the
    /// helpers would have taken runs on this thread instead.
    fn lose_helpers(&mut self) {
        self.helpers_gone = true;
        self.pending_opens.clear();
    }

    fn suspend(&self) {
        let list: Vec<*const libc::aiocb> = self
            .inflight
            .iter()
            .map(|entry| &*entry.aiocb as *const libc::aiocb)
            .collect();
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: self.suspend_ns,
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
            self.progress = true;
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
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    break;
                }
            }
        }
        loop {
            match self.back_rx.try_recv() {
                Ok(opened) => self.handle_opened(opened),
                Err(flume::TryRecvError::Empty) => return,
                Err(flume::TryRecvError::Disconnected) => {
                    self.lose_helpers();
                    return;
                }
            }
        }
    }

    /// Whether anything is waiting on this thread: submissions not yet taken,
    /// jobs waiting for an AIO slot, or opens out at the helpers. While it is,
    /// a blocking call on this thread holds all of it up.
    fn has_backlog(&self) -> bool {
        !self.rx.is_empty() || !self.queue.is_empty() || !self.pending_opens.is_empty()
    }

    fn handle_msg(&mut self, msg: Msg) {
        self.progress = true;
        match msg {
            Msg::Submit { id, op, cb } => self.handle_op(id, op, cb),
            Msg::Cancel { id } => self.cancel(id),
        }
    }

    /// A queued job settles ECANCELED immediately; an inflight one gets
    /// `aio_cancel` and is flagged so a surviving chunk is not resubmitted;
    /// one still at its open on a helper thread is marked and settles
    /// ECANCELED when the open comes back. An unknown id already finished:
    /// nothing to do.
    fn cancel(&mut self, id: u64) {
        if let Some(pos) = self.queue.iter().position(|job| job.id == id) {
            let job = self.queue.remove(pos).expect("position is in bounds");
            self.finish(job, Err(cancelled_error()));
            return;
        }
        let Some(entry) = self.inflight.iter_mut().find(|entry| entry.job.id == id) else {
            if self.pending_opens.contains(&id) {
                self.cancelled_opens.insert(id);
            }
            return;
        };
        entry.job.cancelled = true;
        unsafe { libc::aio_cancel(entry.aiocb.aio_fildes, &mut *entry.aiocb) };
    }

    fn handle_op(&mut self, id: u64, op: Op, cb: Callback) {
        match op {
            Op::Nop => cb(Ok(Reply::Unit)),
            Op::Open { path, spec } => self.open_then(id, path, spec, AfterOpen::Handle, cb),
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
            Op::ReadFile { path, inline_max } => self.open_then(
                id,
                path,
                OpenSpec {
                    read: true,
                    ..OpenSpec::default()
                },
                AfterOpen::ReadFile { inline_max },
                cb,
            ),
            Op::WriteFile { path, data } => self.open_then(
                id,
                path,
                OpenSpec {
                    write: true,
                    create: true,
                    truncate: true,
                    ..OpenSpec::default()
                },
                AfterOpen::WriteFile { data },
                cb,
            ),
        }
    }

    /// Open on this thread when nothing else is waiting on it, so a lone op
    /// pays no extra hop; under a backlog hand the open to a helper thread
    /// and continue with the descriptor when it comes back as a message.
    fn open_then(&mut self, id: u64, path: PathBuf, spec: OpenSpec, then: AfterOpen, cb: Callback) {
        let pending = PendingOpen { id, then, cb };
        if !self.has_backlog() {
            let result = open_for(&path, &spec, &pending.then);
            self.opened(pending, result);
            return;
        }
        self.pending_opens.insert(id);
        let job = HelperJob::Open {
            path,
            spec,
            pending,
        };
        if let Err(flume::SendError(job)) = self.helper_tx.send(job) {
            self.pending_opens.remove(&id);
            self.run_here(job);
        }
    }

    /// A job the helpers could not take, run on this thread: an open settles
    /// its op, a close closes.
    fn run_here(&mut self, job: HelperJob) {
        match job {
            HelperJob::Open {
                path,
                spec,
                pending,
            } => {
                let result = open_for(&path, &spec, &pending.then);
                self.opened(pending, result);
            }
            HelperJob::Close(fd) => {
                close_raw(fd).ok();
            }
        }
    }

    fn handle_opened(&mut self, opened: Opened) {
        self.progress = true;
        let id = opened.pending.id;
        self.pending_opens.remove(&id);
        if self.cancelled_opens.remove(&id) {
            if let Ok((fd, _)) = opened.result {
                close_raw(fd).ok();
            }
            (opened.pending.cb)(Err(cancelled_error()));
            return;
        }
        self.opened(opened.pending, opened.result);
    }

    fn opened(&mut self, pending: PendingOpen, result: io::Result<(i32, u64)>) {
        let PendingOpen { id, then, cb } = pending;
        let (fd, size) = match result {
            Ok(opened) => opened,
            Err(e) => {
                cb(Err(e));
                return;
            }
        };
        match then {
            AfterOpen::Handle => cb(Ok(self.register(fd, size))),
            AfterOpen::ReadFile { inline_max } if size > inline_max => {
                cb(Ok(self.register(fd, size)))
            }
            AfterOpen::ReadFile { .. } => self.enqueue(
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
            ),
            AfterOpen::WriteFile { data } => self.enqueue(
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
        }
    }

    /// Close a descriptor only this driver knows about. Nothing waits for
    /// it, so under a backlog a helper thread takes the call.
    fn close_owned(&mut self, fd: i32) {
        if !self.has_backlog() {
            close_raw(fd).ok();
            return;
        }
        if let Err(flume::SendError(job)) = self.helper_tx.send(HelperJob::Close(fd)) {
            self.run_here(job);
        }
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
            self.close_owned(fd);
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

#[cfg(test)]
mod tests {
    use super::helpers_for;

    #[test]
    fn helper_count_follows_hardware_threads() {
        assert_eq!(helpers_for(1), 2);
        assert_eq!(helpers_for(8), 2);
        assert_eq!(helpers_for(10), 3);
        assert_eq!(helpers_for(12), 4);
        assert_eq!(helpers_for(32), 4);
    }
}
