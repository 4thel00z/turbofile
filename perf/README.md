# turbofile performance analysis

A layered harness for attributing turbofile's per-operation cost to a specific
layer, rather than guessing at it. Modelled on the method in
[no eBPF for macOS](https://tahrioui.de/blog/no-ebpf-for-macos): build probes
whose ground truth you already know, and read the application's numbers as
*ratios against those probes*. That post had to do it without kernel tracing
because macOS has none to offer; here the same discipline applies for a
different reason, since a stock Ubuntu box ships `perf_event_paranoid=4` and a
root-only `tracefs`, so every kernel tier is locked until you open it.

## Unlocking the kernel tiers

Everything in `ladder.py` and `wake_probe.py` runs unprivileged. `counters.sh`,
`flame.sh` and `uring.bt` need capabilities:

```sh
sudo setcap 'cap_perfmon,cap_bpf,cap_sys_ptrace,cap_dac_read_search+ep' "$(command -v perf)"
sudo setcap 'cap_perfmon,cap_bpf,cap_sys_ptrace,cap_dac_read_search,cap_dac_override+ep' "$(command -v bpftrace)"
sudo sysctl -w kernel.kptr_restrict=0
```

`CAP_PERFMON` bypasses `perf_event_paranoid` and `CAP_BPF` bypasses
`unprivileged_bpf_disabled`, so no system-wide sysctl loosening is required.
bpftrace additionally checks for `CAP_DAC_OVERRIDE` because it writes probe
definitions into tracefs.

## The tools

| target | what it answers |
| ------ | --------------- |
| `make ladder` | *Which layer* owns the time |
| `make wake` | Is a slow thread hop this machine's floor, or ours |
| `make counters` | *Why* — context switches, IPC, stalls |
| `make flame` | *Where* in the code |
| `make uring PID=…` | What the kernel did with each submission |

Every target takes `FS=<dir>` to choose the filesystem. **Use it.** See below.

## The ladder

`ladder.py` measures rungs that each add exactly one layer to the rung below,
so the difference between two adjacent rungs is that layer and nothing else:

```
py_coro   -> ffi -> future -> bridge        additive chain
pread                                       reference floor (not a layer)
try_read / file_read / read / read_bytes    real paths
```

`bridge` submits an `Op::Nop` — a full submit → driver thread → doorbell →
drain round trip that performs no kernel work at all. `bridge - future` is
therefore the bridge's own cost, isolated from any I/O. `read / pread` is the
overhead multiple: what the same bytes cost through turbofile versus through a
plain blocking syscall.

## Measurement discipline

- **Warm up.** This CPU boosts; the first samples are worthless. Every rung is
  warmed before it is timed.
- **Read `min`, not `mean`.** Absolute times drift with boost and load. `min`
  is the least contaminated estimator and the ratios are what matter.
- **Check the load.** A `load average` above ~2 makes `bench.py` numbers move
  by multiples. The ladder's min-of-N is far more robust, but not immune.
- **Calibrate the CPU.** The load average does not show everything: in one
  overnight window it read under 2 while every probe ran eight times slower
  than usual (a resolved future under `gather` cost 3.9 us per item instead of
  0.42 us). Bracket a run with a fixed CPU-bound loop, `sum(range(10**7))` at
  about 0.085 s on this machine, and discard runs whose calibration moves.
- **Name the filesystem.** It is the single biggest confounder here.

## The filesystem trap

`tempfile.TemporaryDirectory()` defaults to `/tmp`, which on most distributions
is **tmpfs** — and tmpfs sets no `FMODE_NOWAIT`. Two consequences follow, and
both of them will silently mislead you:

1. `preadv2(RWF_NOWAIT)` fails with `EOPNOTSUPP`, so the page-cache fast path
   cannot engage at all.
2. io_uring cannot attempt a non-blocking issue, so it **punts every buffered
   read to an io-wq kernel worker** — confirmed by `uring.bt`, which counts one
   `io_uring_queue_async_work` per `io_uring_submit_req`.

The same 4 KiB read measures ~1.3 µs on ext4 and ~61 µs on tmpfs. Benchmarking
on tmpfs makes io_uring look 10x worse than it is and hides the fast path
entirely. Always pass `FS=` at a real filesystem, and report which one.

## What this harness found

Running the ladder on ext4 gave, per 4 KiB page-cache-hot read:

| rung | min |
| ---- | --- |
| `pread` (blocking syscall floor) | 1,079 ns |
| `future` (asyncio machinery) | 539 ns |
| `bridge` (thread hop, **zero I/O**) | 40,582 ns |
| `read` (turbofile, before) | 44,274 ns |

`bridge` accounted for **93%** of all overhead while doing no kernel work, so
the I/O mechanism was never the problem. `counters.sh` confirmed the mechanism:
`read` ran **4.08 context switches per op** against `pread`'s 0.0001, at
identical IPC — ruling out memory- and CPU-bound explanations the way the
original post ruled out cache thrashing.

`wake_probe.py` then established that this was not a turbofile bug: a bare
`threading.Event` round trip costs **34 µs** on this machine, because
`acpi_idle` exposes a C2 state with an **18 µs** exit latency and every
operation puts both threads to sleep. The bridge was already at its floor. The
only fix available was to stop crossing threads.

Hence `FastPath.read` / `.read_all` and `try_read_file`: serve reads from
resident pages on the event-loop thread, and fall back to the existing async
submission when — and only when — the read would block. On Linux that is
`preadv2(RWF_NOWAIT)`, and the kernel's `EAGAIN` is what makes it safe; nothing
on the fast path can stall the loop.

Result on ext4: sized reads went 44,274 ns → **1,303 ns (34x)**, whole-file
`read_bytes` 73,103 ns → **5,284 ns (13.8x)**, both within ~250 ns of the
blocking `pread` floor. tmpfs takes the fallback and is unchanged.

### macOS

The same ladder on an Apple Silicon Mac (macOS 26.4) shows the same picture:
`bridge` costs 34,030 ns against a 534 ns `pread` floor, so the thread hop is
91% of `read`. Darwin has no `RWF_NOWAIT`, but `mincore` on a `PROT_NONE`
mapping of the file reports which pages the unified buffer cache holds, and it
tracks residency exactly: a buffered write leaves every page resident, an
`F_NOCACHE` write none, one `pread` makes exactly that page resident, and pages
past EOF report absent. So `FastPath` keeps one such mapping per open file,
asks `mincore` about the pages a read touches, and copies with a plain `pread`
— never through the mapping, so a truncate elsewhere cannot fault the process.
The check and the copy are two syscalls, so a page evicted between them makes
that one `pread` wait for the disk: rare, bounded, never wrong.

Result on APFS: `file_read` went 37,958 ns → **1,124 ns (34x)**, about 530 ns
above the `pread` floor; the `mincore` call is most of that gap.

`read_bytes` stays on the async path on macOS. Darwin has no equivalent of
`openat2(RESOLVE_CACHED)`, and measuring `open()` on this machine showed why an
inline open is not acceptable: with Jamf Protect's Endpoint Security extension
active, the first open of a file costs ~300 µs at the median and over 400 µs at
p99, and even repeated opens of one file show 1–2 ms tails. Inline would cut
the median from 65 µs to 17 µs and put every one of those stalls on the event
loop instead of the driver thread.

`inline_read` needs an io_uring, so the ladder has that rung on Linux only.

### The doorbell

With the fast path serving hot reads, what remained of `bridge` was the wake
itself. `wake_probe.py` on the same Mac put a bare futex round trip at 7.7 µs
and a `call_soon_threadsafe` round trip at 28.7 µs, against a `bridge` rung of
31.4 µs: about 3 µs of the bridge was turbofile's, and the rest was CPython's
wake path (a socketpair send, a `kevent` return, a self-pipe read, then the
scheduled handle). A one-byte write to a pipe registered with `add_reader`
does the same wake in 18.9 µs on the stock loop and 8.0 µs under uvloop, the
futex floor. It also needs no GIL on the driver thread, where
`call_soon_threadsafe` could wait a whole switch interval behind a busy loop
and stall the reaping of every other completion.

Result: `bridge` 31,728 ns → 19,663 ns, `read_bytes` 64,922 ns → 53,998 ns
per hot 4 KiB file.

### Large reads

`read_parallel` filled an 8 MiB read as 4 × 2 MiB chunks. A chunk sweep
against Darwin's four kernel AIO threads (`kern.aiothreads`) showed the copy
throughput peaking with at least sixteen chunks in flight and 512 KiB the
best size from 8 MiB to 128 MiB: 8 MiB went 0.579 → 0.386 ms, 128 MiB
6.6 → 5.1 ms against 17.6 ms for a blocking `pread`. Fresh destination
pages matter too: a single 8 MiB `aio_read` into a never-touched buffer costs
1.09 ms against 0.78 ms into a resident one, which is the cross-map fault path
the kernel thread takes. The public read-all also paid two driver round trips
for its size snapshot and its end-of-file check; both are now an inline
`fstat`, the same call the fast path already makes.

`read_bytes` was the one large read still on a single `aio_read`: one
submission did open, a read-to-end into a `Vec`, and close, and the `Vec` was
then copied into `bytes` on delivery, 1.24 ms for 8 MiB against 0.98 ms for
an executor read of the same file. The op now hands back an open handle when
the file is above 1 MiB and the caller runs the same parallel fill into the
returned `bytes`, then closes: three round trips instead of one, but no copy
and sixteen chunks in flight. 8 MiB 1.24 → 0.46 ms; 64 MiB 2.67 ms against
8.5 ms for the executor read. Files at or under 1 MiB keep the single
submission.

### One round trip per read-to-end

Every read to end on the darwin driver cost two AIO round trips. The first
`aio_read` filled the buffer to the size `fstat` had reported; the chunk step
then saw a non-zero count on a read-to-end, grew the buffer (a realloc and a
copy of everything read so far) and submitted a second `aio_read` whose only
purpose was to return zero. That is the whole of `read_bytes` for files up to
1 MiB and of every cold `read()` that the fast path declined. The step now
asks `fstat` after each chunk and ends the op when a regular file's size is at
or below what has been read; anything that is not a regular file keeps reading
until the zero, since its `st_size` means nothing.

Result per hot 4 KiB file, same session, load average 4 to 6:

| rung | before | after |
| ---- | ------ | ----- |
| `read_bytes` min | 55,453 ns | 45,396 ns |
| `read_bytes` p50 | 56,184 ns | 46,255 ns |
| `read` (sized, one chunk already) | 33,441 ns | 33,052 ns |

The bench's 200-file storm went from 5.0x to 5.4x aiofiles for the same reason
(5.9x to 6.1x when that workload runs alone).

### Resident reads on the driver thread, measured and dropped

The remaining hop per `read_bytes` is the kernel AIO thread: `aio_read`, the
copy on that thread, and the `aio_suspend` wake. `mincore` on a one-shot
`PROT_NONE` mapping can tell the driver the pages are resident, in which case a
plain `pread` on the driver thread would do. Three interleaved rounds through
the public `read_bytes`, min / p50 in µs per call, plus the bench's 200-file
storm:

| variant | 4 KiB | 64 KiB | 128 KiB | storm p50 |
| ------- | ----- | ------ | ------- | --------- |
| `aio_read` (shipped) | 40.3 / 46.0 | 45.2 / 49.3 | 46.9 / 51.6 | 2.94 ms |
| `mincore` then `pread` | 39.7 / 43.8 | 42.8 / 47.7 | 47.7 / 51.2 | 3.66 ms |
| `pread` when the driver is idle, no check | 36.0 / 40.5 | 37.7 / 44.5 | 41.9 / 47.2 | 3.00 ms |

`mmap`, `mincore` and `munmap` cost 3 to 4 µs for one page and grow with the
page count, which is within a microsecond of the hop they replace; on the
storm the check runs 200 times on the one driver thread while the copies it
replaced ran on four kernel threads, hence the 25% loss. Skipping the check
when nothing else is pending at the driver does save 5 µs on a lone hot read,
but a lone cold read then blocks the driver for a disk read, and staggered
concurrent cold reads (a semaphore-bounded crawl over a cold directory) would
serialize on one thread instead of four. Neither variant shipped. The probe
scripts stayed out of the repo; the numbers are here so the experiment is not
repeated.

### The notice cliff

The darwin driver has two ways to sleep. With nothing in flight it blocks on
the submission channel and a new op wakes it at once. With ops in flight it
blocks in `aio_suspend`, which only a completion or its timeout can end; the
driver takes no completions through `kqueue` (XNU gained that route in macOS
26, measured further down), and `aio_suspend` watches no file descriptor, so
the channel cannot reach it. An op submitted while the only
things in flight are slow therefore waited for the fixed 1 ms timeout.

Measured with a hot 4 KiB read through the driver path (`_turbofile.read` on
an open handle, 35 µs idle), first beside a single 256 MiB `aio_read` that
holds the driver for about 35 ms, then beside a coroutine writing 1 MiB
chunks; p50 / p99 in µs, two interleaved rounds per row. The last column is
the process CPU added over one 256 MiB read with no other traffic, which is
the polling cost while a slow op is in flight and nothing completes.

| wait | lone slow op | beside writes | polling cost |
| ---- | ------------ | ------------- | ------------ |
| fixed 1 ms (before) | 1,136 / 1,200 | 103 / 170 to 750 | none |
| fixed 200 µs | 235 / 250 | 103 / 240 | none |
| fixed 50 µs | 65 / 86 to 307 | 63 / 105 | +2.5 to 3 ms of 35 (7 to 8% of a core) |
| fixed 20 µs | 33 / 70 to 86 | 35 / 69 | +1.7 to 6 ms (5 to 17%) |
| adaptive 20 µs, doubling to 1 ms | 33 / 350 to 520 | 35 / 80 | +0.3 to 1.8 ms |
| adaptive 20 µs, doubling to 320 µs | 35 / 365 | 35 / 76 | none measurable |
| adaptive 20 µs, doubling to 160 µs | 33 / 190 | 36 / 72 | none measurable |

The adaptive wait starts at 20 µs after any pass that reaped a completion or
handled a message and doubles while nothing moves, so traffic keeps it short
and a lull costs at most the cap. The 160 µs cap shipped: same p50 as the
fixed 20 µs wait everywhere, the same p99 beside writes, a longer tail only
for the first op after a lull, and no polling bill. The fixed 20 µs wait buys
that tail for 5 to 17% of a core whenever slow ops are in flight. A
`pthread_kill` from the submitter to a no-op handler on the driver thread
would end `aio_suspend` on the submission itself and remove the cliff
entirely; it needs an `EINTR` retry around every blocking syscall the driver
makes and a timeout kept as backstop for the window between the flag check
and entering `aio_suspend`. It was not prototyped and the adaptive wait keeps
a timeout anyway, so it can replace the timer later if the tail matters.

The shipped build, measured the same way: 35 / 130 to 180 beside the lone slow
op, 37 / 72 to 79 beside writes, idle 34 / 44, and the CPU over a 256 MiB read
within 0.6 ms of its wall time. The ladder's `bridge`, `read` and `read_bytes`
rungs and the 200-file storm are unchanged, since a completion ends the wait
before either timeout matters there.

### Opens off the driver thread

With one round trip per file the 200-file storm still took 2.8 ms, and a
split of that time put the driver thread on the critical path. Two hundred
serial `open` and `close` calls cost 1.9 ms on this machine, about 8 µs per
`open` with Jamf Protect's Endpoint Security extension authorizing each one
and 2 µs per `close`, while the event loop's 200 coroutines, submissions and
resumptions cost 0.9 ms and ran alongside. `openat` on a directory
descriptor, `O_NONBLOCK` and `O_CLOEXEC` change nothing, and a `stat` of the
same path is 1 µs, so the cost is the open itself, not the lookup. Spread over
threads it does scale: 200 opens took 1.05 ms on four Python threads against
1.95 ms on one, GIL included.

| part of the storm | ms |
| ----------------- | -- |
| 200 gathered coroutines, no I/O | 0.38 |
| 200 gathered `probe_nop` (the bridge alone) | 0.89 |
| 200 serial `open` + `close`, one thread | 1.95 |
| 200 serial `open` + `pread` + `close`, one thread | 2.33 |
| 200 gathered `read_bytes` | 2.96 |

Two ways to run opens on more threads were weighed. Several driver threads,
each with its own `aio_suspend` loop, would spread the opens with no extra
hop, but XNU sleeps every `aio_suspend` caller on one per-process channel and
wakes all of them on each completion (`wakeup`, not `wakeup_one`, in
`kern_aio.c`), so N loops pay N−1 spurious kernel wakes and a rescan under
the process's AIO lock per completion, exactly during a burst. They would
also have to share the 16-slot `kern.aioprocmax` cap, and the fallback that
runs an op synchronously when a submission returns EAGAIN with nothing in
flight would then put blocking I/O on a driver thread whenever another driver
held the slots. So the driver keeps its single loop, and a small pool of
helper threads runs only the `open` (and the `close` of a descriptor the
driver owns), only while the driver has a backlog: submissions waiting in its
channel, jobs waiting for an AIO slot, or opens already out at the helpers. A
lone op opens on the driver thread as before and pays no extra hop. The
descriptors come back as messages the driver takes in the same pass as its
completions, and every callback still runs on the driver thread; a helper
settles an op itself only when the driver has already gone away at shutdown.
A cancel for an op whose open is out at a helper is remembered and wins when
the open comes back. An open that never returns, on a path that hangs, keeps
one helper and the backlog flag for the driver's lifetime, so later opens all
take the helper hop; before, the driver itself was stuck.

The helper count, storm p50 in ms over three interleaved rounds (one helper
is the pipelined case: the driver submits, reaps and closes while the helper
opens):

| helpers | 1 | 2 | 3 | 4 | 5 | 6 |
| ------- | ---- | ---- | ---- | ---- | ---- | ---- |
| storm p50 | 2.60 | 1.67 | 1.39 | 1.51 | 1.84 | 2.10 |

Three won on this 6P+6E machine and six lost to two, since the driver, the
event loop and the four kernel AIO threads want cores as well. The count was
set to one helper per four hardware threads, at least two and at most four.

Re-measured three days later on a quiet machine, with a fixed CPU-bound
calibration loop steady before and after, as wheels in one venv per count,
three interleaved legs of 200 storms each:

| helpers | 3 | 4 | 5 | 6 |
| ------- | ---- | ---- | ---- | ---- |
| storm min | 1.33 to 1.35 ms | 1.17 to 1.20 ms | 1.21 to 1.30 ms | 1.35 to 1.53 ms |
| storm p50 | 1.41 to 1.49 ms | 1.25 to 1.26 ms | 1.41 ms | 1.71 to 1.72 ms |

Four beats three by a tenth on every leg, five gives it back and six loses to
three. The earlier 1.51 ms for four did not come back; the load average at
the time was not recorded, so the difference is unexplained. The count is now
one helper per three hardware threads, still at least two and at most four.
Per thread, CPU per storm with four helpers: 1.06 ms each in the kernel
(against 1.27 ms each with three), the driver 0.69 ms, the event loop
0.99 ms, the kernel's AIO workers 1.32 ms together; the storm's wall time
1.17 ms min, 1.25 ms p50.

Merged after the whole-file and positional futures (#14, #18), so the
bench's storm row was measured again on that master the next night at 01:40:
one-minute load 1.6 to 2.2, the calibration loop at 0.091 to 0.092 s. Thread
profile over 200 storms: wall 0.89 ms min and 1.05 ms p50, the four helpers
1.05 ms of kernel time each, the driver 0.75 ms, the event loop 0.71 ms, the
AIO workers 1.35 ms together. `make bench` three times put turbofile's storm
at 0.99, 1.09 and 1.01 ms p50 against 1.21 to 1.22 ms on the #14 build that
morning, a sixth less. aiofiles measured 14.6 to 14.9 ms in this window
against 17.0 ms in the morning's, so the row moves from 14.0x to 14.5x
(median of the three) by less than turbofile's own time did.

### The open path's floor

Why more helpers stop helping. Python probes on 200 hot 16 KiB files in one
directory, one process, quiet machine:

| syscalls per file, one thread | per file |
| ----------------------------- | -------- |
| `open` | 8.2 us |
| `close` | 1.5 us |
| `stat` by path | 1.5 us |
| `open` + `close`, kernel CPU | 10 us |

| threads doing `open` + `close` | wall per 200 files | kernel CPU per file |
| ------------------------------ | ------------------ | ------------------- |
| 1 | 2.00 ms | 10 us |
| 2 | 1.47 ms | 15 us |
| 3 | 1.17 ms | 17 us |
| 4 | 1.06 ms | 20 us |
| 6 | 1.14 ms | 27 us |

Spreading the files over four directories changes nothing, and leaving the
descriptors open instead of closing them scales no better, so the lock is in
`open` itself. Three processes each opening and closing their own 200 files at
the same time take 3.0 to 3.3 ms per round each, the same aggregate rate as
three threads in one process: the serialization is system-wide, in the
kernel's open path (the endpoint-security check is the likely holder), not
the process's descriptor table. By Amdahl about 37% of an open+close, 3.7 us,
runs one at a time machine-wide, which puts 200 opens at 0.75 ms however
they are spread; four threads reach 1.06 ms. turbofile's storm with four
helpers is 1.17 ms, so it sits a tenth above the pure open+close wall, and
nothing in the process can move the serialized part. What is left per file on
the driver side is one `fstat` on the helper (`open_sized`) and a second one
on the driver when the read-to-end completes (`ReadJob::complete`), then the
close.

Result, the previous build against this one as wheels in two venvs, three
interleaved rounds each:

| build | storm min | storm p50 | process CPU per storm |
| ----- | --------- | --------- | --------------------- |
| one driver thread (before) | 2.72 ms | 2.87 ms | 5.0 to 5.1 ms |
| helper opens (this change) | 1.31 ms | 1.40 ms | 6.8 ms |

The CPU that the storm costs the process goes up by a third while its wall
time halves: the same 200 opens and closes run on three threads instead of
one, and the kernel's open path contends where it used to run alone. A helper
that polled its queue for 20 µs before blocking, to save the sleep and wake
between jobs, took the CPU from 6.8 to 6.6 ms and the wall time nowhere, so
the helpers block at once.

In the full bench the storm went from 5.4x to 12.6x aiofiles (6.0x to 13.5x
when that workload runs alone). The ladder's `read_bytes` (41.4 µs min) and
`bridge` (24.3 µs) rungs are within the session's variation of the previous
build, and a read beside a slow op still takes 29 to 32 µs p50 and 158 to
188 µs p99. Of the 1.4 ms that remain, 0.9 ms is the event loop's 200
coroutines and the bridge.


### The task per gathered file

With opens off the driver thread, the storm's remaining time split across
threads that were all within reach of each other. Per-thread CPU per storm
over 200 storms, from mach `thread_info` on each thread of the process, on a
quiet machine (one-minute load average 2.1 to 2.5 as the run started and about
3 while it ran, with the CPU calibration loop steady before and after):

| thread | coroutines (before) | futures (this change) |
| ------ | ------------------- | --------------------- |
| event loop | 1.05 ms | 0.73 ms |
| driver | 0.76 ms | 0.80 ms |
| each of three open helpers | 1.27 ms, 1.23 ms of it in the kernel | 1.22 ms, 1.17 ms of it in the kernel |
| the kernel's AIO workers, together | 0.82 ms | 0.95 ms |
| storm wall time, min and p50 | 1.31 ms, 1.39 ms | 1.14 ms, 1.21 ms |

The loop's share is Python and asyncio. `asyncio.gather` wraps every
coroutine it is handed in a Task: an allocation, a `call_soon` for the first
step, the step, a wakeup and a second step when the awaited future settles,
then the Task's own done callbacks. A future handed to `gather` gets one done
callback. Per gathered item, min and p50 over 300 rounds of 200 items, on the
previous build:

| gathered item | min | p50 |
| ------------- | --- | --- |
| a coroutine that returns at once | 1.64 us | 1.76 us |
| a resolved future | 0.42 us | 0.43 us |
| a `probe_nop` future (the bridge round trip, no I/O) | 1.23 us | 1.29 us |
| a coroutine awaiting `probe_nop` | 2.79 us | 3.03 us |
| a `read_bytes` coroutine, hot 16 KiB file | 6.61 us | 7.01 us |

`read_bytes` and `write_bytes` were coroutines that awaited one future and
returned its value; the only work after the await was the large-file handoff.
Inside a running loop they now return the completion future itself. The
handoff moved into the drain: a `read_file` above `inline_max` calls a Python
continuation with the open handle, which starts an eagerly started task for
the parallel fill and settles the future from that task's outcome. Eager start
matters: a cancel thrown into a coroutine that has not started skips its
`finally`, and the first version leaked the descriptor in exactly that case.
The future also answers `send`, `throw` and `close`, which is what
`asyncio.create_task` needs to drive it like a coroutine, so wrapping
`read_bytes` in a task keeps working and a cancelled task still waits for the
kernel op to settle before it raises. Outside a loop both functions return a
coroutine, so `asyncio.run(turbofile.read_bytes(p))` works as before.

Two smaller cuts on the same path: the future class has `__slots__`, so no
per-future `__dict__` is allocated when the drain stores its op ids, and the
drain no longer asks `done()` before settling, since nothing but the drain
settles a kernel future. Together they take the `probe_nop` item from 1.23 to
1.09 us. The `read_bytes` item goes from 6.61 to 5.71 us, most of that from
the Task that is no longer created.

Result, the previous build against this one as wheels in two venvs, three
interleaved legs of 200 storms each, same quiet machine:

| build | storm min | storm p50 | loop CPU per storm |
| ----- | --------- | --------- | ------------------ |
| coroutines (before) | 1.35 to 1.43 ms | 1.44 to 1.55 ms | 1.09 to 1.16 ms |
| futures (this change) | 1.15 to 1.18 ms | 1.25 to 1.37 ms | 0.75 to 0.81 ms |

The loop's CPU per storm drops by a third and the wall time by a sixth. The
profile above says where the storm's floor is now: each open helper spends
1.22 ms of CPU per storm, nearly all of it in the kernel, against a wall time
of 1.14 ms, so the three helpers are busy for the whole storm and the loop and
the driver each have a third of it idle. What remains is the kernel's `open`
and `close` of 200 files spread over three threads, about 18 us per file with
the endpoint-security scan included; the next cut is there, not in Python.

### Positional reads as futures

`BinaryFile.read_at` and `readinto_at` were coroutines: check the page-cache
fast path, else await the bridge's read. Inside a running loop they now return
the completion future itself, under the same rule as `read_bytes`: pages the
fast path finds resident settle a `KernelFuture` at once, anything else is the
future of the submitted kernel read, and outside a loop both return a
coroutine. The position-based methods (`read`, `read1`, `peek`, `readline`,
`readinto`) stopped going through `read_at`; they ask the synchronous
`read_resident` and `readinto_resident` helpers first and submit the kernel
read themselves, so a hot `f.read(n)` pays neither a coroutine nor a future.

The trade is in the lone await. Min of nine windows on a quiet machine:

| step | cost |
| ---- | ---- |
| create a coroutine object and close it | 49 ns |
| `await` a coroutine that returns a value | 61 ns |
| `KernelFuture(loop=loop)` | 104 ns |
| `loop.create_future()` | 97 ns |
| `await` a settled `KernelFuture` | 186 ns |
| `await` a plain done `Future` | 177 ns |

A resident read through a coroutine costs the first two lines, about 0.11 us;
through a settled future the third and fifth, about 0.29 us. A lone
`await f.read_at(pos, n)` on a hot page therefore gains about 0.2 us, and the
ladder's `file_read` rung, which is exactly that call, went from 0.99 to 1.21
to 1.25 us against a `pread` floor of 0.48 to 0.50 us in the same session.

A gather gains the Task per read and, for resident pages, the driver round
trip. 32 positional 4 KiB reads of a hot 4 MiB file on one open `BinaryFile`,
min and p50 over 500 rounds (20,000 for the lone calls), the previous build
against this one as wheels in two venvs, three legs each:

| call | min | p50 |
| ---- | --- | --- |
| 32 gathered `read_at`, coroutines (before) | 114.3 to 114.8 us | 117.3 to 118.6 us |
| 32 gathered raw `_turbofile.read` (bridge submissions, no fast path) | 87.8 to 99.2 us | 115.6 to 117.3 us |
| 32 gathered `read_at`, futures (this change) | 47.2 to 47.4 us | 47.9 to 50.8 us |
| lone `read_at`, coroutine (before) | 0.96 us | 1.04 to 1.08 us |
| lone `read_at`, future (this change) | 1.17 us | 1.29 us |
| lone `seek` + `read`, both builds | 1.21 to 1.25 us | 1.33 to 1.42 us |

The gathered coroutines were slower than the raw bridge submissions: each of
the 32 paid a Task and a resident read on the loop thread, while the bridge
path left the loop idle and let the kernel's AIO threads copy in parallel. As
futures the same 32 resident reads finish in 47 us, and `seek` + `read` on
the same file is unchanged, which is the point of the synchronous helpers.

The bench's "32 concurrent 4 KiB random reads" row reads through
`turbofile.open` and `read_at` now, instead of the raw bridge call. With all
32 pages resident that row is served on the loop thread, gathered, rather
than by 32 driver submissions: 48.3 to 48.5 us against 116 to 119 us, 55x
against 23x over aiofiles. The mechanism behind the row changed, not only its
multiplier. The "4 KiB read on an open file" row goes through `read()` and
stays at 59x; the storm stays at 14.0x.

### kqueue completions and list submission, measured

XNU on macOS 26 accepts `SIGEV_KEVENT` in an aiocb's `aio_sigevent`, with the
kqueue descriptor in `sigev_signo`; the completion arrives as an `EVFILT_AIO`
event whose `ident` is the aiocb, `udata` the `sival_ptr`, and `ext[0]` and
`ext[1]` the errno and return value, so `kevent64` is needed to see them. The
kernel consumes the request when it delivers the event, and `aio_return`
afterwards fails with EINVAL. The note under the notice cliff above, that XNU delivers
no AIO completions through kqueue, was true of earlier releases and is not
true of this one. Whether it buys the driver anything, one thread, per op:

| path | min | p50 |
| ---- | --- | --- |
| lone op: `aio_read`, `aio_suspend`, `aio_error`, `aio_return` | 2.75 us | 4.58 us |
| lone op: `aio_read`, `kevent64` | 2.21 us | 2.83 us |
| 16 in flight: suspend, `aio_error` scan, `aio_return` | 0.90 us | 1.66 us |
| 16 in flight: `kevent64` | 1.38 us | 2.07 us |
| lone op seen through an outer kqueue watching the inner one | 2.67 us | 3.38 us |

A lone op saves under two microseconds; a batch loses, since every submission
also registers a knote under a global lock, and the storm's driver time is
batches. The driver keeps `aio_suspend`. The kevent path would still remove
the notice cliff, because the driver could wait for completions and a doorbell
in one call, and a kqueue can be watched by another kqueue, so completions
could reach the event loop's own selector; an outer wait with no timeout hung
once in that probe, so a design on it needs a bounded wait.

`lio_listio(LIO_NOWAIT)` submits up to 16 requests in one call. For 16 hot
4 KiB reads, min over 1000 rounds: submission 4.1 us against 9.6 us for
sixteen `aio_read` calls, but 19.9 us against 15.6 us from submission to the
last reap, so the batch runs on fewer kernel workers than sixteen separate
wakeups get it. With the process cap full the whole list fails with EAGAIN and
nothing is queued. Not used.
