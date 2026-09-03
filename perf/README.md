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
