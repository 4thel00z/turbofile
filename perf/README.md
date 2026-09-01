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

Hence `try_read` / `try_read_all` / `try_read_file`: serve reads from resident
pages with `preadv2(RWF_NOWAIT)` on the event-loop thread, and fall back to the
existing async submission when — and only when — the read would block. The
kernel's `EAGAIN` is what makes that safe; nothing on the fast path can stall
the loop.

Result on ext4: sized reads went 44,274 ns → **1,303 ns (34x)**, whole-file
`read_bytes` 73,103 ns → **5,284 ns (13.8x)**, both within ~250 ns of the
blocking `pread` floor. tmpfs takes the fallback and is unchanged.
