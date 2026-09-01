<h1 align="center">turbofile</h1>

<p align="center">
  <b>Real async file I/O for Python.</b><br>
  A Rust core drives the best completion mechanism each OS has (io_uring on
  Linux, POSIX AIO on macOS) behind an aiofiles-compatible <code>asyncio</code> API.
</p>

<p align="center">
  <a href="https://github.com/4thel00z/turbofile/actions/workflows/ci.yaml"><img src="https://github.com/4thel00z/turbofile/actions/workflows/ci.yaml/badge.svg?branch=master" alt="CI"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.12%2B-blue" alt="Python 3.12+"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/core-rust-orange" alt="Rust core"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License: MIT OR Apache-2.0"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> ·
  <a href="#usage">Usage</a> ·
  <a href="#benchmarks">Benchmarks</a> ·
  <a href="#backends">Backends</a> ·
  <a href="#limitations">Limitations</a> ·
  <a href="#development">Development</a>
</p>

---

Existing Python async-file libraries dispatch every call to a thread pool.
turbofile submits the I/O to the kernel and completes your `await` when the
kernel says the data moved:

- **Zero-copy reads and writes.** The kernel fills the `bytes` object your
  `await f.read(n)` returns; writes pin your buffer and hand the kernel its
  pointer. No intermediate copies on the hot paths.
- **Completion batching.** A burst of completions costs one event-loop wakeup
  (one `call_soon_threadsafe` doorbell per burst, drained entirely in Rust).
- **Parallel large reads.** Read-all on a large file is split into chunks
  filled concurrently into one buffer.
- **Whole-file ops.** `read_bytes`/`write_bytes` do open+read/write+close as a
  single submission: one round trip per file.

## Installation

First release pending; until it's on PyPI, build from source:

```
git clone https://github.com/4thel00z/turbofile
cd turbofile
make develop   # uv + maturin, builds the extension into the venv
```

## Usage

```python
import turbofile

async def main() -> None:
    async with turbofile.open("data.bin", "rb") as f:
        payload = await f.read()

    async with turbofile.open("log.txt", "a", encoding="utf-8") as f:
        await f.write("one line\n")

    data = await turbofile.read_bytes("data.bin")   # whole file, one op
    text = await turbofile.read_text("notes.md")
```

`turbofile.open` mirrors `aiofiles.open`: binary and text modes, buffering,
`encoding`/`errors`/`newline` with full universal-newline semantics,
`seek`/`tell` (text cookies follow CPython's `_pyio` scheme), `readline`,
async iteration, `readinto`, `truncate`, `fsync` via `sync`. Migration is
`import turbofile as aiofiles` for the `open` API.

## Benchmarks

`make bench` compares against aiofiles on your machine. On an Apple-silicon
Mac (macOS 26.4, POSIX AIO backend, page-cache-hot files):

| workload                                   | vs aiofiles |
| ------------------------------------------ | ----------- |
| 4 KiB whole-file read (`read_bytes`)       | 2.5x        |
| 32 concurrent 4 KiB random reads           | 15x         |
| 200 small files read concurrently          | 3.6x        |
| 8 MiB whole-file read (`open` + `read`)    | 1.0x        |
| 8 MiB sequential write (1 MiB chunks)      | 1.0x        |

Large sequential transfers are memory-bandwidth-bound in the page cache, so
every implementation converges there; turbofile wins where per-op overhead and
concurrency dominate, which is what an asyncio application actually does.
io_uring numbers on Linux come from CI; run `make bench` there for your
hardware.

## Backends

| OS      | backend                | mechanism                                    |
| ------- | ---------------------- | -------------------------------------------- |
| Linux   | compio (fusion driver) | io_uring, automatic polling fallback under seccomp/old kernels |
| macOS   | darwin-aio             | POSIX AIO (`aio_read`/`aio_write`/`aio_fsync` in XNU) |
| macOS   | compio (opt-in)        | kqueue polling driver with thread dispatch   |

`TURBOFILE_BACKEND=compio` selects the compio driver on macOS (benchmarking,
or as an escape hatch). Windows (IOCP via compio) is planned.

## Limitations

- `opener=` and integer file descriptors are not supported.
- `read_bytes` on very large files pays one buffer copy; prefer
  `open(...).read()` for multi-megabyte files.
- Cancelling an `await` sends a kernel-level abort request (`aio_cancel` on
  macOS; the compio driver stops before the next chunk), best-effort by
  nature: an op the kernel finishes first settles with its result. Either way
  the `CancelledError` arrives only once the op has settled, so your buffer
  is never touched after the `await` raises.

## Development

```
make develop   # build the extension into the venv (uv + maturin)
make test      # cargo test + pytest
make lint      # clippy -D warnings
make bench     # benchmark against aiofiles
```

## License

MIT OR Apache-2.0
