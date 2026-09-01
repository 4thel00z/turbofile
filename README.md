# turbofile

Real async file I/O for Python. A Rust core drives the best completion
mechanism each OS has — io_uring on Linux, POSIX AIO on macOS — behind an
aiofiles-compatible `asyncio` API.

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
  single submission — one round trip per file.

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

On Linux, reads whose pages are already resident are served inline on the
event-loop thread with `preadv2(RWF_NOWAIT)` — no submission, no driver-thread
hop, no completion wakeup — falling back to async submission when the kernel
says the read would block. Page-cache-hot 4 KiB reads on ext4:

| operation                | before  | after   |         |
| ------------------------ | ------- | ------- | ------- |
| `open` + `read(n)`       | 44.3 us | 1.30 us | **34x** |
| `read_bytes`             | 73.1 us | 5.28 us | **14x** |
| blocking `pread` (floor) | 1.08 us |         |         |

Benchmark on a real filesystem: tmpfs sets no `FMODE_NOWAIT`, which disables
the fast path and makes io_uring punt every read to a kernel worker, so `/tmp`
numbers are not comparable. `make bench` and `make ladder` take `FS=<dir>` and
print which filesystem they measured. See `perf/README.md` for the analysis
harness.

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
- Cancelling an `await` detaches the future; the kernel op still completes
  (and, for `readinto`, may still write into the buffer) — standard
  completion-model semantics.

## Development

```
make develop   # build the extension into the venv (uv + maturin)
make test      # cargo test + pytest
make lint      # clippy -D warnings
make bench     # benchmark against aiofiles
```

## License

MIT OR Apache-2.0
