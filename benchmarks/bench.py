"""Benchmark turbofile against aiofiles and the sync baseline.

Run with: uv run python benchmarks/bench.py [--quick]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable

import aiofiles

import turbofile
from turbofile import _turbofile

KIB = 1024
MIB = 1024 * 1024


async def timed(runs: int, op: Callable[[], Awaitable[None]]) -> list[float]:
    for _ in range(max(1, runs // 10)):
        await op()
    samples = []
    for _ in range(runs):
        start = time.perf_counter()
        await op()
        samples.append(time.perf_counter() - start)
    return samples


def report(name: str, samples: list[float], per_run_bytes: int, baseline: float | None) -> float:
    mean = statistics.mean(samples)
    p50 = statistics.median(samples)
    mbps = per_run_bytes / mean / MIB if per_run_bytes else 0.0
    speedup = f"  {baseline / mean:5.2f}x vs aiofiles" if baseline else ""
    line = f"  {name:<28} {p50 * 1e3:9.3f} ms p50 {mean * 1e3:9.3f} ms mean"
    if per_run_bytes:
        line += f" {mbps:9.1f} MiB/s"
    print(line + speedup)
    return mean


async def bench_whole_file(dirpath: str, size: int, runs: int) -> None:
    path = os.path.join(dirpath, f"whole-{size}.bin")
    payload = os.urandom(size)
    with open(path, "wb") as f:
        f.write(payload)
    label = f"{size // KIB} KiB" if size < MIB else f"{size // MIB} MiB"
    print(f"whole-file read, {label}, {runs} runs")

    async def with_aiofiles() -> None:
        async with aiofiles.open(path, "rb") as f:
            await f.read()

    async def with_turbofile() -> None:
        await turbofile.read_bytes(path)

    async def with_turbofile_open() -> None:
        async with turbofile.open(path, "rb") as f:
            await f.read()

    def sync_read() -> None:
        with open(path, "rb") as f:
            f.read()

    async def with_sync() -> None:
        sync_read()

    base = report("aiofiles", await timed(runs, with_aiofiles), size, None)
    report("turbofile.read_bytes", await timed(runs, with_turbofile), size, base)
    report("turbofile.open+read", await timed(runs, with_turbofile_open), size, base)
    report("sync baseline (blocking)", await timed(runs, with_sync), size, base)
    print()


async def bench_random_reads(dirpath: str, concurrency: int, runs: int) -> None:
    size = 64 * MIB
    chunk = 4 * KIB
    path = os.path.join(dirpath, "random.bin")
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    offsets = [(i * 7919 * chunk) % (size - chunk) for i in range(concurrency)]
    print(f"random 4 KiB reads, {concurrency} concurrent, 64 MiB file, {runs} runs")

    async def with_aiofiles() -> None:
        async with aiofiles.open(path, "rb") as f:
            async def one(off: int) -> None:
                await f.seek(off)
                await f.read(chunk)
            for off in offsets:
                await one(off)

    handle, _, _ = await _turbofile.open(path, True, False, False, False, False, False)

    async def with_turbofile() -> None:
        await asyncio.gather(
            *(_turbofile.read(handle, off, chunk) for off in offsets)
        )

    per_run = chunk * concurrency
    base = report("aiofiles (sequential)", await timed(runs, with_aiofiles), per_run, None)
    report("turbofile (gathered)", await timed(runs, with_turbofile), per_run, base)
    await _turbofile.close(handle)
    print()


async def bench_write(dirpath: str, runs: int) -> None:
    total = 8 * MIB
    chunk = MIB
    payload = os.urandom(chunk)
    print(f"sequential write, {total // MIB} MiB in 1 MiB chunks, {runs} runs")

    async def with_aiofiles() -> None:
        async with aiofiles.open(os.path.join(dirpath, "w-a.bin"), "wb") as f:
            for _ in range(total // chunk):
                await f.write(payload)

    async def with_turbofile() -> None:
        async with turbofile.open(os.path.join(dirpath, "w-t.bin"), "wb") as f:
            for _ in range(total // chunk):
                await f.write(payload)

    base = report("aiofiles", await timed(runs, with_aiofiles), total, None)
    report("turbofile", await timed(runs, with_turbofile), total, base)
    print()


async def bench_small_file_storm(dirpath: str, count: int, runs: int) -> None:
    size = 16 * KIB
    paths = []
    for i in range(count):
        p = os.path.join(dirpath, f"small-{i}.bin")
        with open(p, "wb") as f:
            f.write(os.urandom(size))
        paths.append(p)
    print(f"small-file storm: read {count} files of 16 KiB concurrently, {runs} runs")

    async def read_one_aiofiles(p: str) -> None:
        async with aiofiles.open(p, "rb") as f:
            await f.read()

    async def with_aiofiles() -> None:
        await asyncio.gather(*(read_one_aiofiles(p) for p in paths))

    async def with_turbofile() -> None:
        await asyncio.gather(*(turbofile.read_bytes(p) for p in paths))

    per_run = size * count
    base = report("aiofiles", await timed(runs, with_aiofiles), per_run, None)
    report("turbofile.read_bytes", await timed(runs, with_turbofile), per_run, base)
    print()


def fstype(path: str) -> str:
    """Filesystem backing `path`, from the longest matching mount point.

    Only Linux is asked, via /proc/self/mounts; elsewhere this reports "?".
    The label exists to warn about tmpfs, which is a Linux concern, and it is
    never worth failing a benchmark over a cosmetic string.
    """
    try:
        with open("/proc/self/mounts") as mounts:
            best, name = "", "?"
            for line in mounts:
                parts = line.split()
                if len(parts) < 3:
                    continue
                if path.startswith(parts[1]) and len(parts[1]) >= len(best):
                    best, name = parts[1], parts[2]
            return name
    except OSError:
        return "?"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--dir",
        help="where to put fixture files. This matters: tmpfs sets no "
        "FMODE_NOWAIT, so the page-cache fast path is unavailable there and "
        "io_uring must punt every buffered read to a kernel worker. Defaults "
        "to the system temp dir, which is usually tmpfs.",
    )
    args = parser.parse_args()
    runs = 20 if args.quick else 100

    with tempfile.TemporaryDirectory(dir=args.dir) as dirpath:
        print(
            f"turbofile backend: {_turbofile.backend_name()}  "
            f"python: {sys.version.split()[0]}  platform: {sys.platform}  "
            f"fs: {fstype(dirpath)}\n"
        )
        await bench_whole_file(dirpath, 4 * KIB, runs * 2)
        await bench_whole_file(dirpath, 8 * MIB, max(10, runs // 2))
        await bench_random_reads(dirpath, 32, runs)
        await bench_write(dirpath, max(10, runs // 2))
        await bench_small_file_storm(dirpath, 200, max(10, runs // 4))


if __name__ == "__main__":
    asyncio.run(main())
