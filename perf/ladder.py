#!/usr/bin/env python
"""The turbofile latency ladder: calibration probes with known ground truth.

Each rung adds exactly one layer to the rung below it, so the *difference*
between two adjacent rungs is that layer's cost. This is what makes a number
attributable instead of merely suggestive: `bridge - future` is the submit
channel plus the driver-thread hop plus the completion doorbell, and nothing
else, because those two rungs are identical in every other respect.

Rungs, innermost first:

    py_coro     await a coroutine returning a constant   -- asyncio floor
    ffi         + a pyo3 call that returns immediately   -- FFI floor
    future      + create/resolve/await an asyncio future -- future machinery
    bridge      + submit -> driver thread -> doorbell    -- THE BRIDGE
    pread       a blocking os.pread of the same bytes    -- kernel floor
    read        the real turbofile positional read       -- bridge + io_uring
    read_bytes  open+read+close as one turbofile op      -- whole-file path

`pread` is deliberately not in the additive chain: it is the *reference floor*,
the time the same bytes cost with no async machinery at all. `read / pread` is
the overhead multiple, and it is the number that decides whether a 10x is
available above the kernel or has to come out of the kernel path.

Measurement discipline (see the Apple-silicon post this is modelled on):
frequency scaling makes the first samples useless, so every rung is warmed
before it is timed; absolute times drift with boost state, so `min` is
reported alongside `p50` and the *ratios* are what get interpreted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable

import turbofile
from turbofile import _turbofile

NS = 1e9
SIZE = 4096


def fstype(path: str) -> str:
    """Filesystem backing `path`, from the longest matching mount point."""
    best, name = "", "?"
    for line in open("/proc/self/mounts"):
        parts = line.split()
        if path.startswith(parts[1]) and len(parts[1]) >= len(best):
            best, name = parts[1], parts[2]
    return name


class Fixture:
    """A page-cache-hot 4 KiB file, open on both a raw fd and a turbofile handle."""

    def __init__(self, dirpath: str) -> None:
        self.path = os.path.join(dirpath, "ladder.bin")
        with open(self.path, "wb") as f:
            f.write(os.urandom(SIZE))
        self.fd = os.open(self.path, os.O_RDONLY)
        self.handle: int | None = None
        self.bf = None

    async def start(self) -> None:
        self.handle, _, _ = await _turbofile.open(
            self.path, True, False, False, False, False, False
        )
        # The public object, so the ladder can measure the API users call.
        self.bf = await turbofile.open(self.path, "rb")
        # Fault the page in so no rung pays a major fault.
        for _ in range(64):
            os.pread(self.fd, SIZE, 0)

    async def stop(self) -> None:
        if self.bf is not None:
            await self.bf.close()
        if self.handle is not None:
            await _turbofile.close(self.handle)
        os.close(self.fd)


def build_rungs(fx: Fixture) -> dict[str, Callable[[], Awaitable[object]]]:
    async def py_coro() -> object:
        return 0

    async def ffi() -> object:
        return _turbofile.probe_ffi()

    async def future() -> object:
        return await _turbofile.probe_resolved_future()

    async def bridge() -> object:
        return await _turbofile.probe_nop()

    async def pread() -> object:
        return os.pread(fx.fd, SIZE, 0)

    async def read() -> object:
        return await _turbofile.read(fx.handle, 0, SIZE)

    async def read_bytes() -> object:
        return await turbofile.read_bytes(fx.path)

    async def try_read() -> object:
        # The page-cache fast path on its own: no future, no hop.
        return _turbofile.try_read(fx.fd, 0, SIZE)

    async def file_read() -> object:
        # What an application actually calls, through BinaryFile.
        return await fx.bf.read_at(0, SIZE)

    async def inline_read() -> object:
        # Proposed design's floor: submit + reap on the loop thread, one
        # io_uring_enter, no channel and no doorbell.
        return _turbofile.probe_inline_read(fx.fd, SIZE)

    return {
        "py_coro": py_coro,
        "ffi": ffi,
        "future": future,
        "bridge": bridge,
        "pread": pread,
        "read": read,
        "read_bytes": read_bytes,
        "inline_read": inline_read,
        "try_read": try_read,
        "file_read": file_read,
    }


# Rungs that form the additive chain; `pread` is a reference floor, not a layer.
CHAIN = ["py_coro", "ffi", "future", "bridge", "read"]
LAYER_OF = {
    "ffi": "pyo3 call",
    "future": "asyncio future",
    "bridge": "submit + thread hop + doorbell",
    "read": "io_uring read",
}


async def measure(
    op: Callable[[], Awaitable[object]], batch: int, reps: int, depth: int
) -> dict[str, float]:
    """Time `batch` ops per window, `reps` windows; return per-op nanoseconds.

    At depth > 1 the batch is issued as one `gather`, which is how an asyncio
    application actually drives this library and the only regime where the
    doorbell's amortisation shows up.
    """
    if depth > 1:
        async def window() -> None:
            for _ in range(batch // depth):
                await asyncio.gather(*(op() for _ in range(depth)))
        per_window = (batch // depth) * depth
    else:
        async def window() -> None:
            for _ in range(batch):
                await op()
        per_window = batch

    for _ in range(max(2, reps // 4)):  # warm-up: boost clocks, JIT-free but caches warm
        await window()

    samples = []
    for _ in range(reps):
        start = time.perf_counter_ns()
        await window()
        samples.append((time.perf_counter_ns() - start) / per_window)
    return {
        "min": min(samples),
        "p50": statistics.median(samples),
        "mean": statistics.fmean(samples),
    }


async def run(args: argparse.Namespace) -> int:
    if args.pin:
        cores = {int(c) for c in args.pin.split(",")}
        os.sched_setaffinity(0, cores)

    with tempfile.TemporaryDirectory(dir=args.dir) as dirpath:
        FS[0] = fstype(dirpath)
        fx = Fixture(dirpath)
        await fx.start()
        rungs = build_rungs(fx)

        wanted = [args.only] if args.only else list(rungs)
        for name in wanted:
            if name not in rungs:
                print(f"unknown rung: {name}", file=sys.stderr)
                return 2

        if args.only and args.seconds:
            # Steady-state mode: loop one rung for a fixed duration so an
            # external profiler (perf stat / perf record / bpftrace) has a
            # clean, single-rung window to attribute.
            op = rungs[args.only]
            deadline = time.perf_counter() + args.seconds
            count = 0
            while time.perf_counter() < deadline:
                if args.depth > 1:
                    await asyncio.gather(*(op() for _ in range(args.depth)))
                    count += args.depth
                else:
                    await op()
                    count += 1
            print(f"{args.only}: {count} ops in {args.seconds}s", file=sys.stderr)
            await fx.stop()
            return 0

        results = {}
        for name in wanted:
            results[name] = await measure(rungs[name], args.batch, args.reps, args.depth)

        await fx.stop()

    if args.json:
        print(json.dumps({"depth": args.depth, "rungs": results}, indent=2))
        return 0

    emit(results, args.depth)
    return 0


FS = ["?"]


def emit(results: dict[str, dict[str, float]], depth: int) -> None:
    print(
        f"backend: {_turbofile.backend_name()}   python: {sys.version.split()[0]}   "
        f"depth: {depth}   fs: {FS[0]}\n"
    )
    print(f"  {'rung':<12} {'min ns':>10} {'p50 ns':>10}   layer added")
    print(f"  {'-' * 12} {'-' * 10} {'-' * 10}   {'-' * 34}")
    prev = None
    for name, r in results.items():
        delta = ""
        if name in LAYER_OF and prev is not None:
            delta = f"+{r['min'] - prev:>9,.0f} ns  {LAYER_OF[name]}"
        elif name == "pread":
            delta = "(reference floor, not a layer)"
        elif name == "read_bytes":
            delta = "(open+read+close, not a layer)"
        elif name == "inline_read":
            delta = "(io_uring inline floor, not a layer)"
        elif name == "try_read":
            delta = "(page-cache fast path, not a layer)"
        elif name == "file_read":
            delta = "(public read path, not a layer)"
        print(f"  {name:<12} {r['min']:>10,.0f} {r['p50']:>10,.0f}   {delta}")
        if name in CHAIN:
            prev = r["min"]

    if "read" in results and "pread" in results:
        read, pread = results["read"]["min"], results["pread"]["min"]
        print(f"\n  overhead multiple   read / pread = {read / pread:.1f}x")
        print(f"  absolute overhead   read - pread = {read - pread:,.0f} ns")
    if "bridge" in results and "future" in results:
        cost = results["bridge"]["min"] - results["future"]["min"]
        print(f"  bridge cost         bridge - future = {cost:,.0f} ns")
        if "read" in results:
            share = cost / (results["read"]["min"] - results["pread"]["min"])
            print(f"  bridge share of overhead = {share * 100:.0f}%")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--batch", type=int, default=2000, help="ops per timing window")
    p.add_argument("--reps", type=int, default=20, help="timing windows per rung")
    p.add_argument("--depth", type=int, default=1, help="concurrency per gather")
    p.add_argument("--only", help="run a single rung by name")
    p.add_argument("--seconds", type=float, help="with --only: loop for N s (profiler mode)")
    p.add_argument("--pin", help="comma-separated cores to pin to, e.g. 2,4")
    p.add_argument(
        "--dir",
        help="directory for the fixture file. THIS MATTERS: tmpfs does not set "
        "FMODE_NOWAIT, so io_uring must punt every buffered read to an io-wq "
        "worker there. Defaults to the system temp dir, which is usually tmpfs.",
    )
    p.add_argument("--json", action="store_true")
    return asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
