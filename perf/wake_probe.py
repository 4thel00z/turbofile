#!/usr/bin/env python
"""Ground-truth probes for cross-thread wake cost on this machine.

The ladder says the turbofile bridge costs ~44 us per op while doing no kernel
work. That is either (a) what a thread hop costs on this hardware, in which
case the fix is to stop hopping, or (b) something specific to turbofile's path,
in which case the fix is local. These probes decide which, by measuring the
same handoff with progressively less machinery:

    futex_pingpong    two threads bouncing a threading.Event -- raw wake cost
    queue_pingpong    the same over a queue.Queue            -- + queue
    call_soon_ts      worker -> loop.call_soon_threadsafe    -- the doorbell
                                                               turbofile uses

If `call_soon_ts` lands near `futex_pingpong`, turbofile's bridge is simply
paying what a wake costs here and no amount of local tuning helps; the only
fix is to stop crossing threads.
"""

from __future__ import annotations

import asyncio
import queue
import statistics
import threading
import time

REPS = 4000


def futex_pingpong() -> float:
    a, b = threading.Event(), threading.Event()
    stop = threading.Event()

    def worker() -> None:
        while not stop.is_set():
            if a.wait(0.5):
                a.clear()
                b.set()

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    for _ in range(200):  # warm
        a.set(); b.wait(); b.clear()
    samples = []
    for _ in range(REPS):
        s = time.perf_counter_ns()
        a.set(); b.wait(); b.clear()
        samples.append(time.perf_counter_ns() - s)
    stop.set(); a.set(); t.join(timeout=1)
    return statistics.median(samples)


def queue_pingpong() -> float:
    qa: queue.Queue = queue.Queue()
    qb: queue.Queue = queue.Queue()

    def worker() -> None:
        while True:
            if qa.get() is None:
                return
            qb.put(1)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    for _ in range(200):
        qa.put(1); qb.get()
    samples = []
    for _ in range(REPS):
        s = time.perf_counter_ns()
        qa.put(1); qb.get()
        samples.append(time.perf_counter_ns() - s)
    qa.put(None); t.join(timeout=1)
    return statistics.median(samples)


async def call_soon_ts() -> float:
    """Exactly turbofile's doorbell: a worker thread resolves a loop future."""
    loop = asyncio.get_running_loop()
    work: queue.Queue = queue.Queue()

    def worker() -> None:
        while True:
            item = work.get()
            if item is None:
                return
            fut = item
            loop.call_soon_threadsafe(lambda f=fut: f.done() or f.set_result(None))

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    async def one() -> None:
        fut = loop.create_future()
        work.put(fut)
        await fut

    for _ in range(200):
        await one()
    samples = []
    for _ in range(REPS):
        s = time.perf_counter_ns()
        await one()
        samples.append(time.perf_counter_ns() - s)
    work.put(None)
    return statistics.median(samples)


async def main() -> None:
    print(f"  {'probe':<20} {'p50 ns':>10}")
    print(f"  {'-' * 20} {'-' * 10}")
    print(f"  {'futex_pingpong':<20} {futex_pingpong():>10,.0f}")
    print(f"  {'queue_pingpong':<20} {queue_pingpong():>10,.0f}")
    print(f"  {'call_soon_ts':<20} {await call_soon_ts():>10,.0f}")


if __name__ == "__main__":
    asyncio.run(main())
