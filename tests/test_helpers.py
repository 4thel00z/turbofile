"""Whole-file convenience helpers (single-op fast paths)."""

import asyncio
import inspect
import os
from typing import Any

import pytest

import turbofile
from turbofile.binary import LARGE_READ


@pytest.mark.asyncio
async def test_bytes_roundtrip(tmp_path) -> None:
    path = tmp_path / "payload.bin"
    payload = bytes(range(256)) * 100
    assert await turbofile.write_bytes(path, payload) == len(payload)
    assert await turbofile.read_bytes(path) == payload


@pytest.mark.asyncio
async def test_text_roundtrip(tmp_path) -> None:
    path = tmp_path / "payload.txt"
    text = "grüße aus dem kernel\nzeile zwei\n"
    await turbofile.write_text(path, text)
    assert await turbofile.read_text(path) == text
    assert path.read_text(encoding="utf-8") == text


@pytest.mark.asyncio
async def test_text_helpers_take_encoding(tmp_path) -> None:
    path = tmp_path / "latin.txt"
    await turbofile.write_text(path, "café", encoding="latin-1")
    assert path.read_bytes() == b"caf\xe9"
    assert await turbofile.read_text(path, encoding="latin-1") == "café"


@pytest.mark.asyncio
async def test_read_bytes_missing_file(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        await turbofile.read_bytes(tmp_path / "absent.bin")


def open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


def counting_task_factory(created: list[Any]) -> Any:
    def factory(
        loop: asyncio.AbstractEventLoop, coro: Any, context: Any = None
    ) -> asyncio.Task[Any]:
        created.append(coro)
        return asyncio.Task(coro, loop=loop, context=context)

    return factory


@pytest.mark.asyncio
async def test_read_bytes_in_a_running_loop_is_the_completion_future(tmp_path) -> None:
    path = tmp_path / "direct.bin"
    payload = bytes(range(256)) * 16
    path.write_bytes(payload)
    pending = turbofile.read_bytes(path)
    assert asyncio.isfuture(pending)
    assert await pending == payload


@pytest.mark.asyncio
async def test_gathering_read_bytes_makes_no_task_per_file(tmp_path) -> None:
    payloads = [bytes([i]) * 4096 for i in range(20)]
    paths = []
    for i, payload in enumerate(payloads):
        p = tmp_path / f"many-{i}.bin"
        p.write_bytes(payload)
        paths.append(p)
    loop = asyncio.get_running_loop()
    created: list[Any] = []
    loop.set_task_factory(counting_task_factory(created))
    try:
        results = await asyncio.gather(*(turbofile.read_bytes(p) for p in paths))
    finally:
        loop.set_task_factory(None)
    assert results == payloads
    assert created == []


@pytest.mark.asyncio
async def test_read_bytes_can_be_wrapped_in_a_task(tmp_path) -> None:
    path = tmp_path / "task.bin"
    payload = b"driven by a task"
    path.write_bytes(payload)
    assert await asyncio.create_task(turbofile.read_bytes(path)) == payload
    with pytest.raises(FileNotFoundError):
        await asyncio.create_task(turbofile.read_bytes(tmp_path / "absent.bin"))


@pytest.mark.asyncio
async def test_cancelled_task_around_read_bytes_settles_cancelled(tmp_path) -> None:
    """A task cancelled while its read is in flight ends cancelled.

    Where the platform serves the resident file inline (Linux), the task is
    already done after its first step and there is nothing left to cancel."""
    path = tmp_path / "cancel.bin"
    payload = b"c" * 65536
    path.write_bytes(payload)
    task = asyncio.create_task(turbofile.read_bytes(path))
    await asyncio.sleep(0)
    if task.done():
        assert task.result() == payload
        return
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


def test_read_bytes_outside_a_loop_is_a_coroutine(tmp_path) -> None:
    path = tmp_path / "later.bin"
    payload = b"awaited later"
    path.write_bytes(payload)
    pending = turbofile.read_bytes(path)
    assert inspect.iscoroutine(pending)
    assert asyncio.run(pending) == payload


def test_write_bytes_outside_a_loop_is_a_coroutine(tmp_path) -> None:
    path = tmp_path / "later-write.bin"
    payload = b"written later"
    pending = turbofile.write_bytes(path, payload)
    assert inspect.iscoroutine(pending)
    assert asyncio.run(pending) == len(payload)
    assert path.read_bytes() == payload


@pytest.mark.asyncio
async def test_write_bytes_in_a_running_loop_is_the_completion_future(tmp_path) -> None:
    path = tmp_path / "direct-write.bin"
    payload = b"written through the future"
    pending = turbofile.write_bytes(path, payload)
    assert asyncio.isfuture(pending)
    assert await pending == len(payload)
    assert path.read_bytes() == payload
    assert await asyncio.create_task(turbofile.write_bytes(path, b"again")) == 5
    assert path.read_bytes() == b"again"


@pytest.mark.asyncio
async def test_large_read_bytes_through_gather_and_a_task(tmp_path) -> None:
    path = tmp_path / "large.bin"
    payload = os.urandom(2 * LARGE_READ + 12345)
    path.write_bytes(payload)
    first, second = await asyncio.gather(turbofile.read_bytes(path), turbofile.read_bytes(path))
    assert first == payload
    assert second == payload
    assert await asyncio.create_task(turbofile.read_bytes(path)) == payload


@pytest.mark.asyncio
async def test_cancelling_a_large_read_bytes_closes_the_file(tmp_path) -> None:
    """A cancel during the parallel fill settles cancelled and closes the handle.

    Where the platform serves the resident file inline (Linux), the future is
    already done and there is no handoff to cancel."""
    small = tmp_path / "small.bin"
    small.write_bytes(b"s")
    await turbofile.read_bytes(small)
    path = tmp_path / "huge.bin"
    payload = b"z" * (32 * 1024 * 1024)
    path.write_bytes(payload)
    before = open_descriptors()
    pending = turbofile.read_bytes(path)
    if pending.done():
        assert pending.result() == payload
        return
    for _ in range(10000):
        if pending.done():
            pytest.fail("the large read finished before its handoff was visible")
        if getattr(pending, "continuation", None) is not None:
            break
        await asyncio.sleep(0)
    assert pending.cancel() is False
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert pending.cancelled()
    assert open_descriptors() == before


@pytest.mark.asyncio
async def test_task_cancelled_before_its_first_step_ends_cancelled(tmp_path) -> None:
    path = tmp_path / "early-cancel.bin"
    path.write_bytes(b"e" * 4096)
    task = asyncio.create_task(turbofile.read_bytes(path))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


@pytest.mark.asyncio
async def test_a_settled_result_can_still_be_wrapped_in_a_task() -> None:
    loop = asyncio.get_running_loop()
    done = turbofile.settled(loop, b"served inline")
    assert asyncio.isfuture(done)
    assert await asyncio.create_task(done) == b"served inline"
    assert await asyncio.gather(turbofile.settled(loop, 1), turbofile.settled(loop, 2)) == [1, 2]
