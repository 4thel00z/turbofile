"""Tests for the low-level _turbofile bridge."""

import asyncio

import pytest

from turbofile import _turbofile


@pytest.mark.asyncio
async def test_open_write_read_close_roundtrip(tmp_path) -> None:
    path = str(tmp_path / "bridge.bin")
    handle, size, fd = await _turbofile.open(
        path, False, True, False, True, True, False
    )
    assert size == 0
    assert fd > 0

    payload = b"bridge speaks kernel completions"
    n, end = await _turbofile.write(handle, 0, payload, False)
    assert n == len(payload)
    assert end == len(payload)
    await _turbofile.close(handle)

    handle, size, fd = await _turbofile.open(
        path, True, False, False, False, False, False
    )
    assert size == len(payload)
    data = await _turbofile.read(handle, 0, len(payload))
    assert data == payload
    short = await _turbofile.read(handle, 7, 1000)
    assert short == payload[7:]
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_concurrent_reads_share_one_loop(tmp_path) -> None:
    path = str(tmp_path / "burst.bin")
    payload = bytes(range(256)) * 64
    await _turbofile.write_file(path, payload)

    handle, size, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )
    assert size == len(payload)
    chunks = await asyncio.gather(
        *(_turbofile.read(handle, i * 256, 256) for i in range(64))
    )
    for i, chunk in enumerate(chunks):
        assert chunk == payload[i * 256 : (i + 1) * 256]
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_missing_file_raises_file_not_found(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        await _turbofile.open(
            str(tmp_path / "absent.bin"), True, False, False, False, False, False
        )


@pytest.mark.asyncio
async def test_read_file_and_write_file(tmp_path) -> None:
    path = str(tmp_path / "whole.bin")
    payload = b"\x00\x01turbo\xfffile"
    n, end = await _turbofile.write_file(path, payload)
    assert (n, end) == (len(payload), len(payload))
    assert await _turbofile.read_file(path, 1 << 40) == payload


@pytest.mark.asyncio
async def test_readinto_fills_caller_buffer(tmp_path) -> None:
    path = str(tmp_path / "into.bin")
    await _turbofile.write_file(path, b"0123456789")
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )
    sink = bytearray(4)
    n = await _turbofile.readinto(handle, 2, sink)
    assert n == 4
    assert bytes(sink) == b"2345"
    await _turbofile.close(handle)


class LoopWithoutThreadsafeCalls(asyncio.SelectorEventLoop):
    """A real selector loop whose call_soon_threadsafe is unusable.

    Completions must still arrive: the doorbell may not depend on it, since
    that path needs the GIL on the driver thread.
    """

    def call_soon_threadsafe(self, callback, *args, context=None):  # type: ignore[override]
        raise AssertionError("doorbell used call_soon_threadsafe")


def run_roundtrip_on(loop: asyncio.AbstractEventLoop, path: str) -> bytes:
    """write_file then read_file on `loop`, failing (not hanging) if a completion is lost.

    A lost completion leaves its future unsettled forever, so the inner task is
    shielded and the loop is closed without cancelling it.
    """

    async def roundtrip() -> bytes:
        await _turbofile.write_file(path, b"rung through the doorbell")
        return await _turbofile.read_file(path, 1 << 40)

    async def guarded() -> bytes:
        return await asyncio.wait_for(asyncio.shield(roundtrip()), 5)

    try:
        return loop.run_until_complete(guarded())
    finally:
        loop.close()


def test_completions_arrive_without_call_soon_threadsafe(tmp_path) -> None:
    path = str(tmp_path / "doorbell.bin")
    data = run_roundtrip_on(LoopWithoutThreadsafeCalls(), path)
    assert data == b"rung through the doorbell"


class LoopWithoutReaders(asyncio.SelectorEventLoop):
    """A loop that refuses `add_reader`, as a proactor loop does."""

    def add_reader(self, fd, callback, *args):  # type: ignore[override]
        raise NotImplementedError


def test_loop_without_add_reader_completes_through_call_soon_threadsafe(tmp_path) -> None:
    path = str(tmp_path / "fallback.bin")
    data = run_roundtrip_on(LoopWithoutReaders(), path)
    assert data == b"rung through the doorbell"


def test_backend_name_reports_the_live_driver() -> None:
    import os
    import sys

    name = _turbofile.backend_name()
    override = os.environ.get("TURBOFILE_BACKEND")
    if override in ("aio", "darwin-aio"):
        assert name == "darwin-aio"
    elif override == "compio" and sys.platform == "darwin":
        assert name == "compio-polling"
    elif sys.platform == "darwin":
        assert name == "darwin-aio"
    else:
        assert name in ("compio-io-uring", "compio-polling")


@pytest.mark.asyncio
async def test_read_parallel_fills_one_buffer(tmp_path) -> None:
    path = str(tmp_path / "parallel.bin")
    payload = bytes(range(256)) * (32 * 1024)  # 8 MiB
    await _turbofile.write_file(path, payload)
    handle, size, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )
    assert size == len(payload)
    data = await _turbofile.read_parallel(handle, 0, len(payload), 1024 * 1024)
    assert data == payload
    tail = await _turbofile.read_parallel(handle, len(payload) - 100, 1000, 256)
    assert tail == payload[-100:]
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_read_file_hands_off_a_handle_above_inline_max(tmp_path) -> None:
    path = str(tmp_path / "handoff.bin")
    payload = bytes(range(256)) * 64
    await _turbofile.write_file(path, payload)

    assert await _turbofile.read_file(path, len(payload)) == payload

    handle, size, fd = await _turbofile.read_file(path, len(payload) - 1)
    assert size == len(payload)
    assert fd > 0
    assert await _turbofile.read_parallel(handle, 0, size, 4096) == payload
    await _turbofile.close(handle)
