"""Doorbell and cancellation behavior of the _turbofile bridge."""

import asyncio
import os
import subprocess
import sys

import pytest

from turbofile import _turbofile


@pytest.mark.asyncio
async def test_doorbell_never_loses_a_wakeup(tmp_path) -> None:
    path = str(tmp_path / "hammer.bin")
    payload = bytes(range(256)) * 16
    await _turbofile.write_file(path, payload)
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )

    async def reader(worker: int) -> None:
        for i in range(50):
            pos = ((worker * 31 + i * 7) % 16) * 256
            chunk = await _turbofile.read(handle, pos, 256)
            assert chunk == payload[pos : pos + 256]

    async with asyncio.timeout(30):
        await asyncio.gather(*(reader(w) for w in range(32)))
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_cancel_aborts_or_completes_never_hangs(tmp_path) -> None:
    path = str(tmp_path / "cancel.bin")
    payload = b"y" * 65536
    await _turbofile.write_file(path, payload)
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )

    futures = [_turbofile.read(handle, 0, 65536) for _ in range(8)]
    for fut in futures[:4]:
        assert fut.cancel() is False
    results = await asyncio.gather(*futures, return_exceptions=True)
    for fut, result in zip(futures[:4], results[:4]):
        if isinstance(result, asyncio.CancelledError):
            assert fut.cancelled()
        else:
            assert result == payload
    for result in results[4:]:
        assert result == payload

    assert await _turbofile.read(handle, 0, 4) == b"yyyy"
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_queued_ops_cancel_deterministically(tmp_path) -> None:
    if _turbofile.backend_name() != "darwin-aio":
        pytest.skip("the queued-cancel floor needs the darwin aio queue")
    path = str(tmp_path / "flood.bin")
    payload = b"q" * 65536
    await _turbofile.write_file(path, payload)
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )

    # kern.aioprocmax caps in-flight aiocbs (16 by default), so the tail of
    # this burst is still in the userspace queue when the cancels arrive.
    futures = [_turbofile.read(handle, 0, 65536) for _ in range(64)]
    for fut in futures[32:]:
        fut.cancel()
    results = await asyncio.gather(*futures, return_exceptions=True)
    for result in results[:32]:
        assert result == payload
    cancelled = [r for r in results[32:] if isinstance(r, asyncio.CancelledError)]
    completed = [r for r in results[32:] if not isinstance(r, BaseException)]
    assert len(cancelled) + len(completed) == 32
    assert cancelled

    assert await _turbofile.read(handle, 0, 4) == b"qqqq"
    await _turbofile.close(handle)


@pytest.mark.asyncio
async def test_cancelled_task_buffer_is_stable_after_the_raise(tmp_path) -> None:
    path = str(tmp_path / "cancelinto.bin")
    payload = b"z" * 65536
    await _turbofile.write_file(path, payload)
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )

    buffer = bytearray(len(payload))

    async def fill() -> int:
        return await _turbofile.readinto(handle, 0, buffer)

    task = asyncio.create_task(fill())
    await asyncio.sleep(0)  # one step: the op is submitted, the task suspended
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    snapshot = bytes(buffer)
    await asyncio.sleep(0.05)
    assert bytes(buffer) == snapshot

    await _turbofile.close(handle)


def test_backend_env_override_selects_compio(tmp_path) -> None:
    script = """
import asyncio
from turbofile import _turbofile

async def main():
    assert _turbofile.backend_name().startswith("compio-")
    n, end = await _turbofile.write_file({path!r}, b"override")
    assert (n, end) == (8, 8)
    assert await _turbofile.read_file({path!r}) == b"override"

asyncio.run(main())
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", script.format(path=str(tmp_path / "env.bin"))],
        env={**os.environ, "TURBOFILE_BACKEND": "compio"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
