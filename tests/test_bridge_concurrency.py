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
async def test_cancelled_future_does_not_break_the_bridge(tmp_path) -> None:
    path = str(tmp_path / "cancel.bin")
    payload = b"y" * 65536
    await _turbofile.write_file(path, payload)
    handle, _, _ = await _turbofile.open(
        path, True, False, False, False, False, False
    )

    futures = [_turbofile.read(handle, 0, 65536) for _ in range(8)]
    for fut in futures[:4]:
        fut.cancel()
    survivors = await asyncio.gather(*futures[4:])
    for chunk in survivors:
        assert chunk == payload
    await asyncio.sleep(0.05)

    assert await _turbofile.read(handle, 0, 4) == b"yyyy"
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
