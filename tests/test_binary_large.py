"""Large-file read-all goes through the size-guided zero-copy path."""

import os

import pytest

import turbofile
from turbofile import binary


@pytest.mark.asyncio
async def test_large_read_all_roundtrip(tmp_path) -> None:
    path = tmp_path / "large.bin"
    payload = bytes(range(256)) * (8 * 1024 * 8)  # 16 MiB
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        assert await f.read() == payload


@pytest.mark.asyncio
async def test_large_read_all_from_offset_with_pending(tmp_path) -> None:
    path = tmp_path / "offset.bin"
    payload = bytes(range(256)) * (4 * 1024 * 8)  # 8 MiB
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        head = await f.readline()  # leaves read-ahead pending
        rest = await f.read()
        assert head + rest == payload


@pytest.mark.asyncio
async def test_read_all_sees_growth_past_the_size_snapshot(tmp_path) -> None:
    path = tmp_path / "growing.bin"
    payload = b"z" * (2 * binary.LARGE_READ)
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        size = await f.seek(0, 2)
        assert size == len(payload)
        await f.seek(0)
        # Grow the file after the size snapshot the fast path relies on.
        with open(path, "ab") as w:
            w.write(b"tail")
        assert await f.read() == payload + b"tail"


@pytest.mark.asyncio
async def test_read_all_after_another_writer_truncated(tmp_path) -> None:
    path = tmp_path / "shrinking.bin"
    payload = b"s" * (3 * binary.LARGE_READ)
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        # Shrink after the open-time size snapshot, but keep it a large read.
        os.truncate(path, 2 * binary.LARGE_READ)
        assert await f.read() == payload[: 2 * binary.LARGE_READ]


@pytest.mark.asyncio
async def test_read_bytes_large_file_roundtrip(tmp_path) -> None:
    path = tmp_path / "whole-large.bin"
    payload = bytes(range(256)) * (2 * 4096) + b"odd tail"  # 2 MiB + 8
    path.write_bytes(payload)
    assert await turbofile.read_bytes(path) == payload
