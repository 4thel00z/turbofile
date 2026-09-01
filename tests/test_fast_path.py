"""Parity tests for the page-cache fast paths.

These exercise `try_read` / `try_read_all` / `try_read_file` / `try_readinto`,
which serve reads inline with `preadv2(RWF_NOWAIT)` and fall back to async
submission when the data is not resident. Every case is asserted against the
stdlib's answer for the same bytes.

The filesystem is a parameter on purpose. tmpfs sets no `FMODE_NOWAIT`, so
every fast path there fails with EOPNOTSUPP and takes the fallback, while ext4
takes the inline path -- so running both is what proves the two routes agree.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

import turbofile
from turbofile import _turbofile

REPO_TARGET = Path(__file__).resolve().parent.parent / "target" / "fastpath-tests"


@pytest.fixture(params=["tmpfs", "ondisk"])
def workdir(request, tmp_path: Path) -> Path:
    """A directory on tmpfs (fallback route) and one on the repo's fs (fast route)."""
    if request.param == "tmpfs":
        return tmp_path
    REPO_TARGET.mkdir(parents=True, exist_ok=True)
    d = REPO_TARGET / request.node.name.replace("/", "_")[:80]
    d.mkdir(parents=True, exist_ok=True)
    for stale in d.iterdir():
        stale.unlink()
    return d


SIZES = [0, 1, 7, 4095, 4096, 4097, 65535, 65536, 65537, 300_000]


@pytest.mark.asyncio
@pytest.mark.parametrize("size", SIZES)
async def test_read_all_matches_stdlib(workdir: Path, size: int) -> None:
    p = workdir / f"all-{size}.bin"
    payload = os.urandom(size)
    p.write_bytes(payload)
    async with turbofile.open(p, "rb") as f:
        assert await f.read() == payload
    assert await turbofile.read_bytes(p) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [0, 1, 4096, 65537])
@pytest.mark.parametrize("want", [1, 100, 4096, 999_999])
async def test_sized_read_matches_stdlib(workdir: Path, size: int, want: int) -> None:
    p = workdir / f"sized-{size}-{want}.bin"
    payload = os.urandom(size)
    p.write_bytes(payload)
    async with turbofile.open(p, "rb") as f:
        assert await f.read(want) == payload[:want]


@pytest.mark.asyncio
async def test_sequential_reads_track_position(workdir: Path) -> None:
    payload = os.urandom(50_000)
    p = workdir / "seq.bin"
    p.write_bytes(payload)
    async with turbofile.open(p, "rb") as f:
        acc = b""
        while chunk := await f.read(4096):
            acc += chunk
        assert acc == payload
        assert await f.tell() == len(payload)


@pytest.mark.asyncio
async def test_read_after_seek(workdir: Path) -> None:
    payload = os.urandom(20_000)
    p = workdir / "seek.bin"
    p.write_bytes(payload)
    async with turbofile.open(p, "rb") as f:
        for off in (0, 1, 4095, 4096, 9999, 19_999, 20_000):
            await f.seek(off)
            assert await f.read(1000) == payload[off : off + 1000]
            await f.seek(off)
            assert await f.read() == payload[off:]


@pytest.mark.asyncio
async def test_readinto_matches_stdlib(workdir: Path) -> None:
    payload = os.urandom(10_000)
    p = workdir / "into.bin"
    p.write_bytes(payload)
    async with turbofile.open(p, "rb") as f:
        buf = bytearray(4096)
        n = await f.readinto(buf)
        assert bytes(buf[:n]) == payload[:n]
        rest = bytearray(10_000)
        n2 = await f.readinto(rest)
        assert bytes(rest[:n2]) == payload[n : n + n2]


@pytest.mark.asyncio
async def test_readline_and_iteration(workdir: Path) -> None:
    text = b"".join(b"line %d\n" % i for i in range(5000))
    p = workdir / "lines.bin"
    p.write_bytes(text)
    async with turbofile.open(p, "rb") as f:
        assert [line async for line in f] == text.splitlines(keepends=True)


@pytest.mark.asyncio
async def test_growth_between_size_and_read(workdir: Path) -> None:
    """A file that grows after the size snapshot must not yield a short read."""
    p = workdir / "grow.bin"
    p.write_bytes(b"a" * 1000)
    async with turbofile.open(p, "rb") as f:
        with open(p, "ab") as w:
            w.write(b"b" * 1000)
            w.flush()
        assert await f.read() == b"a" * 1000 + b"b" * 1000


@pytest.mark.asyncio
async def test_large_file_round_trips(workdir: Path) -> None:
    """Larger than every internal chunk, so it spans several reads either way."""
    payload = os.urandom(8 << 20)
    p = workdir / "large.bin"
    p.write_bytes(payload)
    assert await turbofile.read_bytes(p) == payload
    async with turbofile.open(p, "rb") as f:
        assert await f.read() == payload


@pytest.mark.skipif(
    not hasattr(os, "posix_fadvise"),
    reason="needs posix_fadvise to evict the page cache (Linux only)",
)
@pytest.mark.asyncio
async def test_uncached_file_still_correct(workdir: Path) -> None:
    """Evicted pages force the EAGAIN fallback; the bytes must still be right."""
    payload = os.urandom(8 << 20)
    p = workdir / "cold.bin"
    p.write_bytes(payload)
    fd = os.open(p, os.O_RDONLY)
    try:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)
    assert await turbofile.read_bytes(p) == payload
    async with turbofile.open(p, "rb") as f:
        assert await f.read() == payload


def test_try_read_declines_positions_beyond_off_t(workdir: Path) -> None:
    """A position past i64::MAX must not reach the kernel as a wrapped offset."""
    p = workdir / "offset.bin"
    p.write_bytes(b"0123456789")
    fd = os.open(p, os.O_RDONLY)
    try:
        assert _turbofile.try_read(fd, 2**63, 4) is None
        assert _turbofile.try_readinto(fd, 2**63, bytearray(4)) is None
    finally:
        os.close(fd)


def test_fast_read_supported_declines_a_directory(workdir: Path) -> None:
    """Only a completed probe or EAGAIN proves support; EISDIR does not."""
    fd = os.open(workdir, os.O_RDONLY)
    try:
        assert _turbofile.fast_read_supported(fd) is False
    finally:
        os.close(fd)


@pytest.mark.skipif(sys.platform != "linux", reason="RWF_NOWAIT is Linux-only")
@pytest.mark.asyncio
async def test_fast_path_stays_latched_on_for_the_repo_filesystem() -> None:
    """Parity holds on either route, so this is what proves the inline one is taken."""
    REPO_TARGET.mkdir(parents=True, exist_ok=True)
    p = REPO_TARGET / "latched.bin"
    p.write_bytes(b"x" * 8192)
    fd = os.open(p, os.O_RDONLY)
    try:
        assert _turbofile.fast_read_supported(fd) is True
    finally:
        os.close(fd)
    async with turbofile.open(p, "rb") as f:
        assert await f.read(100) == b"x" * 100
        await f.seek(4096)
        assert await f.read() == b"x" * 4096
        assert f.fast is True


@pytest.mark.asyncio
async def test_missing_file_raises_through_fallback(workdir: Path) -> None:
    with pytest.raises(OSError):
        await turbofile.read_bytes(workdir / "does-not-exist.bin")


@pytest.mark.asyncio
async def test_directory_is_not_served_by_fast_path(workdir: Path) -> None:
    """try_read_file must decline non-regular files rather than invent bytes."""
    assert _turbofile.try_read_file(str(workdir)) is None
    with pytest.raises(OSError):
        await turbofile.read_bytes(workdir)


@pytest.mark.asyncio
async def test_text_mode_parity(workdir: Path) -> None:
    text = "hello\nwörld\r\nlast line without newline"
    p = workdir / "text.txt"
    p.write_text(text, encoding="utf-8", newline="")
    async with turbofile.open(p, "r", encoding="utf-8") as f:
        assert await f.read() == text.replace("\r\n", "\n")
    assert await turbofile.read_text(p) == text
