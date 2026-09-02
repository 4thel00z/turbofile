"""Parity tests for the page-cache fast paths.

These exercise `FastPath.read` / `.readinto` / `.read_all` and `try_read_file`,
which serve resident pages inline on the calling thread and return None so the
caller falls back to async submission when the data is not resident. Every
case is asserted against the stdlib's answer for the same bytes.

The filesystem is a parameter on purpose. On Linux, tmpfs sets no
`FMODE_NOWAIT`, so every fast path there fails with EOPNOTSUPP and takes the
fallback, while ext4 takes the inline path -- running both proves the two
routes agree. On macOS both directories are APFS and residency is judged by
`mincore`, so the cold-file cases are what exercise the fallback there.
"""

from __future__ import annotations

import fcntl
import os
import sys
from pathlib import Path

import pytest

import turbofile
from turbofile import _turbofile

REPO_TARGET = Path(__file__).resolve().parent.parent / "target" / "fastpath-tests"
PAGE = os.sysconf("SC_PAGESIZE")
F_NOCACHE = getattr(fcntl, "F_NOCACHE", 48)

fast_platform = pytest.mark.skipif(
    sys.platform not in ("linux", "darwin"),
    reason="no page-cache fast path on this platform",
)


def repo_dir(name: str) -> Path:
    """A fresh directory on the repo's own filesystem, where the fast route applies."""
    d = REPO_TARGET / name.replace("/", "_")[:80]
    d.mkdir(parents=True, exist_ok=True)
    for stale in d.iterdir():
        stale.unlink()
    return d


def write_cold(p: Path, payload: bytes) -> None:
    """Write `payload` so that none of its pages are in the page cache afterwards."""
    if hasattr(os, "posix_fadvise"):
        p.write_bytes(payload)
        fd = os.open(p, os.O_RDONLY)
        try:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
        return
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        fcntl.fcntl(fd, F_NOCACHE, 1)
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)


@pytest.fixture(params=["tmpfs", "ondisk"])
def workdir(request, tmp_path: Path) -> Path:
    """A directory on tmpfs (fallback route) and one on the repo's fs (fast route)."""
    if request.param == "tmpfs":
        return tmp_path
    return repo_dir(request.node.name)


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


@fast_platform
@pytest.mark.asyncio
async def test_uncached_file_still_correct(workdir: Path) -> None:
    """Cold pages force the fallback; the bytes must still be right."""
    payload = os.urandom(8 << 20)
    p = workdir / "cold.bin"
    write_cold(p, payload)
    assert await turbofile.read_bytes(p) == payload
    async with turbofile.open(p, "rb") as f:
        assert await f.read() == payload


def test_fast_path_declines_positions_beyond_off_t(workdir: Path) -> None:
    """A position past i64::MAX must not reach the kernel as a wrapped offset."""
    p = workdir / "offset.bin"
    p.write_bytes(b"0123456789")
    fd = os.open(p, os.O_RDONLY)
    try:
        fast = _turbofile.FastPath(fd)
        assert fast.read(2**63, 4) is None
        assert fast.readinto(2**63, bytearray(4)) is None
    finally:
        os.close(fd)


def test_fast_path_declines_a_directory(workdir: Path) -> None:
    """A descriptor that can never serve a read is unsupported, not an error."""
    fd = os.open(workdir, os.O_RDONLY)
    try:
        assert _turbofile.FastPath(fd).supported() is False
    finally:
        os.close(fd)


@fast_platform
def test_hot_file_is_served_inline() -> None:
    """Parity holds on either route, so this is what proves the inline one is taken."""
    payload = os.urandom(PAGE * 3)
    p = repo_dir("hot") / "hot.bin"
    p.write_bytes(payload)
    fd = os.open(p, os.O_RDONLY)
    try:
        fast = _turbofile.FastPath(fd)
        assert fast.read(0, 100) == payload[:100]
        buf = bytearray(PAGE)
        assert fast.readinto(1000, buf) == PAGE
        assert bytes(buf) == payload[1000 : 1000 + PAGE]
        assert fast.read_all(2000) == payload[2000:]
        assert fast.read(len(payload) - 10, 100) == payload[-10:]
        assert fast.supported() is True
    finally:
        os.close(fd)


@fast_platform
def test_cold_pages_take_the_fallback_until_warmed() -> None:
    """Non-resident pages return None; one ordinary read makes them servable.

    The still-cold check is on the last page of an 8 MiB file, well past any
    readahead window the warming read may have opened at the front.
    """
    payload = os.urandom(8 << 20)
    p = repo_dir("cold-warm") / "cold.bin"
    write_cold(p, payload)
    fd = os.open(p, os.O_RDONLY)
    try:
        fast = _turbofile.FastPath(fd)
        assert fast.read(0, PAGE) is None
        assert fast.supported() is True
        os.pread(fd, PAGE, 0)
        assert fast.read(0, PAGE) == payload[:PAGE]
        assert fast.read(len(payload) - PAGE, PAGE) is None
    finally:
        os.close(fd)


@fast_platform
def test_fast_path_follows_growth_past_its_first_view() -> None:
    """Bytes appended long after the first read are still served once resident."""
    head = os.urandom(PAGE)
    tail = os.urandom(2 << 20)
    p = repo_dir("growth") / "grow.bin"
    p.write_bytes(head)
    fd = os.open(p, os.O_RDONLY)
    try:
        fast = _turbofile.FastPath(fd)
        assert fast.read(0, 100) == head[:100]
        with open(p, "ab") as w:
            w.write(tail)
        assert fast.read(PAGE, PAGE) == tail[:PAGE]
        assert fast.read(len(head) + len(tail) - PAGE, PAGE) == tail[-PAGE:]
        assert fast.read_all(0) == head + tail
    finally:
        os.close(fd)


@fast_platform
@pytest.mark.asyncio
async def test_fast_path_stays_latched_on_for_the_repo_filesystem() -> None:
    """A lost latch would silently hand every read back to the thread hop."""
    p = repo_dir("latched") / "latched.bin"
    p.write_bytes(b"x" * 8192)
    async with turbofile.open(p, "rb") as f:
        assert await f.read(100) == b"x" * 100
        await f.seek(4096)
        assert await f.read() == b"x" * 4096
        assert f.fast is not None


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
