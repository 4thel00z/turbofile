"""Binary-mode behavior of turbofile.open."""

import asyncio
import inspect
from typing import Any

import pytest

import turbofile


@pytest.mark.asyncio
async def test_write_then_read_roundtrip(tmp_path) -> None:
    path = tmp_path / "roundtrip.bin"
    async with turbofile.open(path, "wb") as f:
        assert await f.write(b"hello turbofile") == 15
    async with turbofile.open(path, "rb") as f:
        assert await f.read() == b"hello turbofile"


@pytest.mark.asyncio
async def test_open_is_awaitable_without_context_manager(tmp_path) -> None:
    path = tmp_path / "await.bin"
    path.write_bytes(b"direct")
    f = await turbofile.open(path, "rb")
    try:
        assert await f.read(3) == b"dir"
        assert await f.read() == b"ect"
        assert await f.read() == b""
    finally:
        await f.close()


@pytest.mark.asyncio
async def test_seek_and_tell(tmp_path) -> None:
    path = tmp_path / "seek.bin"
    path.write_bytes(b"0123456789")
    async with turbofile.open(path, "rb") as f:
        assert await f.tell() == 0
        assert await f.seek(4) == 4
        assert await f.read(2) == b"45"
        assert await f.tell() == 6
        assert await f.seek(-2, 2) == 8
        assert await f.read() == b"89"
        assert await f.seek(1, 1) == 11
        assert await f.read() == b""


@pytest.mark.asyncio
async def test_readline_and_iteration(tmp_path) -> None:
    path = tmp_path / "lines.bin"
    path.write_bytes(b"one\ntwo\nthree")
    async with turbofile.open(path, "rb") as f:
        assert await f.readline() == b"one\n"
        assert await f.readline() == b"two\n"
        assert await f.readline() == b"three"
        assert await f.readline() == b""
    async with turbofile.open(path, "rb") as f:
        lines = [line async for line in f]
        assert lines == [b"one\n", b"two\n", b"three"]


@pytest.mark.asyncio
async def test_readlines_matches_builtin(tmp_path) -> None:
    path = tmp_path / "readlines.bin"
    path.write_bytes(b"a\nb\nc\n")
    async with turbofile.open(path, "rb") as f:
        assert await f.readlines() == [b"a\n", b"b\n", b"c\n"]


@pytest.mark.asyncio
async def test_append_mode(tmp_path) -> None:
    path = tmp_path / "append.bin"
    path.write_bytes(b"start-")
    async with turbofile.open(path, "ab") as f:
        assert await f.tell() == 6
        await f.write(b"more")
        assert await f.tell() == 10
    assert path.read_bytes() == b"start-more"


@pytest.mark.asyncio
async def test_update_mode_reads_and_writes(tmp_path) -> None:
    path = tmp_path / "update.bin"
    path.write_bytes(b"abcdef")
    async with turbofile.open(path, "rb+") as f:
        await f.seek(2)
        await f.write(b"XY")
        await f.seek(0)
        assert await f.read() == b"abXYef"


@pytest.mark.asyncio
async def test_exclusive_mode_refuses_existing(tmp_path) -> None:
    path = tmp_path / "exists.bin"
    path.write_bytes(b"")
    with pytest.raises(FileExistsError):
        await turbofile.open(path, "xb")


@pytest.mark.asyncio
async def test_truncate(tmp_path) -> None:
    path = tmp_path / "trunc.bin"
    path.write_bytes(b"0123456789")
    async with turbofile.open(path, "rb+") as f:
        await f.seek(4)
        assert await f.truncate() == 4
        assert await f.truncate(2) == 2
    assert path.read_bytes() == b"01"


@pytest.mark.asyncio
async def test_readinto(tmp_path) -> None:
    path = tmp_path / "into.bin"
    path.write_bytes(b"abcdefgh")
    async with turbofile.open(path, "rb") as f:
        sink = bytearray(5)
        assert await f.readinto(sink) == 5
        assert bytes(sink) == b"abcde"
        assert await f.tell() == 5


@pytest.mark.asyncio
async def test_closed_file_raises_value_error(tmp_path) -> None:
    path = tmp_path / "closed.bin"
    path.write_bytes(b"x")
    f = await turbofile.open(path, "rb")
    await f.close()
    assert f.closed
    await f.close()
    with pytest.raises(ValueError):
        await f.read()


@pytest.mark.asyncio
async def test_metadata_properties_and_probes(tmp_path) -> None:
    path = tmp_path / "meta.bin"
    path.write_bytes(b"x")
    async with turbofile.open(path, "rb") as f:
        assert f.mode == "rb"
        assert f.name == path
        assert not f.closed
        assert await f.readable()
        assert not await f.writable()
        assert await f.seekable()
        assert not await f.isatty()
        assert isinstance(await f.fileno(), int)
        await f.flush()


@pytest.mark.asyncio
async def test_writelines(tmp_path) -> None:
    path = tmp_path / "writelines.bin"
    async with turbofile.open(path, "wb") as f:
        await f.writelines([b"a\n", b"b\n"])
    assert path.read_bytes() == b"a\nb\n"


@pytest.mark.asyncio
async def test_invalid_modes_raise(tmp_path) -> None:
    path = tmp_path / "modes.bin"
    with pytest.raises(ValueError):
        await turbofile.open(path, "rz")
    with pytest.raises(ValueError):
        await turbofile.open(path, "rw")
    with pytest.raises(ValueError):
        await turbofile.open(path, "rb", encoding="utf-8")
    with pytest.raises(ValueError):
        await turbofile.open(path, "rb", newline="\n")
    with pytest.raises(ValueError):
        await turbofile.open(path, "r", buffering=0)


def counting_task_factory(created: list[Any]) -> Any:
    def factory(
        loop: asyncio.AbstractEventLoop, coro: Any, context: Any = None
    ) -> asyncio.Task[Any]:
        created.append(coro)
        return asyncio.Task(coro, loop=loop, context=context)

    return factory


@pytest.mark.asyncio
async def test_read_at_in_a_running_loop_is_a_future(tmp_path) -> None:
    path = tmp_path / "at.bin"
    payload = bytes(range(256)) * 16
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        pending = f.read_at(256, 512)
        assert asyncio.isfuture(pending)
        assert await pending == payload[256:768]
        assert await f.tell() == 0
        assert await asyncio.create_task(f.read_at(0, 4)) == payload[:4]


@pytest.mark.asyncio
async def test_gathering_read_at_makes_no_task_per_read(tmp_path) -> None:
    path = tmp_path / "gathered.bin"
    payload = bytes(range(256)) * 64
    path.write_bytes(payload)
    offsets = [(i * 7919 * 256) % (len(payload) - 256) for i in range(32)]
    loop = asyncio.get_running_loop()
    created: list[Any] = []
    async with turbofile.open(path, "rb") as f:
        loop.set_task_factory(counting_task_factory(created))
        try:
            chunks = await asyncio.gather(*(f.read_at(off, 256) for off in offsets))
        finally:
            loop.set_task_factory(None)
    assert chunks == [payload[off : off + 256] for off in offsets]
    assert created == []


@pytest.mark.asyncio
async def test_read_at_without_the_fast_path_is_the_kernel_future(tmp_path) -> None:
    path = tmp_path / "kernel.bin"
    payload = b"k" * 8192
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        f.fast = None
        pending = f.read_at(4096, 4096)
        assert asyncio.isfuture(pending)
        assert not pending.done()
        assert await pending == payload[4096:]
        sink = bytearray(100)
        filling = f.readinto_at(10, sink)
        assert asyncio.isfuture(filling)
        assert await filling == 100
        assert bytes(sink) == payload[10:110]


@pytest.mark.asyncio
async def test_readinto_at_in_a_running_loop_is_a_future(tmp_path) -> None:
    path = tmp_path / "into.bin"
    payload = bytes(range(256)) * 4
    path.write_bytes(payload)
    async with turbofile.open(path, "rb") as f:
        sink = bytearray(300)
        filling = f.readinto_at(100, sink)
        assert asyncio.isfuture(filling)
        assert await filling == 300
        assert bytes(sink) == payload[100:400]


def test_read_at_outside_a_loop_is_a_coroutine(tmp_path) -> None:
    path = tmp_path / "later.bin"
    payload = b"positional, awaited later"
    path.write_bytes(payload)
    async def opened() -> Any:
        return await turbofile.open(path, "rb")

    f = asyncio.run(opened())
    pending = f.read_at(12, 7)
    assert inspect.iscoroutine(pending)
    assert asyncio.run(pending) == payload[12:19]
    asyncio.run(f.close())
