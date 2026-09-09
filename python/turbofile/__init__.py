"""Real async file I/O for Python, backed by Rust completion queues."""

from __future__ import annotations

import asyncio
import atexit
import functools
import os
from collections.abc import Awaitable
from typing import Any

from turbofile import _turbofile
from turbofile.binary import LARGE_READ, BinaryFile, FileContext, read_to_eof_parallel
from turbofile.futures import KernelFuture
from turbofile.modes import parse_mode
from turbofile.text import TextFile, resolve_encoding

__all__ = ["open", "read_bytes", "read_text", "write_bytes", "write_text"]

# Once the interpreter starts finalizing, driver-thread completions must not
# attach to Python anymore.
atexit.register(_turbofile.shutdown)

# Linux serves a resident whole file on the calling thread; the other platforms
# have no cached-only open and take the async path straight away.
read_file_inline = getattr(_turbofile, "try_read_file", None)


def open(
    file: Any,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,
    opener: Any = None,
) -> FileContext:
    return FileContext(
        open_file(file, mode, buffering, encoding, errors, newline, closefd, opener)
    )


async def open_file(
    file: Any,
    mode: str,
    buffering: int,
    encoding: str | None,
    errors: str | None,
    newline: str | None,
    closefd: bool,
    opener: Any,
) -> Any:
    info = parse_mode(mode)
    if info.binary:
        if encoding is not None:
            raise ValueError("binary mode doesn't take an encoding argument")
        if errors is not None:
            raise ValueError("binary mode doesn't take an errors argument")
        if newline is not None:
            raise ValueError("binary mode doesn't take a newline argument")
    else:
        if buffering == 0:
            raise ValueError("can't have unbuffered text I/O")
        if newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError(f"illegal newline value: {newline!r}")
    if opener is not None:
        raise NotImplementedError("turbofile does not support custom openers")
    if not closefd:
        raise ValueError("Cannot use closefd=False with file name")
    if isinstance(file, int):
        raise NotImplementedError("turbofile does not open existing descriptors")

    path = os.fspath(file)
    handle, size, fd = await _turbofile.open(
        path,
        info.read,
        info.write,
        info.append,
        info.truncate,
        info.create,
        info.create_new,
    )
    opened = BinaryFile(handle, fd, size, name=file, info=info)
    if info.binary:
        return opened
    return TextFile(opened, encoding, errors, newline)


def read_bytes(path: Any) -> Awaitable[bytes]:
    """The whole file as bytes.

    Inside a running loop this is the completion future itself, so a gather
    over many files schedules no task per file; it can still be wrapped in a
    task. Outside a loop it is a coroutine, for `asyncio.run(read_bytes(p))`.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return read_bytes_later(path)
    p = os.fspath(path)
    if read_file_inline:
        data = read_file_inline(p)
        if data is not None:
            return settled(loop, data)
    # One submission does open+read+close on the driver for a file up to
    # LARGE_READ; a larger one comes back as an open handle and
    # finish_large_read fills the returned bytes in parallel chunks with no
    # extra copy.
    return _turbofile.read_file(p, LARGE_READ, finish_large_read)


async def read_bytes_later(path: Any) -> bytes:
    return await read_bytes(path)


def settled(loop: asyncio.AbstractEventLoop, value: Any) -> KernelFuture:
    """A value served inline, as the same kind of future the async path
    returns, so a task can wrap it and a gather takes it as it is."""
    done = KernelFuture(loop=loop)
    done.set_result(value)
    return done


def finish_large_read(pending: KernelFuture, handle: int, fd: int) -> None:
    """Called by the drain when read_file handed back an open handle: fill the
    file on an eagerly started task and settle `pending` from that task's
    outcome. Eager start puts the coroutine inside its `try` before any cancel
    can arrive, so the handle is closed whichever way the task ends."""
    task = asyncio.Task(
        read_handle_to_end(handle, fd), loop=pending.get_loop(), eager_start=True
    )
    pending.continuation = task
    task.add_done_callback(functools.partial(settle_from, pending))


async def read_handle_to_end(handle: int, fd: int) -> bytes:
    try:
        return await read_to_eof_parallel(handle, _turbofile.FastPath(fd), 0)
    finally:
        await _turbofile.close(handle)


def settle_from(pending: KernelFuture, task: asyncio.Task[bytes]) -> None:
    if task.cancelled():
        pending.settle_cancelled()
        return
    exc = task.exception()
    if exc is not None:
        pending.set_exception(exc)
        return
    pending.set_result(task.result())


def write_bytes(path: Any, data: Any) -> Awaitable[int]:
    """Write `data` as the whole file and return the count written, under the
    same future-or-coroutine rule as read_bytes."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return write_bytes_later(path, data)
    return _turbofile.write_file(os.fspath(path), data)


async def write_bytes_later(path: Any, data: Any) -> int:
    return await write_bytes(path, data)


async def read_text(
    path: Any, encoding: str | None = None, errors: str | None = None
) -> str:
    data = await read_bytes(path)
    return data.decode(resolve_encoding(encoding), errors or "strict")


async def write_text(
    path: Any, text: str, encoding: str | None = None, errors: str | None = None
) -> int:
    payload = text.encode(resolve_encoding(encoding), errors or "strict")
    return await write_bytes(path, payload)
