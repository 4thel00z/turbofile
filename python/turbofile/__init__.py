"""Real async file I/O for Python, backed by Rust completion queues."""

from __future__ import annotations

import atexit
import os
from typing import Any

from turbofile import _turbofile
from turbofile.binary import BinaryFile, FileContext
from turbofile.modes import parse_mode
from turbofile.text import TextFile, resolve_encoding

__all__ = ["open", "read_bytes", "read_text", "write_bytes", "write_text"]

# Once the interpreter starts finalizing, driver-thread completions must not
# attach to Python anymore.
atexit.register(_turbofile.shutdown)


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


async def read_bytes(path: Any) -> bytes:
    p = os.fspath(path)
    # Whole file from page cache on this thread when nothing would block;
    # otherwise one submission does open+read+close on the driver.
    data = _turbofile.try_read_file(p)
    if data is None:
        return await _turbofile.read_file(p)
    return data


async def write_bytes(path: Any, data: Any) -> int:
    n, _ = await _turbofile.write_file(os.fspath(path), data)
    return n


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
