"""Binary async file objects over the _turbofile bridge."""

from __future__ import annotations

import errno
import io
from collections.abc import Buffer, Iterable
from typing import Any

from turbofile import _turbofile
from turbofile.modes import ModeInfo

CHUNK = 65536

# Above this expected size, read-all switches from the single-op read_to_end
# (one extra buffer copy) to a size-guided read that the kernel writes straight
# into the returned bytes object, plus a probe for post-snapshot growth.
LARGE_READ = 1024 * 1024

# Chunk size for the parallel fill of one large read; parallel chunks spread
# the page-cache copy across kernel service threads.
PARALLEL_CHUNK = 2 * 1024 * 1024


class BinaryFile:
    def __init__(self, handle: int, fd: int, size: int, name: Any, info: ModeInfo) -> None:
        self.handle = handle
        self.fd = fd
        self.name = name
        self.info = info
        self.pos = size if info.append else 0
        self.is_open = True
        # Read-ahead: bytes already fetched for [pos, pos + len(pending)).
        self.pending = b""
        # Latched off once a probe shows RWF_NOWAIT reads can never succeed on
        # this file (no FMODE_NOWAIT, as on tmpfs), so such files pay one doomed
        # syscall per file rather than per read.
        self.fast = True
        # Last size this object observed; a heuristic, never a correctness input.
        self.size_hint = size

    @property
    def mode(self) -> str:
        return self.info.mode

    @property
    def closed(self) -> bool:
        return not self.is_open

    def check_open(self) -> None:
        if not self.is_open:
            raise ValueError("I/O operation on closed file")

    def check_readable(self) -> None:
        if not self.info.read:
            raise io.UnsupportedOperation("not readable")

    def check_writable(self) -> None:
        if not self.info.writable:
            raise io.UnsupportedOperation("not writable")

    async def read_at(self, pos: int, size: int) -> bytes:
        """One positional read, page-cache fast path first.

        `try_read` serves pages that are already resident on this thread and
        returns None whenever the fast path does not apply: the data is not
        resident and the read would have to block, the filesystem refuses
        RWF_NOWAIT, or the position does not fit the kernel's off_t. Every None
        falls back to a kernel submission and a completion hop, and a refused
        RWF_NOWAIT also latches the fast path off for this file.
        """
        data = _turbofile.try_read(self.fd, pos, size) if self.fast else None
        if data is None:
            if self.fast and not _turbofile.fast_read_supported(self.fd):
                self.fast = False
            return await _turbofile.read(self.handle, pos, size)
        return data

    async def readinto_at(self, pos: int, view: Buffer) -> int:
        """`readinto` counterpart of `read_at`."""
        n = _turbofile.try_readinto(self.fd, pos, view) if self.fast else None
        if n is None:
            if self.fast and not _turbofile.fast_read_supported(self.fd):
                self.fast = False
            return await _turbofile.readinto(self.handle, pos, view)
        return n

    async def read(self, size: int | None = -1, /) -> bytes:
        self.check_open()
        self.check_readable()
        if size is None or size < 0:
            start = self.pos + len(self.pending)
            if self.size_hint - start >= LARGE_READ:
                return await self.read_all_large(start)
            rest = _turbofile.try_read_all(self.fd, start) if self.fast else None
            if rest is None:
                if self.fast and not _turbofile.fast_read_supported(self.fd):
                    self.fast = False
                rest = await _turbofile.read_to_end(self.handle, start)
            data = self.pending + rest if self.pending else rest
            self.pending = b""
            self.pos += len(data)
            self.size_hint = max(self.size_hint, self.pos)
            return data
        if len(self.pending) >= size:
            data = self.pending[:size]
            self.pending = self.pending[size:]
            self.pos += size
            return data
        head = self.pending
        self.pending = b""
        rest = await self.read_at(self.pos + len(head), size - len(head))
        data = head + rest if head else rest
        self.pos += len(data)
        return data

    async def read_all_large(self, start: int) -> bytes:
        size_now = await _turbofile.size(self.handle)
        remaining = max(size_now - start, 0)
        first = await _turbofile.read_parallel(
            self.handle, start, remaining, PARALLEL_CHUNK
        )
        parts = [self.pending, first] if self.pending else [first]
        offset = start + len(first)
        if len(first) == remaining:
            # The file may have grown past the size snapshot; read to true EOF.
            while True:
                probe = await _turbofile.read_to_end(self.handle, offset)
                if not probe:
                    break
                parts.append(probe)
                offset += len(probe)
        self.pending = b""
        self.pos = offset
        self.size_hint = max(self.size_hint, offset)
        if len(parts) == 1:
            return parts[0]
        return b"".join(parts)

    async def read1(self, size: int = -1, /) -> bytes:
        self.check_open()
        self.check_readable()
        want = CHUNK if size < 0 else size
        if not self.pending:
            data = await self.read_at(self.pos, want)
            self.pos += len(data)
            return data
        data = self.pending[:want]
        self.pending = self.pending[want:]
        self.pos += len(data)
        return data

    async def readall(self) -> bytes:
        return await self.read()

    async def peek(self, size: int = 0, /) -> bytes:
        self.check_open()
        self.check_readable()
        if not self.pending:
            self.pending = await self.read_at(self.pos, CHUNK)
        return self.pending

    async def readline(self, size: int | None = -1, /) -> bytes:
        self.check_open()
        self.check_readable()
        limit = None if size is None or size < 0 else size
        idx = self.pending.find(b"\n")
        if idx == -1 and (limit is None or len(self.pending) < limit):
            parts = [self.pending]
            total = len(self.pending)
            while True:
                chunk = await self.read_at(self.pos + total, CHUNK)
                if not chunk:
                    break
                found = chunk.find(b"\n")
                parts.append(chunk)
                total += len(chunk)
                if found != -1:
                    break
                if limit is not None and total >= limit:
                    break
            self.pending = b"".join(parts)
            idx = self.pending.find(b"\n")
        end = len(self.pending) if idx == -1 else idx + 1
        if limit is not None and limit < end:
            end = limit
        line = self.pending[:end]
        self.pending = self.pending[end:]
        self.pos += end
        return line

    async def readlines(self, hint: int | None = -1, /) -> list[bytes]:
        lines: list[bytes] = []
        total = 0
        while True:
            line = await self.readline()
            if not line:
                return lines
            lines.append(line)
            total += len(line)
            if hint is not None and 0 < hint <= total:
                return lines

    async def readinto(self, buffer: Buffer, /) -> int:
        self.check_open()
        self.check_readable()
        view = memoryview(buffer).cast("B")
        if not self.pending:
            n = await self.readinto_at(self.pos, view)
            self.pos += n
            return n
        n = min(len(view), len(self.pending))
        view[:n] = self.pending[:n]
        self.pending = self.pending[n:]
        self.pos += n
        if n == len(view):
            return n
        rest = await self.readinto_at(self.pos, view[n:])
        self.pos += rest
        return n + rest

    async def write(self, data: Buffer, /) -> int:
        self.check_open()
        self.check_writable()
        self.pending = b""
        n, end = await _turbofile.write(self.handle, self.pos, data, self.info.append)
        self.pos = end
        self.size_hint = max(self.size_hint, end)
        return n

    async def writelines(self, lines: Iterable[Buffer], /) -> None:
        for line in lines:
            await self.write(line)

    async def seek(self, pos: int, whence: int = 0, /) -> int:
        self.check_open()
        if whence == 0:
            target = pos
        elif whence == 1:
            target = self.pos + pos
        elif whence == 2:
            size_now = await _turbofile.size(self.handle)
            self.size_hint = size_now
            target = size_now + pos
        else:
            raise ValueError(f"whence value {whence} unsupported")
        if target < 0:
            raise OSError(errno.EINVAL, "Invalid argument")
        self.pending = b""
        self.pos = target
        return target

    async def tell(self) -> int:
        self.check_open()
        return self.pos

    async def truncate(self, size: int | None = None, /) -> int:
        self.check_open()
        self.check_writable()
        target = self.pos if size is None else size
        self.pending = b""
        await _turbofile.set_len(self.handle, target)
        self.size_hint = target
        return target

    async def flush(self) -> None:
        self.check_open()

    async def close(self) -> None:
        if not self.is_open:
            return
        self.is_open = False
        self.pending = b""
        await _turbofile.close(self.handle)

    async def fileno(self) -> int:
        self.check_open()
        return self.fd

    async def isatty(self) -> bool:
        self.check_open()
        return False

    async def readable(self) -> bool:
        self.check_open()
        return self.info.read

    async def writable(self) -> bool:
        self.check_open()
        return self.info.writable

    async def seekable(self) -> bool:
        self.check_open()
        return True

    def detach(self) -> None:
        raise io.UnsupportedOperation("detach")

    def __aiter__(self) -> BinaryFile:
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line


class FileContext:
    """Awaitable and async-context-manager result of turbofile.open()."""

    def __init__(self, coro: Any) -> None:
        self.coro = coro
        self.file: Any = None

    def __await__(self) -> Any:
        return self.coro.__await__()

    async def __aenter__(self) -> Any:
        self.file = await self.coro
        return self.file

    async def __aexit__(self, *exc: Any) -> None:
        if self.file is not None:
            await self.file.close()
