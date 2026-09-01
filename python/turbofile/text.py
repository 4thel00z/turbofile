"""Text-mode support: encoding resolution and the text file object.

Seek/tell reconstruction follows CPython's _pyio.TextIOWrapper: a cookie packs
(start byte, decoder flags, bytes to refeed, eof flag, chars to skip) so any
position reachable by tell() can be restored exactly, stateful codecs included.
"""

from __future__ import annotations

import codecs
import io
import locale
import os
from collections.abc import Iterable
from typing import Any

from turbofile.binary import BinaryFile

CHUNK = 8192


def resolve_encoding(encoding: str | None) -> str:
    chosen = io.text_encoding(encoding)
    if chosen == "locale":
        return locale.getencoding()
    return chosen


class TextFile:
    def __init__(
        self,
        buffer: BinaryFile,
        encoding: str | None,
        errors: str | None,
        newline: str | None,
    ) -> None:
        self.buffer = buffer
        self.encoding = resolve_encoding(encoding)
        self.errors = errors or "strict"
        self.newline = newline
        self.readuniversal = not newline
        self.readtranslate = newline is None
        if newline in ("", "\n"):
            self.writenl = None
        elif newline is None:
            self.writenl = os.linesep
        else:
            self.writenl = newline
        self.decoder: Any = None
        self.encoder: codecs.IncrementalEncoder | None = None
        # Decoded chars of the current chunk and how many are consumed.
        self.decoded = ""
        self.used = 0
        # (decoder flags, bytes) at the start of the current chunk.
        self.snapshot: tuple[int, bytes] | None = None

    @property
    def mode(self) -> str:
        return self.buffer.mode

    @property
    def name(self) -> Any:
        return self.buffer.name

    @property
    def closed(self) -> bool:
        return self.buffer.closed

    @property
    def newlines(self) -> Any:
        if isinstance(self.decoder, io.IncrementalNewlineDecoder):
            return self.decoder.newlines
        return None

    @property
    def line_buffering(self) -> bool:
        return False

    @property
    def write_through(self) -> bool:
        return True

    def make_decoder(self) -> Any:
        decoder = codecs.getincrementaldecoder(self.encoding)(self.errors)
        if self.readuniversal:
            return io.IncrementalNewlineDecoder(decoder, translate=self.readtranslate)
        return decoder

    def take_chars(self, n: int | None = None) -> str:
        avail = self.decoded[self.used :]
        if n is None or n < 0 or n >= len(avail):
            self.used = len(self.decoded)
            return avail
        self.used += n
        return avail[:n]

    def rewind_chars(self, n: int) -> None:
        self.used -= n

    async def fill(self) -> bool:
        if self.decoder is None:
            self.decoder = self.make_decoder()
        dec_buffer, dec_flags = self.decoder.getstate()
        chunk = await self.buffer.read1(CHUNK)
        eof = not chunk
        self.decoded = self.decoder.decode(chunk, final=eof)
        self.used = 0
        self.snapshot = (dec_flags, dec_buffer + chunk)
        return not eof

    async def read(self, size: int | None = -1, /) -> str:
        self.buffer.check_open()
        self.buffer.check_readable()
        if self.decoder is None:
            self.decoder = self.make_decoder()
        if size is None or size < 0:
            rest = await self.buffer.read()
            result = self.take_chars() + self.decoder.decode(rest, final=True)
            self.decoded = ""
            self.used = 0
            self.snapshot = None
            return result
        result = self.take_chars(size)
        while len(result) < size:
            more = await self.fill()
            result += self.take_chars(size - len(result))
            if not more:
                break
        return result

    async def readline(self, size: int | None = -1, /) -> str:
        self.buffer.check_open()
        self.buffer.check_readable()
        limit = -1 if size is None else size
        if self.decoder is None:
            self.decoder = self.make_decoder()

        line = self.take_chars()
        start = 0
        endpos = None
        while True:
            if self.readtranslate:
                pos = line.find("\n", start)
                if pos >= 0:
                    endpos = pos + 1
                    break
                start = len(line)
            elif self.readuniversal:
                nlpos = line.find("\n", start)
                crpos = line.find("\r", start)
                if crpos == -1:
                    if nlpos == -1:
                        start = len(line)
                    else:
                        endpos = nlpos + 1
                        break
                elif nlpos == -1:
                    if crpos == len(line) - 1:
                        # Trailing \r: the next char decides if this is \r\n.
                        start = crpos
                    else:
                        endpos = crpos + 1
                        break
                elif nlpos < crpos:
                    endpos = nlpos + 1
                    break
                elif nlpos == crpos + 1:
                    endpos = nlpos + 1
                    break
                else:
                    endpos = crpos + 1
                    break
            else:
                pos = line.find(self.newline, start)
                if pos >= 0:
                    endpos = pos + len(self.newline)
                    break
                start = max(0, len(line) - len(self.newline) + 1)

            if 0 <= limit <= len(line):
                endpos = limit
                break

            while await self.fill():
                if self.decoded:
                    break
            if not self.decoded:
                self.snapshot = None
                return line
            line += self.take_chars()

        if 0 <= limit < endpos:
            endpos = limit
        self.rewind_chars(len(line) - endpos)
        return line[:endpos]

    async def readlines(self, hint: int | None = -1, /) -> list[str]:
        lines: list[str] = []
        total = 0
        while True:
            line = await self.readline()
            if not line:
                return lines
            lines.append(line)
            total += len(line)
            if hint is not None and 0 < hint <= total:
                return lines

    async def write(self, text: str, /) -> int:
        self.buffer.check_open()
        self.buffer.check_writable()
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        length = len(text)
        if self.writenl:
            text = text.replace("\n", self.writenl)
        if self.encoder is None:
            self.encoder = codecs.getincrementalencoder(self.encoding)(self.errors)
        payload = self.encoder.encode(text)
        if payload:
            await self.buffer.write(payload)
        self.decoded = ""
        self.used = 0
        self.snapshot = None
        if self.decoder is not None:
            self.decoder.reset()
        return length

    async def writelines(self, lines: Iterable[str], /) -> None:
        for line in lines:
            await self.write(line)

    def pack_cookie(
        self,
        position: int,
        dec_flags: int = 0,
        bytes_to_feed: int = 0,
        need_eof: bool = False,
        chars_to_skip: int = 0,
    ) -> int:
        return (
            position
            | (dec_flags << 64)
            | (bytes_to_feed << 128)
            | (chars_to_skip << 192)
            | (int(need_eof) << 256)
        )

    def unpack_cookie(self, cookie: int) -> tuple[int, int, int, bool, int]:
        rest, position = divmod(cookie, 1 << 64)
        rest, dec_flags = divmod(rest, 1 << 64)
        rest, bytes_to_feed = divmod(rest, 1 << 64)
        need_eof, chars_to_skip = divmod(rest, 1 << 64)
        return position, dec_flags, bytes_to_feed, bool(need_eof), chars_to_skip

    async def tell(self) -> int:
        self.buffer.check_open()
        position = self.buffer.pos
        decoder = self.decoder
        if decoder is None or self.snapshot is None:
            return position
        dec_flags, next_input = self.snapshot
        position -= len(next_input)
        chars_to_skip = self.used
        if chars_to_skip == 0:
            return self.pack_cookie(position, dec_flags)

        saved_state = decoder.getstate()
        try:
            decoder.setstate((b"", dec_flags))
            start_pos = position
            start_flags = dec_flags
            bytes_fed = 0
            chars_decoded = 0
            need_eof = False
            for i in range(len(next_input)):
                chars_decoded += len(decoder.decode(next_input[i : i + 1]))
                bytes_fed += 1
                dec_buffer, flags = decoder.getstate()
                if not dec_buffer and chars_decoded <= chars_to_skip:
                    # Clean decoder point: the cookie can start here.
                    start_pos += bytes_fed
                    start_flags = flags
                    bytes_fed = 0
                    chars_to_skip -= chars_decoded
                    chars_decoded = 0
                if chars_decoded >= chars_to_skip:
                    break
            else:
                chars_decoded += len(decoder.decode(b"", final=True))
                need_eof = True
                if chars_decoded < chars_to_skip:
                    raise OSError("can't reconstruct logical file position")
            return self.pack_cookie(
                start_pos, start_flags, bytes_fed, need_eof, chars_to_skip
            )
        finally:
            decoder.setstate(saved_state)

    async def seek(self, cookie: int, whence: int = 0, /) -> int:
        buffer = self.buffer
        buffer.check_open()
        if whence == 1:
            if cookie != 0:
                raise io.UnsupportedOperation("can't do nonzero cur-relative seeks")
            cookie = await self.tell()
            whence = 0
        if whence == 2:
            if cookie != 0:
                raise io.UnsupportedOperation("can't do nonzero end-relative seeks")
            position = await buffer.seek(0, 2)
            self.decoded = ""
            self.used = 0
            self.snapshot = None
            if self.decoder is not None:
                self.decoder.reset()
            if self.encoder is not None:
                self.encoder.setstate(0)
            return position
        if whence != 0:
            raise ValueError(f"invalid whence ({whence}, should be 0, 1 or 2)")
        if cookie < 0:
            raise ValueError(f"negative seek position {cookie!r}")

        start_pos, dec_flags, bytes_to_feed, need_eof, chars_to_skip = (
            self.unpack_cookie(cookie)
        )
        await buffer.seek(start_pos)
        self.decoded = ""
        self.used = 0
        self.snapshot = None

        if cookie == 0 and self.decoder is not None:
            self.decoder.reset()
            self.snapshot = (0, b"")
        elif self.decoder is not None or dec_flags or chars_to_skip:
            if self.decoder is None:
                self.decoder = self.make_decoder()
            self.decoder.setstate((b"", dec_flags))
            self.snapshot = (dec_flags, b"")
        if chars_to_skip:
            chunk = await buffer.read(bytes_to_feed)
            self.decoded = self.decoder.decode(chunk, final=need_eof)
            self.used = 0
            self.snapshot = (dec_flags, chunk)
            if len(self.decoded) < chars_to_skip:
                raise OSError("can't restore logical file position")
            self.used = chars_to_skip
        if self.encoder is not None:
            if start_pos == 0 and not chars_to_skip:
                self.encoder.reset()
            else:
                self.encoder.setstate(0)
        return cookie

    async def truncate(self, size: int | None = None, /) -> int:
        if size is not None:
            return await self.buffer.truncate(size)
        cookie = await self.tell()
        position, dec_flags, bytes_to_feed, need_eof, chars_to_skip = (
            self.unpack_cookie(cookie)
        )
        if dec_flags or bytes_to_feed or need_eof or chars_to_skip:
            raise io.UnsupportedOperation(
                "can't truncate at a position with pending decoder state"
            )
        return await self.buffer.truncate(position)

    async def flush(self) -> None:
        await self.buffer.flush()

    async def close(self) -> None:
        await self.buffer.close()

    async def fileno(self) -> int:
        return await self.buffer.fileno()

    async def isatty(self) -> bool:
        return await self.buffer.isatty()

    async def readable(self) -> bool:
        return await self.buffer.readable()

    async def writable(self) -> bool:
        return await self.buffer.writable()

    async def seekable(self) -> bool:
        return await self.buffer.seekable()

    def detach(self) -> None:
        raise io.UnsupportedOperation("detach")

    def __aiter__(self) -> TextFile:
        return self

    async def __anext__(self) -> str:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line
