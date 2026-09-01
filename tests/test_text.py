"""Text-mode behavior of turbofile.open, held against io.TextIOWrapper."""

import io
import os

import pytest

import turbofile


@pytest.mark.asyncio
async def test_text_write_read_roundtrip(tmp_path) -> None:
    path = tmp_path / "text.txt"
    async with turbofile.open(path, "w", encoding="utf-8") as f:
        assert await f.write("héllo wörld\n") == 12
    async with turbofile.open(path, "r", encoding="utf-8") as f:
        assert await f.read() == "héllo wörld\n"


@pytest.mark.asyncio
async def test_default_mode_is_text(tmp_path) -> None:
    path = tmp_path / "default.txt"
    path.write_text("plain", encoding="utf-8")
    async with turbofile.open(path, encoding="utf-8") as f:
        assert await f.read() == "plain"


@pytest.mark.asyncio
async def test_write_translates_newlines_like_builtin(tmp_path) -> None:
    path = tmp_path / "nl.txt"
    async with turbofile.open(path, "w", encoding="ascii") as f:
        await f.write("a\nb\n")
    assert path.read_bytes() == f"a{os.linesep}b{os.linesep}".encode("ascii")


@pytest.mark.asyncio
async def test_universal_newlines_translate_on_read(tmp_path) -> None:
    path = tmp_path / "universal.txt"
    path.write_bytes(b"one\r\ntwo\rthree\nfour")
    async with turbofile.open(path, "r", encoding="ascii") as f:
        assert await f.read() == "one\ntwo\nthree\nfour"
    async with turbofile.open(path, "r", encoding="ascii") as f:
        lines = [line async for line in f]
        assert lines == ["one\n", "two\n", "three\n", "four"]


@pytest.mark.asyncio
async def test_newline_empty_keeps_terminators(tmp_path) -> None:
    path = tmp_path / "keep.txt"
    path.write_bytes(b"one\r\ntwo\rthree\n")
    async with turbofile.open(path, "r", encoding="ascii", newline="") as f:
        assert await f.readline() == "one\r\n"
        assert await f.readline() == "two\r"
        assert await f.readline() == "three\n"
        assert await f.readline() == ""


@pytest.mark.asyncio
async def test_explicit_newline_read_and_write(tmp_path) -> None:
    path = tmp_path / "crlf.txt"
    async with turbofile.open(path, "w", encoding="ascii", newline="\r\n") as f:
        await f.write("a\nb\n")
    assert path.read_bytes() == b"a\r\nb\r\n"
    async with turbofile.open(path, "r", encoding="ascii", newline="\r\n") as f:
        assert await f.readline() == "a\r\n"
        assert await f.readline() == "b\r\n"


@pytest.mark.asyncio
async def test_read_counts_characters_not_bytes(tmp_path) -> None:
    path = tmp_path / "chars.txt"
    path.write_text("äöüß-tail", encoding="utf-8")
    async with turbofile.open(path, "r", encoding="utf-8") as f:
        assert await f.read(4) == "äöüß"
        assert await f.read() == "-tail"


@pytest.mark.asyncio
async def test_utf16_roundtrip_writes_one_bom(tmp_path) -> None:
    path = tmp_path / "utf16.txt"
    async with turbofile.open(path, "w", encoding="utf-16") as f:
        await f.write("ab")
        await f.write("cd")
    assert path.read_text(encoding="utf-16") == "abcd"
    raw = path.read_bytes()
    assert raw.count(b"\xff\xfe") + raw.count(b"\xfe\xff") == 1


@pytest.mark.asyncio
async def test_errors_ignore(tmp_path) -> None:
    path = tmp_path / "bad.txt"
    path.write_bytes(b"ok\xffok")
    async with turbofile.open(path, "r", encoding="ascii", errors="ignore") as f:
        assert await f.read() == "okok"


@pytest.mark.asyncio
async def test_tell_seek_roundtrip_matches_textiowrapper(tmp_path) -> None:
    path = tmp_path / "cookies.txt"
    path.write_bytes("äb\r\ncdé\nfg\rhij\n".encode("utf-8"))

    with io.open(path, "r", encoding="utf-8") as sync_f:
        expect_first = sync_f.read(3)
        expect_cookie_at = sync_f.tell()
        expect_line = sync_f.readline()
        sync_f.seek(expect_cookie_at)
        expect_line_again = sync_f.readline()

    async with turbofile.open(path, "r", encoding="utf-8") as f:
        assert await f.read(3) == expect_first
        cookie = await f.tell()
        line = await f.readline()
        assert line == expect_line
        assert await f.seek(cookie) == cookie
        assert await f.readline() == expect_line_again


@pytest.mark.asyncio
async def test_seek_start_and_end(tmp_path) -> None:
    path = tmp_path / "ends.txt"
    path.write_text("0123456789", encoding="ascii")
    async with turbofile.open(path, "r", encoding="ascii") as f:
        assert await f.read(4) == "0123"
        assert await f.seek(0) == 0
        assert await f.read(2) == "01"
        end = await f.seek(0, 2)
        assert end == 10
        assert await f.read() == ""


@pytest.mark.asyncio
async def test_text_metadata_and_writelines(tmp_path) -> None:
    path = tmp_path / "meta.txt"
    async with turbofile.open(path, "w", encoding="utf-8") as f:
        assert f.mode == "w"
        assert f.encoding == "utf-8"
        assert not await f.readable()
        assert await f.writable()
        await f.writelines(["x\n", "y\n"])
    assert path.read_text(encoding="utf-8") == "x\ny\n"


@pytest.mark.asyncio
async def test_invalid_newline_rejected(tmp_path) -> None:
    with pytest.raises(ValueError):
        await turbofile.open(tmp_path / "x.txt", "r", newline="\r\r")


@pytest.mark.asyncio
async def test_truncate_at_clean_position(tmp_path) -> None:
    path = tmp_path / "trunc.txt"
    path.write_text("0123456789", encoding="ascii")
    async with turbofile.open(path, "r+", encoding="ascii") as f:
        await f.read(4)
        assert await f.truncate() == 4
    assert path.read_text(encoding="ascii") == "0123"


@pytest.mark.asyncio
async def test_truncate_with_pending_decoder_state_refuses(tmp_path) -> None:
    path = tmp_path / "dirty.txt"
    path.write_bytes(b"a\rbcd")
    async with turbofile.open(path, "r+", encoding="ascii") as f:
        await f.read(2)  # ends on the translated \r; decoder holds pending-cr
        cookie = await f.tell()
        assert cookie >= (1 << 64), "construction must yield a stateful cookie"
        with pytest.raises(io.UnsupportedOperation):
            await f.truncate()
