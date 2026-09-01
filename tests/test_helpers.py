"""Whole-file convenience helpers (single-op fast paths)."""

import pytest

import turbofile


@pytest.mark.asyncio
async def test_bytes_roundtrip(tmp_path) -> None:
    path = tmp_path / "payload.bin"
    payload = bytes(range(256)) * 100
    assert await turbofile.write_bytes(path, payload) == len(payload)
    assert await turbofile.read_bytes(path) == payload


@pytest.mark.asyncio
async def test_text_roundtrip(tmp_path) -> None:
    path = tmp_path / "payload.txt"
    text = "grüße aus dem kernel\nzeile zwei\n"
    await turbofile.write_text(path, text)
    assert await turbofile.read_text(path) == text
    assert path.read_text(encoding="utf-8") == text


@pytest.mark.asyncio
async def test_text_helpers_take_encoding(tmp_path) -> None:
    path = tmp_path / "latin.txt"
    await turbofile.write_text(path, "café", encoding="latin-1")
    assert path.read_bytes() == b"caf\xe9"
    assert await turbofile.read_text(path, encoding="latin-1") == "café"


@pytest.mark.asyncio
async def test_read_bytes_missing_file(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        await turbofile.read_bytes(tmp_path / "absent.bin")
