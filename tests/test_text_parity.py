"""Randomized parity: turbofile text mode against io.TextIOWrapper."""

import io
import random

import pytest

import turbofile

CORPUS = "äb\r\ncd é\n\rfg\r\nhi\njk lmno\npqr\r\nstu vw\nxyz"


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("newline", [None, "", "\n", "\r\n"])
async def test_random_op_sequences_match(tmp_path, seed: int, newline) -> None:
    path = tmp_path / f"parity-{seed}.txt"
    path.write_bytes(CORPUS.encode("utf-8"))
    rng = random.Random(seed)

    # Cookies are implementation-specific opaque ints (io.open's C _io packs
    # decoder state differently from the _pyio scheme this library follows),
    # so each stream seeks to its own cookie taken at the same logical point.
    with io.open(path, "r", encoding="utf-8", newline=newline) as sync_f:
        async with turbofile.open(path, "r", encoding="utf-8", newline=newline) as f:
            cookies: list[tuple[int, int]] = []
            for _ in range(40):
                op = rng.choice(["read", "readline", "tell", "seek0", "seekback"])
                if op == "read":
                    n = rng.randint(0, 6)
                    assert await f.read(n) == sync_f.read(n), f"read({n})"
                elif op == "readline":
                    assert await f.readline() == sync_f.readline()
                elif op == "tell":
                    cookies.append((await f.tell(), sync_f.tell()))
                elif op == "seek0":
                    assert await f.seek(0) == sync_f.seek(0)
                elif op == "seekback" and cookies:
                    ours, theirs = rng.choice(cookies)
                    assert await f.seek(ours) == ours
                    sync_f.seek(theirs)
                    assert await f.read(3) == sync_f.read(3)
