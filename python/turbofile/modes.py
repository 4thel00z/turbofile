"""Mode-string parsing with builtin-open semantics."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ModeInfo:
    mode: str
    read: bool
    write: bool
    append: bool
    truncate: bool
    create: bool
    create_new: bool
    binary: bool

    @property
    def writable(self) -> bool:
        return self.write or self.append


def parse_mode(mode: str) -> ModeInfo:
    letters = set(mode)
    if not letters <= set("rwxab+t") or len(mode) != len(letters):
        raise ValueError(f"invalid mode: {mode!r}")
    base = letters & set("rwxa")
    if len(base) != 1:
        raise ValueError("must have exactly one of create/read/write/append mode")
    if "t" in letters and "b" in letters:
        raise ValueError("can't have text and binary mode at once")

    updating = "+" in letters
    return ModeInfo(
        mode=mode,
        read="r" in letters or updating,
        write="w" in letters or "x" in letters or updating,
        append="a" in letters,
        truncate="w" in letters,
        create="w" in letters or "a" in letters,
        create_new="x" in letters,
        binary="b" in letters,
    )
