"""Completion futures for kernel ops, and how asyncio drives them."""

from __future__ import annotations

import asyncio
from typing import Any

from turbofile import _turbofile


class KernelFuture(asyncio.Future[Any]):
    """Completion of in-flight kernel ops; `cancel()` requests their abort.

    `cancel()` forwards an abort request for the future's ops (their ids sit in
    `op_ids`) and returns False: the future settles when the ops do, promptly
    on abort, so a caller's buffer is never touched after its `await` raises.
    The drain turns an ECANCELED settle into a real cancellation through
    `settle_cancelled`. A whole-file read that came back as an open handle
    keeps the task filling it in `continuation`; cancelling the future cancels
    that task, which settles the future when it is done.

    `send`, `throw` and `close` let `asyncio.create_task` drive the future the
    way it drives a coroutine. `read_bytes` returns the completion future
    itself, so a gather over many files schedules no task per file, and a
    caller that wraps one in a task keeps working. An exception thrown in
    while the ops are still in flight is kept in `thrown` and raised once
    they settle, the way a coroutine cancelled at its `await` ends cancelled
    even when the awaited value arrives.
    """

    __slots__ = ("op_ids", "continuation", "thrown")

    def cancel(self, msg: object = None) -> bool:
        if self.done():
            return False
        continuation = getattr(self, "continuation", None)
        if continuation is not None:
            continuation.cancel()
            return False
        _turbofile.cancel_ops(self.op_ids)
        return False

    def settle_cancelled(self) -> bool:
        return super().cancel()

    def send(self, value: object) -> KernelFuture:
        if not self.done():
            self._asyncio_future_blocking = True
            return self
        thrown = getattr(self, "thrown", None)
        if thrown is not None:
            raise thrown
        raise StopIteration(self.result())

    def throw(
        self, exc: BaseException, value: object = None, traceback: object = None
    ) -> KernelFuture:
        if not self.done():
            self.thrown = exc
            self.cancel()
            self._asyncio_future_blocking = True
            return self
        raise exc

    def close(self) -> None:
        self.cancel()
