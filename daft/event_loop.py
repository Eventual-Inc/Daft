from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future, TimeoutError
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine


_KEEPALIVE_INTERVAL_SECONDS = 0.01
_STARTUP_TIMEOUT_SECONDS = 5.0


class BackgroundEventLoop:
    def __init__(self) -> None:
        start_result: Future[asyncio.AbstractEventLoop] = Future()

        def run_loop() -> None:
            try:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self._schedule_keepalive()
                start_result.set_result(self.loop)
            except BaseException as exc:
                if not start_result.done():
                    start_result.set_exception(exc)
                return
            self.loop.run_forever()

        thread = threading.Thread(
            target=run_loop,
            name="DaftBackgroundEventLoop",
            daemon=True,
        )
        thread.start()
        try:
            self.loop = start_result.result(timeout=_STARTUP_TIMEOUT_SECONDS)
        except TimeoutError as exc:
            raise RuntimeError("Timed out starting Daft background event loop") from exc

    @classmethod
    def from_existing_loop(cls, loop: asyncio.AbstractEventLoop) -> BackgroundEventLoop:
        obj = cls.__new__(cls)
        obj.loop = loop
        return obj

    def _schedule_keepalive(self) -> None:
        # Keep selector waits finite so cross-thread coroutine submissions are
        # picked up even on platforms where the wakeup fd does not fire.
        self.loop.call_later(_KEEPALIVE_INTERVAL_SECONDS, self._schedule_keepalive)

    def run(self, future: Coroutine[Any, Any, Any]) -> Any:
        return asyncio.run_coroutine_threadsafe(future, self.loop).result()


LOOP: BackgroundEventLoop | None = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    global LOOP
    LOOP = BackgroundEventLoop.from_existing_loop(loop)


def get_or_init_event_loop() -> BackgroundEventLoop:
    global LOOP
    if LOOP is None:
        LOOP = BackgroundEventLoop()
    return LOOP
