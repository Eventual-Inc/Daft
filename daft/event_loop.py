from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine


class BackgroundEventLoop:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=self.loop.run_forever,
            name="DaftBackgroundEventLoop",
            daemon=True,
        )
        thread.start()

    @classmethod
    def from_existing_loop(cls, loop: asyncio.AbstractEventLoop) -> BackgroundEventLoop:
        obj = cls.__new__(cls)
        obj.loop = loop
        return obj

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
