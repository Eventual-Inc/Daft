from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.context import DaftContext


class Heartbeat:
    """Per-query heartbeat to notify subscribers query is still alive.

    Runs in a background thread once start() is called.
    """

    interval: float
    ctx: DaftContext
    query_id: str
    stop_event: threading.Event
    heart: threading.Thread

    def __init__(self, interval: float, ctx: DaftContext, query_id: str):
        self.interval = interval
        self.ctx = ctx
        self.query_id = query_id
        self.stop_event = threading.Event()
        self.heart = threading.Thread(target=self._beat, daemon=True)

    def start(self) -> None:
        """Start the heartbeat in a background thread."""
        self.heart.start()

    def _beat(self) -> None:
        while not self.stop_event.wait(self.interval):
            self.ctx._notify_query_heartbeat(self.query_id)

    def stop(self) -> None:
        """Stop sending heartbeats. Call this before process exit to ensure all heartbeats are fully sent."""
        self.stop_event.set()
        self.heart.join()
