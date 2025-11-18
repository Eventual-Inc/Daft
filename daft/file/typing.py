from __future__ import annotations

from typing import TypedDict


class VideoMetadata(TypedDict):
    width: int | None
    height: int | None
    fps: float | None
    duration: float | None
    frame_count: int | None
    time_base: float | None
