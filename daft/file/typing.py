from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    import PIL


class VideoMetadata(TypedDict):
    width: int | None
    height: int | None
    fps: float | None
    duration: float | None
    frame_count: int | None
    time_base: float | None


class VideoFrameData(TypedDict):
    frame_index: int
    frame_time: float | None
    frame_time_base: str | None
    frame_pts: int | None
    frame_dts: int | None
    frame_duration: int | None
    is_key_frame: bool
    data: PIL.Image.Image


class AudioMetadata(TypedDict):
    sample_rate: int
    channels: int
    frames: int
    format: str
    subtype: str | None


class ImageMetadata(TypedDict):
    width: int | None
    height: int | None
    format: str | None
    mode: str | None
