from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypedDict

if TYPE_CHECKING:
    from daft.dependencies import np, pil_image


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
    data: pil_image.Image


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


class Hdf5Metadata(TypedDict):
    shape: tuple[int, ...]
    dtype: np.dtype
    chunks: tuple[int, ...] | None
    compression: str | None
    compression_opts: Any


class Hdf5ObjectMetadata(TypedDict):
    h5path: str
    kind: Literal["dataset", "group"]
    shape: list[int]
    dtype: str
    chunks: list[int]
    compression: str
