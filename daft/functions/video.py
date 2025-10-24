from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.file import VideoMetadata
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from collections.abc import Iterator

    import PIL


def get_metadata_impl(
    file: daft.VideoFile,
    *,
    probesize: str = "64k",
    analyzeduration_us: int = 200_000,
) -> VideoMetadata:
    """Get metadata for a video file.

    Args:
        file (VideoFile): The video file to get metadata for.
        probesize (str, optional): The size of the probe buffer. Defaults to "64k".
        analyzeduration_us (int, optional): The duration of the analysis. Defaults to 200_000.

    Returns:
        Struct: A struct containing the metadata (width, height, fps, frame_count, time_base)
    """
    return file.metadata(probesize=probesize, analyzeduration_us=analyzeduration_us)


get_metadata = Func._from_func(
    get_metadata_impl, return_dtype=VideoMetadata, unnest=False, use_process=None, is_batch=False, batch_size=None
)


def keyframes_impl(
    file: daft.VideoFile, *, start_time: float = 0, end_time: float | None = None
) -> Iterator[PIL.Image.Image]:
    """Get keyframes for a video file.

    Args:
        file (VideoFile): The video file to get keyframes for.
        start_time (float, optional): The start time of the keyframes. Defaults to 0.
        end_time (float | None, optional): The end time of the keyframes. Defaults to None.

    Returns:
        Python Iterator: A Python iterator containing the keyframes.
    """
    return file.keyframes(start_time, end_time)


keyframes = Func._from_func(
    keyframes_impl, return_dtype=daft.DataType.python(), unnest=False, use_process=None, is_batch=False, batch_size=None
)
