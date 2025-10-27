from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    import PIL

    from daft.file.typing import VideoMetadata


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
    get_metadata_impl,
    return_dtype=daft.DataType.struct(
        {
            "width": daft.DataType.int64(),
            "height": daft.DataType.int64(),
            "fps": daft.DataType.float64(),
            "frame_count": daft.DataType.int64(),
            "time_base": daft.DataType.float64(),
        }
    ),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
)


def keyframes_impl(
    file: daft.VideoFile, *, start_time: float = 0, end_time: float | None = None
) -> list[PIL.Image.Image]:
    """Get keyframes for a video file.

    Args:
        file (VideoFile): The video file to get keyframes for.
        start_time (float, optional): The start time of the keyframes. Defaults to 0.
        end_time (float | None, optional): The end time of the keyframes. Defaults to None.

    Returns:
        Python Iterator: A Python iterator containing the keyframes.
    """
    return list(file.keyframes(start_time, end_time))


keyframes = Func._from_func(
    keyframes_impl,
    return_dtype=daft.DataType.list(daft.DataType.image()),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
)
