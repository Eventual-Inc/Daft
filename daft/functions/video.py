"""Video Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    import PIL

    from daft import Expression
    from daft.file.typing import VideoMetadata


def get_metadata_impl(
    file: daft.VideoFile,
) -> VideoMetadata:
    return file.metadata()


video_metadata_fn = Func._from_func(
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


def video_metadata(
    file_expr: Expression,
) -> Expression:
    """Get metadata for a video file.

    Args:
        file_expr (VideoFile Expression): The video file to get metadata for.

    Returns:
        Expression (Struct Expression): A struct containing the metadata (width, height, fps, frame_count, time_base)
    """
    return video_metadata_fn(file_expr)


def keyframes_impl(
    file: daft.VideoFile, *, start_time: float = 0, end_time: float | None = None
) -> list[PIL.Image.Image]:
    return list(file.keyframes(start_time, end_time))


video_keyframes_fn = Func._from_func(
    keyframes_impl,
    return_dtype=daft.DataType.list(daft.DataType.image()),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
)


def video_keyframes(
    file_expr: Expression,
    *,
    start_time: float = 0,
    end_time: float | None = None,
) -> Expression:
    """Get keyframes for a video file.

    Args:
    file (VideoFile): The video file to get keyframes for.
    start_time (float, optional): The start time of the keyframes. Defaults to 0.
    end_time (float | None, optional): The end time of the keyframes. Defaults to None.

    Returns:
    Expression (List Expression): List of keyframes.
    """
    return video_keyframes_fn(file_expr, start_time=start_time, end_time=end_time)  # type: ignore
