"""Video Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    import PIL

    from daft import Expression
    from daft.file.typing import VideoFrameData, VideoMetadata


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
    name_override="video_metadata",
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
    return [frame["data"] for frame in file.frames(start_time=start_time, end_time=end_time, is_key_frame=True)]


video_keyframes_fn = Func._from_func(
    keyframes_impl,
    return_dtype=daft.DataType.list(daft.DataType.image()),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="video_keyframes",
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


def frames_impl(
    file: daft.VideoFile,
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> list[VideoFrameData]:
    return list(
        file.frames(
            start_time=start_time,
            end_time=end_time,
            width=width,
            height=height,
            is_key_frame=is_key_frame,
            sample_interval_seconds=sample_interval_seconds,
        )
    )


_VIDEO_FRAMES_RETURN_DTYPE = daft.DataType.list(
    daft.DataType.struct(
        {
            "frame_index": daft.DataType.int64(),
            "frame_time": daft.DataType.float64(),
            "frame_time_base": daft.DataType.string(),
            "frame_pts": daft.DataType.int64(),
            "frame_dts": daft.DataType.int64(),
            "frame_duration": daft.DataType.int64(),
            "is_key_frame": daft.DataType.bool(),
            "data": daft.DataType.image(),
        }
    )
)


video_frames_fn = Func._from_func(
    frames_impl,
    return_dtype=_VIDEO_FRAMES_RETURN_DTYPE,
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="video_frames",
)


def video_frames(
    file_expr: Expression,
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> Expression:
    """Decode all video frames within a time range, with per-frame metadata.

    Mirrors the per-frame schema of ``daft.read_video_frames()``.

    Args:
        file_expr (VideoFile Expression): The video file to decode frames from.
        start_time (float, optional): Start of the time range in seconds. Defaults to 0.
        end_time (float | None, optional): End of the time range in seconds. Defaults to None (all frames).
        width (int | None, optional): Target width for resizing frames. Must be provided with ``height``.
        height (int | None, optional): Target height for resizing frames. Must be provided with ``width``.
        is_key_frame (bool | None, optional): If True, decode only keyframes. If False,
            decode only non-keyframes. If None, decode all frames.
        sample_interval_seconds (float | None, optional): If provided and > 0, sample frames at
            approximately this time interval in seconds based on ``frame_time``. The algorithm
            picks the first frame whose timestamp is >= the next target time (``start_time``,
            ``start_time + interval``, ``start_time + 2*interval``, ...). Frames without valid
            timestamps are skipped. Same semantics as the source-side
            :func:`daft.read_video_frames`. Defaults to None (no sampling).

    Returns:
        Expression (List[Struct] Expression): List of structs, each containing:
            - frame_index (int): 0-based index of the frame in the video stream
            - frame_time (float): Presentation time in seconds
            - frame_time_base (str): Time base as a fraction string
            - frame_pts (int): Presentation timestamp in stream time_base units
            - frame_dts (int): Decode timestamp in stream time_base units
            - frame_duration (int): Duration in stream time_base units
            - is_key_frame (bool): Whether this frame is a keyframe
            - data (Image): The decoded frame as an image
    """
    return video_frames_fn(
        file_expr,
        start_time=start_time,
        end_time=end_time,
        width=width,
        height=height,
        is_key_frame=is_key_frame,
        sample_interval_seconds=sample_interval_seconds,
    )  # type: ignore


def frames_from_bytes_impl(
    video_bytes: bytes | None,
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> list[VideoFrameData]:
    import io

    import av

    from daft.dependencies import pil_image
    from daft.file.video import _iter_frames_from_container

    # Null bytes (e.g. from an upstream download UDF that signalled failure
    # by returning None) are treated as "no frames" rather than an exception
    # so the surrounding pipeline can branch on the original null column to
    # decide how to surface the error — typically by populating an
    # ``extract_error`` column. Raising here would abort the whole batch
    # before any sibling expressions get to handle the null.
    if video_bytes is None:
        return []
    if not pil_image.module_available():
        raise ImportError(
            "The 'pillow' module is required for frame decoding. Install it with `pip install daft[video]`."
        )
    if (width is None) != (height is None):
        raise ValueError("Both width and height must be specified together for resizing.")
    if sample_interval_seconds is not None and sample_interval_seconds <= 0:
        raise ValueError("sample_interval_seconds must be positive if provided")

    # PyAV's container.__exit__ does not close the underlying file-like, so we
    # own the BytesIO lifetime here. Without an explicit close() the raw video
    # bytes (often hundreds of MB per row) would stay pinned until the next GC
    # pass — costly inside long-running Ray actor processes.
    buf = io.BytesIO(video_bytes)
    try:
        with av.open(buf) as container:
            return list(
                _iter_frames_from_container(
                    container,
                    start_time=start_time,
                    end_time=end_time,
                    width=width,
                    height=height,
                    is_key_frame=is_key_frame,
                    sample_interval_seconds=sample_interval_seconds,
                )
            )
    finally:
        buf.close()


video_frames_from_bytes_fn = Func._from_func(
    frames_from_bytes_impl,
    return_dtype=_VIDEO_FRAMES_RETURN_DTYPE,
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="video_frames_from_bytes",
)


def video_frames_from_bytes(
    bytes_expr: Expression,
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> Expression:
    """Decode video frames directly from a binary column holding the encoded video bytes.

    Companion to :func:`video_frames` for cases where the video data is already in memory
    (for example, downloaded from a non-URI-addressable storage system inside a UDF) and
    you do not want to round-trip through a path. Returns the same per-frame schema as
    :func:`video_frames`.

    Null inputs are mapped to empty frame lists so that an upstream UDF can signal
    download / fetch failure with ``None`` and let downstream operators (typically a
    sibling ``when(col.is_null(), ...)`` populating an ``extract_error`` column) handle
    it without aborting the whole batch.

    Args:
        bytes_expr (Binary Expression): A column of bytes, each row containing one encoded video.
        start_time (float, optional): Start of the time range in seconds. Defaults to 0.
        end_time (float | None, optional): End of the time range in seconds. Defaults to None.
        width (int | None, optional): Target width for resizing frames. Must be provided with ``height``.
        height (int | None, optional): Target height for resizing frames. Must be provided with ``width``.
        is_key_frame (bool | None, optional): If True, decode only keyframes. If False,
            decode only non-keyframes. If None, decode all frames.
        sample_interval_seconds (float | None, optional): If provided and > 0, sample frames at
            approximately this time interval in seconds. Same semantics as :func:`video_frames`.

    Returns:
        Expression (List[Struct] Expression): List of structs with the same schema as
        :func:`video_frames`. Empty when the input row is null.
    """
    return video_frames_from_bytes_fn(
        bytes_expr,
        start_time=start_time,
        end_time=end_time,
        width=width,
        height=height,
        is_key_frame=is_key_frame,
        sample_interval_seconds=sample_interval_seconds,
    )  # type: ignore
