from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.dataframe.dataframe import DataFrame
    from daft.daft import IOConfig

__all__ = [ 
    # TODO: additional video support
    # "read_audio_frames",
    # "read_audio_streams",
    # "read_audio_streams_metadata",
    # "read_subtitle_frames",
    # "read_subtitle_streams",
    # "read_subtitle_streams_metadata",
    "read_video_frames",
    # "read_video_streams",
    # "read_video_streams_metadata",
]


def read_video_frames(
    path: str | list[str],
    image_height: int,
    image_width: int,
    is_key_frame: bool | None = None,
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Creates a DataFrame by reading the frames of one or more video files.

    This produces a DataFrame with the following fields:
        * path (string): path to the video file that produced this frame.
        * frame_number (int): frame number in the video.
        * frame_time (float): frame time in fractioanl seconds as a floating point.
        * frame_time_base (str): fractional unit of seconds in which timestamps are expressed.
        * frame_pts (int): frame presentation timestamp in time_base units.
        * frame_dts (int): frame decoding timestamp in time_base units.
        * frame_duration (int): frame duration in time_base units.
        * is_key_frame (bool): true iff this is a key frame.

    Warning:
        This requires PyAV which can be installed with `pip install av`.

    Note:
        This function will stream the frames from all videos as a DataFrame of images.
        If you wish to load an entire video into a single row, this can be done with
        read_glob_path and url.download.

    Args:
        path (str|list[str]): Path(s) to the video file(s) which allows wildcards.
        image_height (int): Height to which each frame will be resized.
        image_width (int): Width to which each frame will be resized.
        is_key_frame (bool|None): If True, only include key frames; if False, only non-key frames; if None, include all frames.
        io_config (IOConfig|None): Optional IOConfig.

    Returns:
        DataFrame: dataframe of images.

    Examples:
        >>> df = daft.read_video("/path/to/file.")
        >>> df = daft.read_video("/path/to/directory")
        >>> df = daft.read_video("/path/to/files-*.csv")
        >>> df = daft.read_video("s3://path/to/files-*.csv")
    """
    try:
        from daft.io.video._read_video_frames import _VideoFramesSource
    except ImportError as e:
        raise ImportError("read_video_frames requires PyAV. Please install it with `pip install av`.") from e
    return _VideoFramesSource(
        path=path,
        image_height=image_height,
        image_width=image_width,
        is_key_frame=is_key_frame,
        io_config=io_config,
    ).read()
