from __future__ import annotations

from typing import TYPE_CHECKING

from daft.io.video.video_source import VideoSource

if TYPE_CHECKING:
    from daft.dataframe.dataframe import DataFrame
    from daft.daft import IOConfig


__all__ = [ "VideoSource", "read_video"]


def read_video(
    path: str | list[str],
    image_height: int,
    image_width: int,
    is_key_frame: bool = False,
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Creates a DataFrame by reading frames in a video file.

    This produces a DataFrame with the following fields:
        * path (string): path to the video file that produced this frame.
        * frame_number (int): frame number in the video.
        * frame_time (float): frame time in fractioanl seconds as a floating point.
        * frame_time_base (str): fractional unit of seconds in which timestamps are expressed.
        * frame_pts (int): frame presentation timestamp in time_base units.
        * frame_dts (int): frame decoding timestamp in time_base units.
        * frame_duration (int): frame duration in time_base units.
        * is_key_frame (bool): true iff this is a key frame.

    Note:
        This function will stream the frames from all videos as a DataFrame of images.
        If you wish to load an entire video into a single row, this can be done with
        read_glob_path and url.download.

    Args:
        path (str|list[str]): Path(s) to the video file(s) which allows wildcards.
        image_height (int): Height to which each frame will be resized.
        image_width (int): Width to which each frame will be resized.
        is_key_frame (bool): If set, only key frames will be added to the DataFrame.
        io_config (IOConfig|None): Optional IOConfig.

    Returns:
        DataFrame: dataframe of images.

    Examples:
        >>> df = daft.read_video("/path/to/file.csv")
        >>> df = daft.read_video("/path/to/directory")
        >>> df = daft.read_video("/path/to/files-*.csv")
        >>> df = daft.read_video("s3://path/to/files-*.csv")
    """
    # TODO: image_mode and some kind of sample rate.
    return VideoSource(
        path=path,
        image_height=image_height,
        image_width=image_width,
        is_key_frame=is_key_frame,
        io_config=io_config,
    ).read()
