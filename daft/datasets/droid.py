from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import (
    file_exists,
    format,
    hdf5_file,
    regexp_replace,
    unnest,
    video_file,
    when,
)
from daft.io import GCSConfig, IOConfig

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


_PUBLIC_GCS_BUCKET = "gs://gresearch/robotics/droid_raw"

_METADATA_DTYPE = DataType.struct(
    {
        "uuid": DataType.string,
        "lab": DataType.string,
        "user": DataType.string,
        "user_id": DataType.string,
        "date": DataType.date,
        "timestamp": DataType.string,
        "hdf5_path": DataType.string,
        "building": DataType.string,
        "scene_id": DataType.int64,
        "success": DataType.bool,
        "robot_serial": DataType.string,
        "r2d2_version": DataType.string,
        "current_task": DataType.string,
        "trajectory_length": DataType.int64,
        "wrist_cam_serial": DataType.string,
        "ext1_cam_serial": DataType.string,
        "ext2_cam_serial": DataType.string,
        "wrist_cam_extrinsics": DataType.list(DataType.float64),
        "ext1_cam_extrinsics": DataType.list(DataType.float64),
        "ext2_cam_extrinsics": DataType.list(DataType.float64),
        "wrist_svo_path": DataType.string,
        "wrist_mp4_path": DataType.string,
        "ext1_svo_path": DataType.string,
        "ext1_mp4_path": DataType.string,
        "ext2_svo_path": DataType.string,
        "ext2_mp4_path": DataType.string,
        "left_mp4_path": DataType.string,
        "right_mp4_path": DataType.string,
    }
)


@PublicAPI
def raw(
    # By default, use the official public GCS bucket
    path: str = _PUBLIC_GCS_BUCKET,
    io_config: IOConfig | None = None,
    # TODO: Add support for stereo videos
    # include_stereo: bool = False,
    # TODO: Add support for SVO camera recordings
) -> DataFrame:
    r"""Load the raw DROID robotics dataset as a lazy episode-level DataFrame.

    This function discovers episodes by globbing ``metadata_*.json`` files under the
    provided dataset root, reads the episode metadata, and attaches lazy file references
    to the per-episode trajectory HDF5 file and MP4 camera recordings.

    Note:
        The public dataset is missing camera recordings for some episodes. Those that are missing
        will be set to `None`.

    Args:
        path: Root path to the raw DROID dataset. Defaults to the official public
            GCS release at `gs://gresearch/robotics/droid_raw`. Also supports
            local paths and other remote object stores.
        io_config: IO configuration for accessing remote storage.
        verify_videos: Whether to verify that the video files exist and are valid. Defaults to True.

    Returns:
        A DataFrame with one row per episode. Metadata fields from each episode's JSON
        file are stored in the `metadata` struct column, along with:

        - `episode_dir`: path to the episode directory
        - `metadata.*`: metadata fields parsed from the metadata JSON file
        - `trajectory`: lazy `daft.Hdf5File` reference to the trajectory HDF5 file
        - `wrist_video`: lazy `daft.VideoFile` reference to the wrist camera MP4 file
        - `ext1_video`: lazy `daft.VideoFile` reference to the external camera 1 MP4 file
            Often the left camera feed.
        - `ext2_video`: lazy `daft.VideoFile` reference to the external camera 2 MP4 file
            Often the right camera feed.

    Examples:
        >>> import daft
        >>> df = daft.datasets.droid.raw()  # doctest: +SKIP
        >>> df.select("episode_dir", "ext1_video").show()  # doctest: +SKIP
    """
    # Configure IO config with anonymous access to the public GCS bucket
    if io_config is None and path == _PUBLIC_GCS_BUCKET:
        io_config = IOConfig(gcs=GCSConfig(anonymous=True))

    episodes = (
        daft.from_glob_path(f"{path.rstrip('/')}/**/metadata_*.json", io_config=io_config)
        .select(
            col("path")
            .download(io_config=io_config)
            .cast(DataType.string)
            .try_deserialize("json", _METADATA_DTYPE)
            .alias("metadata"),
            regexp_replace(col("path"), r"/metadata_[^/]+\.json$", "").alias("episode_dir"),
        )
        .select(unnest(col("metadata")), "episode_dir")
    )

    # Create a file column for the trajectory HDF5 file
    episodes = episodes.with_column(
        "trajectory",
        hdf5_file(format("{}/trajectory.h5", col("episode_dir")), io_config=io_config),
    ).with_column(
        "trajectory",
        when(file_exists(col("trajectory")), col("trajectory")).otherwise(lit(None)),
    )

    # Create VideoFile columns for MP4 camera recordings
    episodes = episodes.with_columns(
        {
            "wrist_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("wrist_cam_serial")),
                io_config=io_config,
            ),
            "ext1_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("ext1_cam_serial")),
                io_config=io_config,
            ),
            "ext2_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("ext2_cam_serial")),
                io_config=io_config,
            ),
        }
    ).with_columns(
        {
            "wrist_video": when(file_exists(col("wrist_video")), col("wrist_video")).otherwise(lit(None)),
            "ext1_video": when(file_exists(col("ext1_video")), col("ext1_video")).otherwise(lit(None)),
            "ext2_video": when(file_exists(col("ext2_video")), col("ext2_video")).otherwise(lit(None)),
        }
    )

    return episodes


# TODO: Add a custom expression to read & parse the trajectory HDF5 file

__all__ = [
    "raw",
]
