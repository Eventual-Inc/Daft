from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import (
    file,
    format,
    regexp_replace,
    unnest,
    video_file,
)
from daft.io import GCSConfig, IOConfig

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


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
    path: str = "gs://gresearch/robotics/droid_raw",
    io_config: IOConfig | None = None,
    # TODO: Add support for stereo videos
    # include_stereo: bool = False,
    # TODO: Add support for SVO camera recordings
) -> DataFrame:
    r"""Load the raw DROID robotics dataset as a lazy episode-level DataFrame.

    This function discovers episodes by globbing ``metadata_*.json`` files under the
    provided dataset root, reads the episode metadata, and attaches lazy file references
    to the per-episode trajectory HDF5 file and MP4 camera recordings.

    Each row corresponds to one DROID episode with the following layout on disk:

        episode/
        |---- metadata_<episode_id>.json    # Episode metadata like building ID, data collector ID etc.
        |---- trajectory.h5                 # All low-dimensional information like action and proprioception trajectories.
        |---- recordings/
                  |---- MP4/
                         |---- <camera_serial>.mp4
                         |---- <camera_serial>-stereo.mp4  # Optional stereo views.
                  |---- SVO/
                         |---- <camera_serial>.svo         # Raw ZED SVO file with encoded camera recording information (contains some additional metadata)

    Args:
        path: Root path to the raw DROID dataset. Defaults to the official public
            GCS release at `gs://gresearch/robotics/droid_raw`. Also supports
            local paths and other remote object stores.
        io_config: IO configuration for accessing remote storage.

    Returns:
        A DataFrame with one row per episode. Metadata fields from each episode's JSON
        file are stored in the `metadata` struct column, along with:

        - `episode_dir`: path to the episode directory
        - `metadata.*`: metadata fields parsed from the metadata JSON file
        - `trajectory`: lazy `daft.File` reference to the trajectory HDF5 file
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
    if io_config is None:
        io_config = IOConfig(gcs=GCSConfig(anonymous=True))

    episodes = (
        daft.from_glob_path(f"{path.rstrip('/')}/**/metadata_*.json", io_config=io_config)
        .select(
            col("path")
            .download(io_config=io_config)
            .cast(DataType.string())
            .try_deserialize("json", _METADATA_DTYPE)
            .alias("metadata"),
            regexp_replace(col("path"), r"/metadata_[^/]+\.json$", "").alias("episode_dir"),
        )
        .select(unnest(col("metadata")), "episode_dir")
    )

    # Create a file column for the trajectory HDF5 file
    episodes = episodes.with_column(
        "trajectory",
        file(format("{}/{}", col("episode_dir"), lit("trajectory.h5")), io_config=io_config),
    )

    # Create VideoFile columns for MP4 camera recordings
    episodes = (
        episodes.with_column(
            "wrist_video",
            video_file(format("{}/{}.mp4", col("episode_dir"), col("wrist_cam_serial")), io_config=io_config),
        )
        .with_column(
            "ext1_video",
            video_file(format("{}/{}.mp4", col("episode_dir"), col("ext1_cam_serial")), io_config=io_config),
        )
        .with_column(
            "ext2_video",
            video_file(format("{}/{}.mp4", col("episode_dir"), col("ext2_cam_serial")), io_config=io_config),
        )
    )

    return episodes


# TODO: Add a custom expression to read & parse the trajectory HDF5 file

__all__ = [
    "raw",
]
