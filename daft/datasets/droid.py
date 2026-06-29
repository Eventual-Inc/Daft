from __future__ import annotations

import re
from collections.abc import Sequence
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
    video_frames,
    when,
)
from daft.io import GCSConfig, IOConfig

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.file.hdf5 import Hdf5File


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

_DEFAULT_TRAJECTORY_FIELDS: tuple[str, ...] = (
    "action/joint_position",
    "action/joint_velocity",
    "action/gripper_position",
    "action/gripper_velocity",
    "action/cartesian_position",
    "action/cartesian_velocity",
    "action/target_gripper_position",
    "action/target_cartesian_position",
    "observation/robot_state/joint_positions",
    "observation/robot_state/joint_velocities",
    "observation/robot_state/gripper_position",
    "observation/robot_state/cartesian_position",
)

_FLOAT64_TRAJECTORY_DTYPE = DataType.tensor(DataType.float64())
_INT64_TRAJECTORY_DTYPE = DataType.tensor(DataType.int64())
_BOOL_TRAJECTORY_DTYPE = DataType.tensor(DataType.bool())

_FLOAT64_TRAJECTORY_FIELDS: frozenset[str] = frozenset(
    {
        "action/cartesian_position",
        "action/cartesian_velocity",
        "action/gripper_position",
        "action/gripper_velocity",
        "action/joint_position",
        "action/joint_velocity",
        "action/target_cartesian_position",
        "action/target_gripper_position",
        "action/robot_state/cartesian_position",
        "action/robot_state/gripper_position",
        "action/robot_state/joint_positions",
        "action/robot_state/joint_torques_computed",
        "action/robot_state/joint_velocities",
        "action/robot_state/motor_torques_measured",
        "action/robot_state/prev_controller_latency_ms",
        "action/robot_state/prev_joint_torques_computed",
        "action/robot_state/prev_joint_torques_computed_safened",
        "observation/robot_state/cartesian_position",
        "observation/robot_state/gripper_position",
        "observation/robot_state/joint_positions",
        "observation/robot_state/joint_torques_computed",
        "observation/robot_state/joint_velocities",
        "observation/robot_state/motor_torques_measured",
        "observation/robot_state/prev_controller_latency_ms",
        "observation/robot_state/prev_joint_torques_computed",
        "observation/robot_state/prev_joint_torques_computed_safened",
    }
)

_INT64_TRAJECTORY_FIELDS: frozenset[str] = frozenset(
    {
        "observation/timestamp/control/control_start",
        "observation/timestamp/control/policy_start",
        "observation/timestamp/control/sleep_start",
        "observation/timestamp/control/step_end",
        "observation/timestamp/control/step_start",
        "observation/timestamp/robot_state/read_end",
        "observation/timestamp/robot_state/read_start",
        "observation/timestamp/robot_state/robot_timestamp_nanos",
        "observation/timestamp/robot_state/robot_timestamp_seconds",
    }
)

_BOOL_TRAJECTORY_FIELDS: frozenset[str] = frozenset(
    {
        "action/robot_state/prev_command_successful",
        "observation/controller_info/controller_on",
        "observation/controller_info/failure",
        "observation/controller_info/movement_enabled",
        "observation/controller_info/success",
        "observation/robot_state/prev_command_successful",
        "observation/timestamp/skip_action",
    }
)

_CAMERA_EXTRINSICS_PATTERN = re.compile(r"^observation/camera_extrinsics/[^/]+_(?:left|right)(?:_gripper_offset)?$")
_CAMERA_TYPE_PATTERN = re.compile(r"^observation/camera_type/[^/]+$")
_CAMERA_TIMESTAMP_PATTERN = re.compile(
    r"^observation/timestamp/cameras/[^/]+_"
    r"(?:estimated_capture|frame_received|read_end|read_start)$"
)

_CAMERA_VIDEO_COLUMNS: dict[str, str] = {
    "wrist": "wrist_cam_video",
    "ext1": "ext1_cam_video",
    "ext2": "ext2_cam_video",
}


def _trajectory_field_dtype(field: str) -> DataType | None:
    if field in _FLOAT64_TRAJECTORY_FIELDS or _CAMERA_EXTRINSICS_PATTERN.match(field):
        return _FLOAT64_TRAJECTORY_DTYPE
    if field in _INT64_TRAJECTORY_FIELDS or _CAMERA_TYPE_PATTERN.match(field) or _CAMERA_TIMESTAMP_PATTERN.match(field):
        return _INT64_TRAJECTORY_DTYPE
    if field in _BOOL_TRAJECTORY_FIELDS:
        return _BOOL_TRAJECTORY_DTYPE
    return None


def _trajectory_return_dtype(fields: Sequence[str]) -> DataType:
    return DataType.struct({field: _trajectory_field_dtype(field) or DataType.python() for field in fields})


def _resolve_cameras(cameras: str | Sequence[str]) -> tuple[str, ...]:
    selected_cameras = (cameras,) if isinstance(cameras, str) else tuple(cameras)
    if len(selected_cameras) == 0:
        raise ValueError("cameras must contain at least one camera")

    unknown = [camera for camera in selected_cameras if camera not in _CAMERA_VIDEO_COLUMNS]
    if unknown:
        known = ", ".join(_CAMERA_VIDEO_COLUMNS)
        raise ValueError(f"Unknown camera(s): {unknown}. Expected one or more of: {known}.")

    return tuple(dict.fromkeys(selected_cameras))


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

    Returns:
        A DataFrame with one row per episode. Metadata fields from each episode's JSON
        file are unnested into top-level columns, along with:

        - `episode_dir`: path to the episode directory
        - `trajectory`: lazy `daft.Hdf5File` reference to the trajectory HDF5 file
        - `wrist_cam_video`: lazy `daft.VideoFile` reference to the wrist camera MP4 file
        - `ext1_cam_video`: lazy `daft.VideoFile` reference to the external camera 1 MP4 file
            Often the left camera feed.
        - `ext2_cam_video`: lazy `daft.VideoFile` reference to the external camera 2 MP4 file
            Often the right camera feed.

    Examples:
        >>> import daft
        >>> df = daft.datasets.droid.raw()  # doctest: +SKIP
        >>> df.select("episode_dir", "ext1_cam_video").show()  # doctest: +SKIP
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

    # Create VideoFile and Hdf5File columns for MP4 camera recordings and trajectory HDF5 file.
    episodes = episodes.with_columns(
        {
            "trajectory": hdf5_file(
                format("{}/trajectory.h5", col("episode_dir")),
                io_config=io_config,
            ),
            "wrist_cam_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("wrist_cam_serial")),
                io_config=io_config,
            ),
            "ext1_cam_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("ext1_cam_serial")),
                io_config=io_config,
            ),
            "ext2_cam_video": video_file(
                format("{}/recordings/MP4/{}.mp4", col("episode_dir"), col("ext2_cam_serial")),
                io_config=io_config,
            ),
        }
    ).with_columns(
        {
            "trajectory": when(file_exists(col("trajectory")), col("trajectory")).otherwise(lit(None)),
            "wrist_cam_video": when(file_exists(col("wrist_cam_video")), col("wrist_cam_video")).otherwise(lit(None)),
            "ext1_cam_video": when(file_exists(col("ext1_cam_video")), col("ext1_cam_video")).otherwise(lit(None)),
            "ext2_cam_video": when(file_exists(col("ext2_cam_video")), col("ext2_cam_video")).otherwise(lit(None)),
        }
    )

    # Sort fields into a stable, grouped order for easier access.
    metadata_cols = [
        col("uuid"),
        col("lab"),
        col("date"),
        col("timestamp"),
        col("scene_id"),
        col("trajectory_length"),
        col("current_task"),
        col("success"),
        col("episode_dir"),
        col("user"),
        col("user_id"),
        col("building"),
        col("robot_serial"),
        col("r2d2_version"),
        col("trajectory"),
    ]

    wrist_cols = [
        col("wrist_cam_serial"),
        col("wrist_cam_extrinsics"),
        col("wrist_cam_video"),
    ]

    ext1_cols = [
        col("ext1_cam_serial"),
        col("ext1_cam_extrinsics"),
        col("ext1_cam_video"),
    ]

    ext2_cols = [
        col("ext2_cam_serial"),
        col("ext2_cam_extrinsics"),
        col("ext2_cam_video"),
    ]

    return episodes.select(
        *metadata_cols,
        *wrist_cols,
        *ext1_cols,
        *ext2_cols,
    )


@PublicAPI
def trajectory(
    episodes: DataFrame,
    fields: Sequence[str] = _DEFAULT_TRAJECTORY_FIELDS,
) -> DataFrame:
    r"""Read selected trajectory datasets from episode-level HDF5 files.

    This helper takes the lazy episode catalog produced by :func:`raw` and adds
    tensor columns for the requested HDF5 datasets. Each output row still
    corresponds to one episode; use filters such as ``limit`` on ``episodes``
    before calling this function to avoid reading more data than needed.

    Args:
        episodes: Episode-level DataFrame from :func:`raw` containing a ``trajectory``
            ``Hdf5File`` column.
        fields: HDF5 dataset paths to read, such as ``"action/joint_position"``.
            Defaults to a curated set of common action and observation fields.

    Returns:
        The input DataFrame with one tensor column per requested field. Rows with
        a null ``trajectory`` file are skipped before reading.

    Examples:
        >>> import daft
        >>> from daft.datasets.droid import raw, trajectory
        >>> episodes = raw().where(daft.col("success")).limit(1)  # doctest: +SKIP
        >>> traj = trajectory(  # doctest: +SKIP
        ...     episodes,
        ...     fields=["action/joint_position", "action/gripper_position"],
        ... )
        >>> traj.select("uuid", "action/joint_position", "action/gripper_position").collect()  # doctest: +SKIP
    """
    from daft.dependencies import h5py

    if not h5py.module_available():  # ty:ignore[unresolved-attribute]
        raise ImportError(
            "The 'daft[hdf5]' extra is required to read DROID HDF5 trajectory files. "
            "Please install it with: pip install 'daft[hdf5]'"
        )

    fields = tuple(fields)
    if "trajectory" not in episodes.schema().column_names():
        raise ValueError("Expected an episode DataFrame with a `trajectory` column.")

    if len(fields) == 0:
        raise ValueError("fields must contain at least one HDF5 dataset path")

    episodes = episodes.where(col("trajectory").not_null())

    @daft.func(return_dtype=_trajectory_return_dtype(fields))
    def read_droid_trajectory(file: Hdf5File) -> dict[str, object]:
        with file.to_tempfile() as tmp, h5py.File(tmp.name, "r") as h5:
            return {field: h5[field][()] for field in fields}

    episodes = episodes.with_column("trajectory", read_droid_trajectory(col("trajectory")))

    return episodes.select(
        "uuid",
        "scene_id",
        "robot_serial",
        "r2d2_version",
        "current_task",
        "success",
        "trajectory_length",
        col("trajectory").unnest(),
        "wrist_cam_video",
        "wrist_cam_extrinsics",
        "ext1_cam_video",
        "ext1_cam_extrinsics",
        "ext2_cam_video",
        "ext2_cam_extrinsics",
    )


@PublicAPI
def camera_frames(
    episodes: DataFrame,
    cameras: str | Sequence[str] = ("wrist", "ext1", "ext2"),
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> DataFrame:
    r"""Decode DROID camera videos into per-episode frame-list columns.

    This helper takes an episode-level DataFrame from :func:`raw` or :func:`trajectory`
    and appends one frame-list column per requested camera. It keeps one row per
    episode; each frame-list column contains the structs returned by
    :func:`daft.functions.video_frames`, including frame metadata and image data.

    Args:
        episodes: Episode-level DataFrame containing DROID camera ``VideoFile`` columns.
        cameras: Camera or cameras to decode. May be a single camera string or a
            sequence of camera names. Supported values are ``"wrist"``, ``"ext1"``,
            and ``"ext2"``. Defaults to all three cameras.
        start_time: Start of the time range in seconds. Defaults to 0.
        end_time: End of the time range in seconds. Defaults to None, meaning all frames.
        width: Target width for resizing frames. Must be provided with ``height``.
        height: Target height for resizing frames. Must be provided with ``width``.
        is_key_frame: If True, decode only keyframes. If False, decode only non-keyframes.
            If None, decode all frames.
        sample_interval_seconds: If provided, sample frames at approximately this time
            interval in seconds.

    Returns:
        The input DataFrame with ``<camera>_cam_frames`` columns appended.

    Examples:
        >>> import daft
        >>> from daft.datasets.droid import camera_frames, raw
        >>> episodes = raw().where(daft.col("success")).limit(1)  # doctest: +SKIP
        >>> frames = camera_frames(episodes, width=224, height=224)  # doctest: +SKIP
        >>> frames.select("uuid", "wrist_cam_frames").collect()  # doctest: +SKIP
    """
    from daft.dependencies import av

    if not av.module_available():  # ty:ignore[unresolved-attribute]
        raise ImportError(
            "The 'daft[video]' extra is required to decode DROID camera frames. "
            "Please install it with: pip install 'daft[video]'"
        )

    selected_cameras = _resolve_cameras(cameras)
    input_columns = {field.name for field in episodes.schema()}
    missing_columns = [
        _CAMERA_VIDEO_COLUMNS[camera]
        for camera in selected_cameras
        if _CAMERA_VIDEO_COLUMNS[camera] not in input_columns
    ]
    if missing_columns:
        raise ValueError(
            f"Expected an episode DataFrame with DROID camera video columns. Missing columns: {missing_columns}."
        )

    frame_columns = {
        f"{camera}_cam_frames": video_frames(
            col(_CAMERA_VIDEO_COLUMNS[camera]),
            start_time=start_time,
            end_time=end_time,
            width=width,
            height=height,
            is_key_frame=is_key_frame,
            sample_interval_seconds=sample_interval_seconds,
        )
        for camera in selected_cameras
    }

    return episodes.with_columns(frame_columns)


__all__ = [
    "camera_frames",
    "raw",
    "trajectory",
]
