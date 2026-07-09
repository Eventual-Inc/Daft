from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import (
    file_exists,
    hdf5_file,
    regexp_replace,
    video_file,
    video_frames,
    when,
)

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.file.hdf5 import Hdf5File
    from daft.io import IOConfig


# The 68 upper-body and hand joints tracked with a per-frame confidence score.
JOINTS: tuple[str, ...] = (
    "hip",
    "leftArm",
    "leftForearm",
    "leftHand",
    "leftIndexFingerIntermediateBase",
    "leftIndexFingerIntermediateTip",
    "leftIndexFingerKnuckle",
    "leftIndexFingerMetacarpal",
    "leftIndexFingerTip",
    "leftLittleFingerIntermediateBase",
    "leftLittleFingerIntermediateTip",
    "leftLittleFingerKnuckle",
    "leftLittleFingerMetacarpal",
    "leftLittleFingerTip",
    "leftMiddleFingerIntermediateBase",
    "leftMiddleFingerIntermediateTip",
    "leftMiddleFingerKnuckle",
    "leftMiddleFingerMetacarpal",
    "leftMiddleFingerTip",
    "leftRingFingerIntermediateBase",
    "leftRingFingerIntermediateTip",
    "leftRingFingerKnuckle",
    "leftRingFingerMetacarpal",
    "leftRingFingerTip",
    "leftShoulder",
    "leftThumbIntermediateBase",
    "leftThumbIntermediateTip",
    "leftThumbKnuckle",
    "leftThumbTip",
    "neck1",
    "neck2",
    "neck3",
    "neck4",
    "rightArm",
    "rightForearm",
    "rightHand",
    "rightIndexFingerIntermediateBase",
    "rightIndexFingerIntermediateTip",
    "rightIndexFingerKnuckle",
    "rightIndexFingerMetacarpal",
    "rightIndexFingerTip",
    "rightLittleFingerIntermediateBase",
    "rightLittleFingerIntermediateTip",
    "rightLittleFingerKnuckle",
    "rightLittleFingerMetacarpal",
    "rightLittleFingerTip",
    "rightMiddleFingerIntermediateBase",
    "rightMiddleFingerIntermediateTip",
    "rightMiddleFingerKnuckle",
    "rightMiddleFingerMetacarpal",
    "rightMiddleFingerTip",
    "rightRingFingerIntermediateBase",
    "rightRingFingerIntermediateTip",
    "rightRingFingerKnuckle",
    "rightRingFingerMetacarpal",
    "rightRingFingerTip",
    "rightShoulder",
    "rightThumbIntermediateBase",
    "rightThumbIntermediateTip",
    "rightThumbKnuckle",
    "rightThumbTip",
    "spine1",
    "spine2",
    "spine3",
    "spine4",
    "spine5",
    "spine6",
    "spine7",
)

# The 69 SE(3) transform datasets: every joint plus the egocentric camera pose.
TRANSFORM_JOINTS: tuple[str, ...] = ("camera", *JOINTS)

# All 138 HDF5 dataset paths present in every EgoDex episode file, in file order.
TRAJECTORY_FIELDS: tuple[str, ...] = (
    "camera/intrinsic",
    *(f"confidences/{joint}" for joint in JOINTS),
    *(f"transforms/{joint}" for joint in TRANSFORM_JOINTS),
)

# Every EgoDex dataset is float32: intrinsics are (3, 3), confidences are (N,),
# and transforms are (N, 4, 4) where N is the episode's frame count.
_TRAJECTORY_DTYPES: dict[str, DataType] = {field: DataType.tensor(DataType.float32()) for field in TRAJECTORY_FIELDS}

_DEFAULT_TRAJECTORY_FIELDS: tuple[str, ...] = (
    "camera/intrinsic",
    "transforms/camera",
    "transforms/leftHand",
    "transforms/rightHand",
    "confidences/leftHand",
    "confidences/rightHand",
)
DEFAULT_TRAJECTORY_FIELDS = _DEFAULT_TRAJECTORY_FIELDS

# Root attributes attached to each episode HDF5 file. Some keys are absent on
# older files; missing attributes surface as nulls in the metadata struct.
_METADATA_FIELD_DTYPES: dict[str, DataType] = {
    "task": DataType.string(),
    "llm_description": DataType.string(),
    "llm_description2": DataType.string(),
    "which_llm_description": DataType.string(),
    "llm_type": DataType.string(),
    "llm_verbs": DataType.list(DataType.string()),
    "llm_objects": DataType.list(DataType.string()),
    "environment": DataType.string(),
    "object": DataType.string(),
    "session_name": DataType.string(),
    "annotated": DataType.bool(),
    "annotator_version": DataType.string(),
    "extra": DataType.string(),
    "description": DataType.string(),
    "description2": DataType.string(),
    "type": DataType.string(),
}

_METADATA_DTYPE = DataType.struct(_METADATA_FIELD_DTYPES)

_METADATA_FIELDS: tuple[str, ...] = tuple(_METADATA_FIELD_DTYPES)

_LIST_METADATA_FIELDS: frozenset[str] = frozenset(("llm_verbs", "llm_objects"))


def _attr_value(value: object) -> object:
    """Coerce h5py attribute values (NumPy scalars/arrays, bytes) to plain Python."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "ndim"):
        array_value = cast("Any", value)
        if array_value.ndim == 0:
            return _attr_value(array_value.item())
        return [_attr_value(item) for item in array_value.tolist()]
    if hasattr(value, "item"):
        scalar_value = cast("Any", value)
        return _attr_value(scalar_value.item())
    return value


def _as_str_list(value: str | Sequence[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _as_int_list(value: int | Sequence[int] | None) -> list[int] | None:
    if value is None:
        return None
    if isinstance(value, int):
        return [value]
    return list(value)


@PublicAPI
def raw(
    path: str,
    io_config: IOConfig | None = None,
    *,
    tasks: str | Sequence[str] | None = None,
    episode_ids: int | Sequence[int] | None = None,
) -> DataFrame:
    r"""Load a local copy of the raw EgoDex dataset as a lazy episode-level DataFrame.

    `EgoDex https://github.com/apple/ml-egodex` is a large-scale egocentric
    dexterous manipulation dataset from Apple, with 829 hours of Apple Vision Pro
    video across 194 tabletop tasks, paired with 3D upper-body and hand pose
    annotations. Each episode is one ``<n>.hdf5`` pose/annotation file plus a
    sibling ``<n>.mp4`` egocentric video, grouped in one directory per task.

    This function discovers episodes by globbing ``*.hdf5`` files under the provided
    dataset root, derives ``task`` and ``episode_id`` from the file path, optionally
    filters by those path-derived fields, reads the episode-level annotation
    attributes, and attaches lazy file references to the trajectory HDF5 file and
    its sibling MP4 video.

    Note:
        The EgoDex dataset is released under CC-BY-NC-ND terms, which prohibit
        redistribution, so Daft cannot host it or default to a public copy — you
        must download it yourself and pass the extracted root path. The archives
        (``part1.zip`` … ``part5.zip`` for train, ``test.zip``, and ``extra.zip``)
        are hosted on Apple's CDN::

            curl -O https://ml-site.cdn-apple.com/datasets/egodex/test.zip

        See https://github.com/apple/ml-egodex for the full download and license
        details. ``path`` may point at the extracted root, a single part, or a
        single task directory; episodes are discovered recursively.

    Args:
        path: Root path of a downloaded EgoDex extraction. Supports local paths
            and remote object stores (for private copies you host yourself).
        io_config: IO configuration for accessing remote storage.
        tasks: Optional task name or names to keep. This filter is applied before
            HDF5 metadata attributes are read.
        episode_ids: Optional episode id or ids to keep. This filter is applied
            before HDF5 metadata attributes are read.

    Returns:
        A DataFrame with one row per episode:

        - ``task``: task name, derived from the episode's parent directory
        - ``episode_id``: integer episode id, derived from the file stem
        - ``metadata``: struct of episode-level HDF5 root attributes (natural
          language descriptions, annotation provenance, etc.)
        - ``trajectory``: lazy ``daft.Hdf5File`` reference to the pose HDF5 file
        - ``video``: lazy ``daft.VideoFile`` reference to the egocentric MP4,
          or null if the sibling video is missing

    Examples:
        >>> import daft
        >>> df = daft.datasets.egodex.raw("/data/egodex")  # doctest: +SKIP
        >>> towels = daft.datasets.egodex.raw("/data/egodex", tasks="fold_towel")  # doctest: +SKIP
        >>> df.select("task", "episode_id", "video").show()  # doctest: +SKIP
    """
    hdf5_glob = f"{path.rstrip('/')}/**/*.hdf5"
    task_values = _as_str_list(tasks)
    episode_id_values = _as_int_list(episode_ids)

    @daft.func(
        return_dtype=_METADATA_DTYPE,
        use_process=False,
    )
    def read_egodex_metadata(file: Hdf5File) -> dict[str, object]:
        attrs = file.attrs()
        values = {name: _attr_value(attrs.get(name)) for name in _METADATA_FIELDS}
        # Daft cannot yet build a struct series when a list-typed field is null on
        # every row of a batch, so missing list attributes become empty lists.
        for name in _LIST_METADATA_FIELDS:
            if values[name] is None:
                values[name] = []
        return values

    episode_paths = daft.from_glob_path(hdf5_glob, io_config=io_config).select(
        "path",
        col("path").split("/")[-2].alias("task"),
        col("path").split("/")[-1].split(".")[0].cast(DataType.int64()).alias("episode_id"),
    )
    if task_values is not None:
        episode_paths = episode_paths.where(col("task").is_in(task_values))
    if episode_id_values is not None:
        episode_paths = episode_paths.where(col("episode_id").is_in(episode_id_values))

    episodes = (
        episode_paths.select(
            "task",
            "episode_id",
            hdf5_file(col("path"), io_config=io_config).alias("trajectory"),
            video_file(
                regexp_replace(col("path"), r"\.hdf5$", ".mp4"),
                io_config=io_config,
            ).alias("video"),
        )
        .with_column(
            "video",
            when(file_exists(col("video")), col("video")).otherwise(lit(None)),
        )
        .with_column("metadata", read_egodex_metadata(col("trajectory")))
    )

    return episodes.select(
        "task",
        "episode_id",
        "metadata",
        "trajectory",
        "video",
    )


@PublicAPI
def trajectory(
    episodes: DataFrame,
    fields: Sequence[str] = _DEFAULT_TRAJECTORY_FIELDS,
) -> DataFrame:
    r"""Read selected pose datasets from episode-level EgoDex HDF5 files.

    This helper takes the lazy episode catalog produced by :func:`raw` and adds
    tensor columns for the requested HDF5 datasets. Each output row still
    corresponds to one episode; use filters such as ``limit`` on ``episodes``
    before calling this function to avoid reading more data than needed.

    Every EgoDex episode file contains 138 float32 datasets (see
    ``daft.datasets.egodex.TRAJECTORY_FIELDS``):

    - ``camera/intrinsic``: the ``(3, 3)`` camera intrinsic matrix
    - ``transforms/<joint>``: ``(N, 4, 4)`` world-frame SE(3) poses per frame,
      for the camera and each of the 68 tracked joints
    - ``confidences/<joint>``: ``(N,)`` per-frame tracking confidence per joint

    Args:
        episodes: Episode-level DataFrame from :func:`raw` containing a
            ``trajectory`` ``Hdf5File`` column.
        fields: HDF5 dataset paths to read, such as ``"transforms/leftHand"``.
            Defaults to a curated set covering the camera intrinsics and pose,
            and both hand poses with their confidences.

    Returns:
        The input DataFrame with one tensor column per requested field. Rows with
        a null ``trajectory`` file are skipped before reading.

    Examples:
        >>> import daft
        >>> from daft.datasets.egodex import raw, trajectory
        >>> episodes = raw("/data/egodex").where(daft.col("task") == "fold_towel").limit(4)  # doctest: +SKIP
        >>> traj = trajectory(  # doctest: +SKIP
        ...     episodes,
        ...     fields=["transforms/leftHand", "transforms/rightHand"],
        ... )
        >>> traj.select("task", "episode_id", "transforms/rightHand").collect()  # doctest: +SKIP
    """
    from daft.dependencies import h5py

    if not h5py.module_available():  # ty:ignore[unresolved-attribute]
        raise ImportError(
            "The 'daft[hdf5]' extra is required to read EgoDex HDF5 trajectory files. "
            "Please install it with: pip install 'daft[hdf5]'"
        )

    # Validation checks
    fields = tuple(fields)
    if "trajectory" not in episodes.schema().column_names():
        raise ValueError("Expected an episode DataFrame with a `trajectory` column.")

    if len(fields) == 0:
        raise ValueError("fields must contain at least one HDF5 dataset path")

    unknown = [f for f in fields if f not in _TRAJECTORY_DTYPES]
    if unknown:
        raise ValueError(f"Unknown trajectory field(s): {unknown}")

    # Build the UDF that will read the trajectory data and return a struct of the requested fields
    @daft.func(
        return_dtype=DataType.struct({field: _TRAJECTORY_DTYPES[field] for field in fields}),
        use_process=False,
        unnest=True,
    )
    def read_egodex_trajectory(file: Hdf5File) -> dict[str, object]:
        with file.to_tempfile() as tmp, h5py.File(tmp.name, "r") as h5:
            return {field: h5[field][()] for field in fields}

    # Select the columns we need and apply the UDF to the trajectory column
    return episodes.where(col("trajectory").not_null()).select(
        "task",
        "episode_id",
        "metadata",
        read_egodex_trajectory(col("trajectory")),
        "video",
    )


@PublicAPI
def camera_frames(
    episodes: DataFrame,
    *,
    start_time: float = 0,
    end_time: float | None = None,
    width: int | None = None,
    height: int | None = None,
    is_key_frame: bool | None = None,
    sample_interval_seconds: float | None = None,
) -> DataFrame:
    r"""Decode EgoDex egocentric videos into a per-episode frame-list column.

    This helper takes an episode-level DataFrame from :func:`raw` or
    :func:`trajectory` and appends a ``video_frames`` column. It keeps one row
    per episode; the frame-list column contains the structs returned by
    :func:`daft.functions.video_frames`, including frame metadata and image data.

    EgoDex videos are 1080p at 30fps, matching the pose annotations frame for
    frame; pass ``sample_interval_seconds`` and/or ``width``/``height`` to keep
    decode volume manageable.

    Args:
        episodes: Episode-level DataFrame containing an EgoDex ``video``
            ``VideoFile`` column.
        start_time: Start of the time range in seconds. Defaults to 0.
        end_time: End of the time range in seconds. Defaults to None, meaning all frames.
        width: Target width for resizing frames. Must be provided with ``height``.
        height: Target height for resizing frames. Must be provided with ``width``.
        is_key_frame: If True, decode only keyframes. If False, decode only non-keyframes.
            If None, decode all frames.
        sample_interval_seconds: If provided, sample frames at approximately this time
            interval in seconds.

    Returns:
        The input DataFrame with a ``video_frames`` column appended.

    Examples:
        >>> import daft
        >>> from daft.datasets.egodex import camera_frames, raw
        >>> episodes = raw("/data/egodex").limit(1)  # doctest: +SKIP
        >>> frames = camera_frames(episodes, width=224, height=224, sample_interval_seconds=1.0)  # doctest: +SKIP
        >>> frames.select("task", "episode_id", "video_frames").collect()  # doctest: +SKIP
    """
    from daft.dependencies import av

    if not cast("Any", av).module_available():
        raise ImportError(
            "The 'daft[video]' extra is required to decode EgoDex video frames. "
            "Please install it with: pip install 'daft[video]'"
        )

    if "video" not in episodes.schema().column_names():
        raise ValueError("Expected an episode DataFrame with an EgoDex `video` column.")

    return episodes.with_column(
        "video_frames",
        video_frames(
            col("video"),
            start_time=start_time,
            end_time=end_time,
            width=width,
            height=height,
            is_key_frame=is_key_frame,
            sample_interval_seconds=sample_interval_seconds,
        ),
    )


__all__ = [
    "DEFAULT_TRAJECTORY_FIELDS",
    "JOINTS",
    "TRAJECTORY_FIELDS",
    "TRANSFORM_JOINTS",
    "camera_frames",
    "raw",
    "trajectory",
]
