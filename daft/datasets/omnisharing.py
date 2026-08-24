"""PX OmniSharing Dataset helpers for `daft.datasets`.

The PX OmniSharing DB is PaXini's omnimodal embodied-AI dataset: synchronized
multi-view video, multi-dimensional tactile sensing, hand proprioception,
object poses, audio and language instructions captured with a force/tactile
exoskeleton glove.

This module reads the **DF-2** stage (and the structurally identical DF-2R
stage) directly from HDF5. The pipeline stages are:

| Stage  | Format               | Filename suffix | Notes                                        |
| ------ | -------------------- | --------------- | -------------------------------------------- |
| DF-1   | HDF5                 | *(none)*        | Raw capture, unparsed encoder/tactile streams |
| DF-2   | HDF5                 | `_glove`        | Parsed, with bimanual + object poses          |
| DF-2R  | HDF5                 | `_{model}`      | Retargeted to a dexterous hand (dh13, mano)   |
| DF-3   | LeRobot Dataset v2.1 | *(none)*        | Use [`daft.datasets.lerobot`][daft.datasets.lerobot] instead |

Dataset home: https://huggingface.co/datasets/paxini/Omnisharing_DB_SampleData

Note:
    The published OmniSharing data is licensed **CC-BY-NC-SA 4.0**
    (non-commercial). The toolkit that produces it lives at
    https://github.com/px-DataCollection/px_omnisharing_dataprocess_kit
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import coalesce, format, regexp_extract, unnest, when
from daft.functions.file_ import hdf5_file

if TYPE_CHECKING:
    from collections.abc import Sequence

    from daft.dataframe import DataFrame
    from daft.file.hdf5 import Hdf5File
    from daft.io import IOConfig


# ---------------------------------------------------------------------------
# Stage / layout constants
# ---------------------------------------------------------------------------

#: Root group that every OmniSharing HDF5 file nests its content under.
ROOT_GROUP = "dataset"

#: Filename suffix that marks the DF-2 ("glove") stage.
DF2_SUFFIX = "glove"

#: Stage identifiers returned in the ``stage`` column.
STAGE_DF1 = "DF-1"
STAGE_DF2 = "DF-2"
STAGE_DF2R = "DF-2R"

STAGES: tuple[str, ...] = (STAGE_DF1, STAGE_DF2, STAGE_DF2R)

#: ``episode_{index}_{HHMMSS}_{room}_{personnel}[_{suffix}].hdf5``
#:
#: The suffix is optional (DF-1 has none), so it is captured separately. Note
#: that ``episode_index`` is **not** unique across a release: it restarts per
#: ``(room_id, personnel_id)`` capture group, which is why :func:`raw` also
#: emits a composite ``episode_key``.
_EPISODE_FILENAME_RE = r"episode_(\d+)_(\d+)_(\d+)_(\d+)(?:_([A-Za-z0-9]+))?\.(?:hdf5|h5)$"

_HF_REPO_ID_RE = re.compile(r"[\w.-]+/[\w.-]+")

#: Hand sides present in both ``action`` and ``observation``.
SIDES: tuple[str, ...] = ("lefthand", "righthand")

#: Top-level branches of an episode.
BRANCHES: tuple[str, ...] = ("observation", "action")

#: Per-hand signals. ``tactile`` only exists under ``observation``.
_SIGNALS: tuple[str, ...] = ("joints", "handpose", "tactile")

#: Quaternion/position layout of ``handpose`` data. Note ``qw`` comes FIRST.
HANDPOSE_ORDER: tuple[str, ...] = ("x", "y", "z", "qw", "qx", "qy", "qz")

#: Matches the optional per-object pose groups ``obj1``, ``obj2``, ...
_OBJECT_GROUP_RE = re.compile(r"obj\d+")


# ---------------------------------------------------------------------------
# HDF5 access helpers
# ---------------------------------------------------------------------------


def _require_h5py() -> Any:
    from daft.dependencies import h5py

    if not h5py.module_available():  # ty:ignore[unresolved-attribute]
        raise ImportError(
            "The 'daft[hdf5]' extra is required to read OmniSharing HDF5 files. "
            "Please install it with: pip install 'daft[hdf5]'"
        )
    return h5py


def _h5path(*parts: str) -> str:
    """Join path fragments under the episode root group."""
    return "/".join((ROOT_GROUP, *(p.strip("/") for p in parts if p)))


def _open_for_scan(file: Hdf5File) -> Any:
    """Open an episode for metadata-only traversal.

    Metadata traversal issues many small reads at scattered offsets, so HDF5's
    own scan buffer size fits better than the large default meant for bulk
    payload reads.

    Note:
        Over high-latency object storage this is not the bottleneck. Measured
        against a 440 MB episode on ``hf://``, walking all 20 camera streams
        took 344s with the 1 KiB scan buffer versus 362s with the 64 KiB
        default: a 5% difference, because the cost is dominated by per-request
        latency rather than bytes transferred. Prefer local or same-region
        storage when scanning many episodes.
    """
    from daft.file.hdf5 import HDF5_SCAN_BUFFER_SIZE

    return file._open_h5py(HDF5_SCAN_BUFFER_SIZE)


def _to_py(value: Any) -> Any:
    """Convert an h5py attribute value into a JSON-serializable Python object."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    # numpy scalar
    if hasattr(value, "item") and getattr(value, "shape", None) == ():
        return _to_py(value.item())
    # numpy array / list
    if hasattr(value, "tolist"):
        return [_to_py(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_to_py(v) for v in value]
    return value


def _attrs_of(node: Any) -> dict[str, Any]:
    return {str(k): _to_py(v) for k, v in node.attrs.items()}


def _get(h5: Any, path: str) -> Any:
    """Return the node at ``path``, or None when it does not exist."""
    try:
        return h5[path]
    except KeyError:
        return None


def _str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(_to_py(v)) for v in value]
    return [str(_to_py(value))]


def _require_episode_column(episodes: DataFrame, column: str = "episode") -> None:
    if column not in episodes.column_names:
        raise ValueError(
            f"Expected an episode DataFrame with an {column!r} column "
            f"(as produced by daft.datasets.omnisharing.raw). "
            f"Got columns: {episodes.column_names}"
        )


def _append_unnested(episodes: DataFrame, udf_expr: Any) -> DataFrame:
    """Append an ``unnest=True`` UDF's struct fields as new columns.

    ``with_column`` cannot be used here: an ``unnest=True`` UDF expands into
    several columns, so it must be spliced into a ``select`` alongside the
    existing columns.
    """
    return episodes.select(*episodes.column_names, udf_expr)


# ---------------------------------------------------------------------------
# describe()
# ---------------------------------------------------------------------------

_OBJECT_DTYPE = DataType.struct(
    {
        "h5path": DataType.string(),
        "kind": DataType.string(),
        "shape": DataType.list(DataType.int64()),
        "dtype": DataType.string(),
        "attrs": DataType.string(),
    }
)


@PublicAPI
def describe(episodes: DataFrame) -> DataFrame:
    r"""Expose the internal HDF5 layout of each episode as a DataFrame.

    OmniSharing episodes are **not** structurally uniform: camera ids are
    non-contiguous, the number of cameras varies by capture rig, ``obj*``
    groups are optional, and tactile/joint widths differ between DF-2 and
    DF-2R. Use this to discover what a release actually contains before
    selecting fields.

    Only HDF5 metadata is streamed (superblock plus B-tree pages), not the
    bulk payloads, so it stays cheap even for multi-gigabyte episodes.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw]. Filter or ``limit`` it
            first; describing every episode in a release is rarely useful.

    Returns:
        DataFrame with one row per HDF5 object per episode:

        - `episode_key`: which episode the object belongs to.
        - `h5path`: full path such as `dataset/observation/lefthand/tactile/data`.
        - `kind`: `"group"` or `"dataset"`.
        - `shape`: dataset dimensions (empty for groups).
        - `dtype`: NumPy dtype string (empty for groups).
        - `attrs`: the object's HDF5 attributes as a JSON string.

    Examples:
        >>> import daft
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> layout = omnisharing.describe(episodes)  # doctest: +SKIP
        >>> layout.where(daft.col("h5path").str.endswith("tactile/data")).show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    h5py = _require_h5py()

    @daft.func(return_dtype=DataType.list(_OBJECT_DTYPE), use_process=False)
    def _walk(file: Hdf5File) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with _open_for_scan(file) as h5:
            root = h5["/"]
            out.append(
                {
                    "h5path": "/",
                    "kind": "group",
                    "shape": [],
                    "dtype": "",
                    "attrs": json.dumps(_attrs_of(root), ensure_ascii=False),
                }
            )

            def visit(name: str, node: Any) -> None:
                is_dataset = isinstance(node, h5py.Dataset)
                out.append(
                    {
                        "h5path": name,
                        "kind": "dataset" if is_dataset else "group",
                        "shape": [int(d) for d in node.shape] if is_dataset else [],
                        "dtype": str(node.dtype) if is_dataset else "",
                        "attrs": json.dumps(_attrs_of(node), ensure_ascii=False),
                    }
                )

            root.visititems(visit)
        return out

    exploded = episodes.select("episode_key", _walk(col("episode")).alias("object")).explode("object")
    return exploded.select("episode_key", unnest(col("object")))


# ---------------------------------------------------------------------------
# episode_metadata()
# ---------------------------------------------------------------------------

_METADATA_DTYPE = DataType.struct(
    {
        "generated_time": DataType.string(),
        "data_id": DataType.string(),
        "vendor": DataType.string(),
        "instruction": DataType.string(),
        "audio_samplerate": DataType.int64(),
        "audio_samples": DataType.int64(),
        "n_frames": DataType.int64(),
        "checked_cam_name": DataType.string(),
        "task_labels": DataType.string(),
        "camera_names": DataType.list(DataType.string()),
        "object_names": DataType.list(DataType.string()),
    }
)


@PublicAPI
def episode_metadata(episodes: DataFrame) -> DataFrame:
    r"""Attach per-episode metadata, task labels and the language instruction.

    Reads the small metadata surface of each episode in a single file open:

    - ``dataset`` attrs: ``generated_time``, ``data_id``
    - ``dataset/meta`` attrs: ``vendor`` plus free-form task labels. Real
      releases use Chinese label keys such as ``任务物品`` (task object) and
      ``物品初始摆放高度`` (initial object height); because these keys vary per
      task they are returned as a JSON string in ``task_labels`` rather than as
      columns.
    - ``dataset/observation/audio`` attrs: ``samplerate`` and ``txt``, where
      ``txt`` is the natural-language instruction for the episode.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].

    Returns:
        The input DataFrame with metadata columns appended: ``generated_time``,
        ``data_id``, ``vendor``, ``instruction``, ``audio_samplerate``,
        ``audio_samples``, ``n_frames``, ``checked_cam_name``, ``task_labels``
        (JSON string), ``camera_names`` and ``object_names``.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(4)  # doctest: +SKIP
        >>> omnisharing.episode_metadata(episodes).select("episode_key", "instruction").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    @daft.func(return_dtype=_METADATA_DTYPE, use_process=False, unnest=True)
    def _read_metadata(file: Hdf5File) -> dict[str, Any]:
        with _open_for_scan(file) as h5:
            root_attrs = _attrs_of(h5[_h5path()]) if _get(h5, _h5path()) is not None else {}

            meta_node = _get(h5, _h5path("meta"))
            meta_attrs = _attrs_of(meta_node) if meta_node is not None else {}
            vendor = meta_attrs.pop("vendor", None)

            audio = _get(h5, _h5path("observation", "audio"))
            audio_attrs = _attrs_of(audio) if audio is not None else {}
            samplerate = audio_attrs.get("samplerate")
            audio_samples = int(audio.shape[0]) if audio is not None and audio.shape else None

            image = _get(h5, _h5path("observation", "image"))
            camera_names = sorted(image.keys()) if image is not None else []
            image_attrs = _attrs_of(image) if image is not None else {}

            observation = _get(h5, _h5path("observation"))
            object_names = (
                sorted(k for k in observation if _OBJECT_GROUP_RE.fullmatch(k)) if observation is not None else []
            )

            aligned = _get(h5, _h5path("observation", "aligned_timestamp"))
            n_frames = int(aligned.shape[0]) if aligned is not None and aligned.shape else None

            return {
                "generated_time": root_attrs.get("generated_time"),
                "data_id": root_attrs.get("data_id"),
                "vendor": vendor,
                "instruction": audio_attrs.get("txt"),
                "audio_samplerate": int(samplerate) if samplerate is not None else None,
                "audio_samples": audio_samples,
                "n_frames": n_frames,
                "checked_cam_name": image_attrs.get("checked_cam_name"),
                "task_labels": json.dumps(meta_attrs, ensure_ascii=False),
                "camera_names": camera_names,
                "object_names": object_names,
            }

    return _append_unnested(episodes, _read_metadata(col("episode")))


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _normalize_dataset_root(uri: str) -> str:
    """Return a canonical dataset root prefix (no trailing slash) for path joins.

    A bare ``org/name`` is interpreted as a Hugging Face dataset repo id, so
    ``"paxini/Omnisharing_DB_SampleData"`` becomes
    ``"hf://datasets/paxini/Omnisharing_DB_SampleData"``.
    """
    u = uri.strip()
    if not u:
        raise ValueError("dataset_uri must be a non-empty string")

    # A bare "org/name" with no scheme and no leading slash is an HF repo id.
    has_scheme = "://" in u
    looks_local = u.startswith(("/", ".", "~"))
    if not has_scheme and not looks_local and _HF_REPO_ID_RE.fullmatch(u):
        u = f"hf://datasets/{u}"
    return u.rstrip("/")


def _episode_globs(root: str) -> list[str]:
    """Globs covering both HDF5 extensions seen in the wild (``.hdf5`` and ``.h5``)."""
    return [f"{root}/**/*.hdf5", f"{root}/**/*.h5"]


# ---------------------------------------------------------------------------
# raw()
# ---------------------------------------------------------------------------


@PublicAPI
def raw(
    dataset_uri: str,
    io_config: IOConfig | None = None,
    stage: str | None = None,
) -> DataFrame:
    r"""Discover OmniSharing episodes as a lazy episode-level DataFrame.

    Globs ``{dataset_uri}/**/*.hdf5``, parses each episode's metadata straight
    out of its filename (no bytes are read), and attaches a lazy
    [`daft.Hdf5File`][daft.Hdf5File] handle per episode. Pass the result to
    [`trajectory`][daft.datasets.omnisharing.trajectory],
    [`tactile`][daft.datasets.omnisharing.tactile] and friends to pull
    modalities out of the files.

    Filter this DataFrame *before* reading modalities: episodes are 0.4-3.2 GB
    each, so limiting by ``room_id``, ``personnel_id`` or ``limit()`` first
    keeps the amount of streamed data small.

    Args:
        dataset_uri: Hugging Face repo id (``paxini/Omnisharing_DB_SampleData``),
            or a local / remote directory (``hf://datasets/...``, ``s3://...``,
            ``/data/omnisharing``).
        io_config: Optional IO configuration for remote reads.
        stage: Optionally keep only one pipeline stage: ``"DF-1"``, ``"DF-2"``
            or ``"DF-2R"``. Defaults to None (keep everything).

    Returns:
        Lazy DataFrame with one row per episode file:

        - `episode_key`: stable composite identity, `"{index}_{time}_{room}_{personnel}"`.
            Use this instead of `episode_index`, which is **not unique**.
        - `episode_index`, `capture_time`, `room_id`, `personnel_id`: parsed from the filename.
        - `stage`: `"DF-1"`, `"DF-2"` or `"DF-2R"`.
        - `hand_model`: retarget target for DF-2R (e.g. `"dh13"`, `"mano"`), else null.
        - `episode`: `Hdf5File` handle for the episode.
        - `path`, `size`: file location and byte size.

    Note:
        Both ``.hdf5`` and ``.h5`` extensions are discovered. If no episode
        files match, collecting the result raises a Daft glob error rather than
        returning an empty DataFrame.

    Examples:
        >>> import daft
        >>> episodes = daft.datasets.omnisharing.raw("paxini/Omnisharing_DB_SampleData")  # doctest: +SKIP
        >>> episodes.select("episode_key", "stage", "size").show()  # doctest: +SKIP
    """
    if stage is not None and stage not in STAGES:
        raise ValueError(f"Unknown stage {stage!r}. Expected one of: {', '.join(STAGES)}.")

    root = _normalize_dataset_root(dataset_uri)

    files = daft.from_glob_path(_episode_globs(root), io_config=io_config)

    # Everything below is pure string manipulation on the globbed paths, so the
    # episode catalog is produced without opening a single HDF5 file.
    parsed = files.select(
        "path",
        "size",
        regexp_extract(col("path"), _EPISODE_FILENAME_RE, 1).alias("episode_index"),
        regexp_extract(col("path"), _EPISODE_FILENAME_RE, 2).alias("capture_time"),
        regexp_extract(col("path"), _EPISODE_FILENAME_RE, 3).alias("room_id"),
        regexp_extract(col("path"), _EPISODE_FILENAME_RE, 4).alias("personnel_id"),
        regexp_extract(col("path"), _EPISODE_FILENAME_RE, 5).alias("suffix"),
    )

    # Drop anything that does not look like an OmniSharing episode file.
    parsed = parsed.where(col("episode_index").not_null() & (col("episode_index") != lit("")))

    suffix = coalesce(col("suffix"), lit(""))
    is_df2 = suffix == lit(DF2_SUFFIX)
    has_suffix = suffix != lit("")

    parsed = parsed.with_columns(
        {
            "stage": (when(is_df2, lit(STAGE_DF2)).when(has_suffix, lit(STAGE_DF2R)).otherwise(lit(STAGE_DF1))),
            # Only DF-2R names a hand model; DF-1/DF-2 leave it null.
            "hand_model": when(is_df2 | ~has_suffix, lit(None)).otherwise(suffix),
            "episode_key": format(
                "{}_{}_{}_{}",
                col("episode_index"),
                col("capture_time"),
                col("room_id"),
                col("personnel_id"),
            ),
            "episode": hdf5_file(col("path"), io_config=io_config),
        }
    )

    if stage is not None:
        parsed = parsed.where(col("stage") == lit(stage))

    return parsed.select(
        "episode_key",
        col("episode_index").cast(DataType.int64()),
        "capture_time",
        col("room_id").cast(DataType.int64()),
        col("personnel_id").cast(DataType.int64()),
        "stage",
        "hand_model",
        "episode",
        "path",
        "size",
    )


# ---------------------------------------------------------------------------
# trajectory()
# ---------------------------------------------------------------------------

#: Signal fields addressable by :func:`trajectory`, in dotted form.
_TRAJECTORY_FIELDS: tuple[str, ...] = tuple(
    f"{branch}.{side}.{signal}" for branch in BRANCHES for side in SIDES for signal in ("joints", "handpose")
)

_DEFAULT_TRAJECTORY_FIELDS: tuple[str, ...] = (
    "observation.lefthand.joints",
    "observation.lefthand.handpose",
    "observation.righthand.joints",
    "observation.righthand.handpose",
    "action.lefthand.joints",
    "action.lefthand.handpose",
    "action.righthand.joints",
    "action.righthand.handpose",
)


def _field_to_h5path(field: str) -> str:
    branch, side, signal = field.split(".")
    return _h5path(branch, side, signal, "data")


@PublicAPI
def trajectory(
    episodes: DataFrame,
    fields: Sequence[str] = _DEFAULT_TRAJECTORY_FIELDS,
    *,
    include_attrs: bool = True,
) -> DataFrame:
    r"""Read hand joint angles and 6-DoF hand poses as tensor columns.

    Each requested field becomes one ``Tensor[Float32]`` column holding the
    whole episode, shaped ``(n_frames, width)``:

    - ``joints`` is ``(n, 29)`` in DF-2 (exoskeleton glove) and ``(n, 17)`` in
      DF-2R (retargeted dexterous hand). The per-column meaning is given by the
      ``joint_names`` attribute, not by position, so read it rather than
      assuming an order.
    - ``handpose`` is ``(n, 7)`` laid out as ``[x, y, z, qw, qx, qy, qz]``.

    Warning:
        The quaternion is **``qw`` first**, not ``qw`` last. Libraries such as
        SciPy's ``Rotation.from_quat`` expect ``[qx, qy, qz, qw]`` and will
        silently produce wrong rotations if fed this layout directly.

    Note:
        ``action`` leads ``observation`` by one frame (``action[i]`` is the state
        at ``i + 1``, with the final frame repeated). Use
        [`frames`][daft.datasets.omnisharing.frames] if you need that alignment
        materialized per row.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw]. Filter it first: every
            episode read here streams tensor data.
        fields: Dotted field names of the form ``"{branch}.{side}.{signal}"``,
            where branch is ``observation``/``action``, side is
            ``lefthand``/``righthand`` and signal is ``joints``/``handpose``.
            Defaults to all eight combinations.
        include_attrs: If True (default), also emit ``joint_names`` for joint
            fields and ``handpose_order``/``handpose_detail`` describing the
            pose layout, plus each hand's ``description``.

    Returns:
        The input DataFrame with one tensor column per requested field, named
        after the dotted field name.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> traj = omnisharing.trajectory(  # doctest: +SKIP
        ...     episodes, fields=["observation.lefthand.joints"]
        ... )
        >>> traj.select("episode_key", "observation.lefthand.joints").collect()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    fields = tuple(fields)
    if not fields:
        raise ValueError("fields must contain at least one field name")

    unknown = [f for f in fields if f not in _TRAJECTORY_FIELDS]
    if unknown:
        raise ValueError(f"Unknown trajectory field(s): {unknown}. Valid fields are: {', '.join(_TRAJECTORY_FIELDS)}")

    return_fields: dict[str, DataType] = {f: DataType.tensor(DataType.float32()) for f in fields}
    if include_attrs:
        sides_used = sorted({f.split(".")[1] for f in fields})
        for f in fields:
            if f.endswith(".joints"):
                return_fields[f"{f}.joint_names"] = DataType.list(DataType.string())
        for side in sides_used:
            return_fields[f"{side}.description"] = DataType.string()
        if any(f.endswith(".handpose") for f in fields):
            return_fields["handpose_order"] = DataType.string()
            return_fields["handpose_detail"] = DataType.string()

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_trajectory(file: Hdf5File) -> dict[str, Any]:
        out: dict[str, Any] = {}
        with file._open_h5py() as h5:
            for f in fields:
                node = _get(h5, _field_to_h5path(f))
                out[f] = None if node is None else node[()]
                if include_attrs and f.endswith(".joints"):
                    attrs = _attrs_of(node) if node is not None else {}
                    out[f"{f}.joint_names"] = _str_list(attrs.get("joint_names"))
            if include_attrs:
                for side in sorted({f.split(".")[1] for f in fields}):
                    # The hand description is identical under both branches, so
                    # take whichever one is present rather than depending on
                    # which branch happened to be requested first.
                    hand = _get(h5, _h5path("observation", side)) or _get(h5, _h5path("action", side))
                    out[f"{side}.description"] = _attrs_of(hand).get("description") if hand is not None else None
                if any(f.endswith(".handpose") for f in fields):
                    first = next(f for f in fields if f.endswith(".handpose"))
                    branch, side, _ = first.split(".")
                    pose = _get(h5, _h5path(branch, side, "handpose"))
                    pose_attrs = _attrs_of(pose) if pose is not None else {}
                    out["handpose_order"] = pose_attrs.get("order")
                    out["handpose_detail"] = pose_attrs.get("detail")
        return out

    return _append_unnested(episodes, _read_trajectory(col("episode")))


# ---------------------------------------------------------------------------
# tactile()
# ---------------------------------------------------------------------------


@PublicAPI
def tactile(
    episodes: DataFrame,
    sides: str | Sequence[str] = SIDES,
    *,
    split_by_sensor: bool = False,
) -> DataFrame:
    r"""Read multi-dimensional tactile readings for one or both hands.

    Tactile is the distinguishing modality of this dataset: each frame is a flat
    vector concatenating every sensor pad on the hand. In DF-2 that vector is
    ``3465`` wide, split across 15 sensors whose widths are given by the
    ``sensor_lengths`` attribute (palm plus per-finger pads). DF-2R uses a
    different total (``3750``), so widths are always read from the file rather
    than assumed.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        sides: Hand or hands to read: ``"lefthand"``, ``"righthand"`` or both.
            Defaults to both.
        split_by_sensor: If True, slice the flat vector into one tensor column
            per sensor using the cumulative ``sensor_lengths`` offsets, named
            ``"{side}.tactile.{sensor_name}"``. Defaults to False, which returns
            the raw ``(n, total_width)`` tensor.

    Returns:
        The input DataFrame with, per requested side:

        - `"{side}.tactile"`: `Tensor[Float32]` of shape `(n, total_width)`
          (omitted when `split_by_sensor=True`).
        - `"{side}.tactile.sensor_names"` and `"{side}.tactile.sensor_lengths"`.
        - One `"{side}.tactile.{sensor}"` tensor column per sensor when
          `split_by_sensor=True`.

    Raises:
        ValueError: If ``sensor_lengths`` does not sum to the stored vector
            width, which would make any per-sensor slicing silently wrong.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> tac = omnisharing.tactile(episodes, sides="lefthand")  # doctest: +SKIP
        >>> tac.select("episode_key", "lefthand.tactile.sensor_names").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    selected = (sides,) if isinstance(sides, str) else tuple(sides)
    if not selected:
        raise ValueError("sides must contain at least one hand")
    unknown = [s for s in selected if s not in SIDES]
    if unknown:
        raise ValueError(f"Unknown side(s): {unknown}. Expected one or more of: {', '.join(SIDES)}.")
    selected = tuple(dict.fromkeys(selected))

    sensor_layout = _probe_sensor_layout(episodes, selected) if split_by_sensor else {}

    return_fields: dict[str, DataType] = {}
    for side in selected:
        if split_by_sensor:
            for sensor in sensor_layout.get(side, []):
                return_fields[f"{side}.tactile.{sensor}"] = DataType.tensor(DataType.float32())
        else:
            return_fields[f"{side}.tactile"] = DataType.tensor(DataType.float32())
        return_fields[f"{side}.tactile.sensor_names"] = DataType.list(DataType.string())
        return_fields[f"{side}.tactile.sensor_lengths"] = DataType.list(DataType.int64())

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_tactile(file: Hdf5File) -> dict[str, Any]:
        out: dict[str, Any] = dict.fromkeys(return_fields)
        with file._open_h5py() as h5:
            for side in selected:
                node = _get(h5, _h5path("observation", side, "tactile", "data"))
                if node is None:
                    continue
                attrs = _attrs_of(node)
                names = _str_list(attrs.get("sensor_names"))
                lengths = [int(v) for v in (attrs.get("sensor_lengths") or [])]
                out[f"{side}.tactile.sensor_names"] = names
                out[f"{side}.tactile.sensor_lengths"] = lengths

                if not split_by_sensor:
                    out[f"{side}.tactile"] = node[()]
                    continue

                width = int(node.shape[-1])
                if sum(lengths) != width:
                    raise ValueError(
                        f"{side} tactile sensor_lengths sum to {sum(lengths)} but the stored "
                        f"vector is {width} wide; per-sensor slicing would be incorrect."
                    )
                data = node[()]
                offset = 0
                for name, length in zip(names, lengths):
                    key = f"{side}.tactile.{name}"
                    if key in out:
                        out[key] = data[:, offset : offset + length]
                    offset += length
        return out

    return _append_unnested(episodes, _read_tactile(col("episode")))


def _probe_sensor_layout(episodes: DataFrame, sides: tuple[str, ...]) -> dict[str, list[str]]:
    """Read sensor names from one episode so the output schema can be fixed up front.

    Daft resolves a plan's schema before execution, so per-sensor column names
    must be known at planning time. One episode is opened for metadata only.
    """
    _require_h5py()

    @daft.func(return_dtype=DataType.string(), use_process=False)
    def _sensor_names_json(file: Hdf5File) -> str:
        layout: dict[str, list[str]] = {}
        with _open_for_scan(file) as h5:
            for side in sides:
                node = _get(h5, _h5path("observation", side, "tactile", "data"))
                attrs = _attrs_of(node) if node is not None else {}
                layout[side] = _str_list(attrs.get("sensor_names"))
        return json.dumps(layout, ensure_ascii=False)

    rows = episodes.limit(1).select(_sensor_names_json(col("episode")).alias("layout")).to_pylist()
    if not rows or not rows[0]["layout"]:
        raise ValueError(
            "Could not probe tactile sensor names: the episode DataFrame is empty. "
            "split_by_sensor=True needs at least one readable episode."
        )
    layout: dict[str, list[str]] = json.loads(rows[0]["layout"])
    missing = [s for s in sides if not layout.get(s)]
    if missing:
        raise ValueError(
            f"Episode has no tactile sensor_names for side(s) {missing}; "
            f"cannot use split_by_sensor=True. Note tactile only exists under 'observation'."
        )
    return layout


# ---------------------------------------------------------------------------
# audio()
# ---------------------------------------------------------------------------

_AUDIO_DTYPE = DataType.struct(
    {
        "waveform": DataType.tensor(DataType.float64()),
        "samplerate": DataType.int64(),
        "n_samples": DataType.int64(),
    }
)


@PublicAPI
def audio(
    episodes: DataFrame,
    *,
    mono: bool = False,
    max_seconds: float | None = None,
) -> DataFrame:
    r"""Read the recorded audio waveform for each episode.

    Note:
        The published layout describes this as a "compressed audio stream
        (includes text)". It is neither compressed nor text-bearing: the samples
        are stored as raw ``float64`` PCM, and the spoken instruction lives in a
        ``txt`` attribute that
        [`episode_metadata`][daft.datasets.omnisharing.episode_metadata] surfaces
        as ``instruction``. No audio codec is needed to read it.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        mono: If True, average across channels to produce a 1-D waveform.
            Defaults to False, which preserves the stored ``(samples, channels)``
            shape.
        max_seconds: Optional limit, in seconds, on how much audio to read.
            Applied as ``max_seconds * samplerate`` samples. Useful for
            previewing without pulling whole recordings across the network.
            Ignored when the episode has no ``samplerate`` attribute, since the
            sample count cannot be derived without it.

    Returns:
        The input DataFrame with three columns appended:

        - `"waveform"`: `Tensor[Float64]`, shape `(samples, channels)` or
          `(samples,)` when `mono=True`.
        - `"samplerate"`: samples per second, from the `samplerate` attribute.
        - `"n_samples"`: number of samples actually read, which reflects
          `max_seconds` when it is set.

        Episodes without an ``audio`` dataset get an empty waveform and null
        metadata rather than an error, so a mixed release still reads cleanly.

    Raises:
        ValueError: If ``max_seconds`` is not positive.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> clip = omnisharing.audio(episodes, mono=True, max_seconds=1.0)  # doctest: +SKIP
        >>> clip.select("episode_key", "samplerate", "n_samples").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    if max_seconds is not None and max_seconds <= 0:
        raise ValueError(f"max_seconds must be positive, got {max_seconds}")

    @daft.func(return_dtype=_AUDIO_DTYPE, use_process=False, unnest=True)
    def _read_audio(file: Hdf5File) -> dict[str, Any]:
        from daft.dependencies import np

        # An all-null tensor column trips a Daft concat error, so absent audio is
        # reported as an empty waveform with null metadata instead of null.
        empty = {
            "waveform": np.empty((0,), dtype="float64"),
            "samplerate": None,
            "n_samples": None,
        }
        with file._open_h5py() as h5:
            node = _get(h5, _h5path("observation", "audio"))
            if node is None:
                return empty

            attrs = _attrs_of(node)
            samplerate = attrs.get("samplerate")
            samplerate = int(samplerate) if samplerate is not None else None

            limit = None
            if max_seconds is not None and samplerate:
                limit = max(1, int(max_seconds * samplerate))

            # Slice inside the read so truncation never pulls the whole stream.
            samples = np.asarray(node[:limit] if limit is not None else node[()])
            if samples.size == 0:
                return {**empty, "samplerate": samplerate, "n_samples": 0}

            if mono and samples.ndim > 1:
                samples = samples.mean(axis=tuple(range(1, samples.ndim)))

            return {
                "waveform": samples,
                "samplerate": samplerate,
                "n_samples": int(samples.shape[0]),
            }

    return _append_unnested(episodes, _read_audio(col("episode")))


# ---------------------------------------------------------------------------
# objects()
# ---------------------------------------------------------------------------

#: Width of each ``obj*/data`` row: a 4x4 transform plus one trailing value.
OBJECT_POSE_WIDTH = 17


@PublicAPI
def objects(
    episodes: DataFrame,
    *,
    max_objects: int | None = None,
) -> DataFrame:
    r"""Read per-object 6D pose tracks, when the episode has any.

    Object poses come from the toolkit's optional pose-estimation stage, so many
    episodes have no ``obj*`` groups at all. Because Daft fixes a plan's schema
    before execution, the number of object columns cannot vary per row: this
    probes the episodes to find the largest object count, emits that many
    columns, and leaves the surplus null for episodes with fewer objects.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        max_objects: Number of ``obj*`` column groups to emit. Defaults to None,
            which probes the episodes for the maximum present. Pass an explicit
            value to skip the probe or to pad the schema for a wider release.

    Returns:
        The input DataFrame with, for each object slot ``i`` in ``1..n``:

        - `"obj{i}"`: `Tensor[Float32]` of shape `(n_frames, 17)`, a flattened
          4x4 transform plus a trailing element described by the `order` attr.
        - `"obj{i}.name"`, `"obj{i}.id"`, `"obj{i}.order"`: identity and layout
          metadata read from the dataset's attributes.

        All four are null for episodes that do not have that object slot. When no
        episode has any objects, only ``n_objects`` is added.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(4)  # doctest: +SKIP
        >>> omnisharing.objects(episodes).select("episode_key", "n_objects", "obj1.name").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    if max_objects is not None and max_objects < 0:
        raise ValueError(f"max_objects must be non-negative, got {max_objects}")

    slots = _probe_object_count(episodes) if max_objects is None else max_objects

    return_fields: dict[str, DataType] = {"n_objects": DataType.int64()}
    for i in range(1, slots + 1):
        return_fields[f"obj{i}"] = DataType.tensor(DataType.float32())
        return_fields[f"obj{i}.name"] = DataType.string()
        return_fields[f"obj{i}.id"] = DataType.int64()
        return_fields[f"obj{i}.order"] = DataType.string()

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_objects(file: Hdf5File) -> dict[str, Any]:
        out: dict[str, Any] = dict.fromkeys(return_fields)
        out["n_objects"] = 0
        with file._open_h5py() as h5:
            observation = _get(h5, _h5path("observation"))
            if observation is None:
                return out
            names = sorted(
                (k for k in observation if _OBJECT_GROUP_RE.fullmatch(k)),
                key=lambda k: int(k[3:]),
            )
            out["n_objects"] = len(names)
            for group_name in names:
                node = _get(h5, _h5path("observation", group_name, "data"))
                if node is None or f"{group_name}" not in return_fields:
                    continue
                attrs = _attrs_of(node)
                out[group_name] = node[()]
                out[f"{group_name}.name"] = attrs.get("obj_name")
                obj_id = attrs.get("obj_id")
                out[f"{group_name}.id"] = int(obj_id) if obj_id is not None else None
                out[f"{group_name}.order"] = attrs.get("order") or attrs.get("detail")
        return out

    return _append_unnested(episodes, _read_objects(col("episode")))


def _probe_object_count(episodes: DataFrame) -> int:
    """Find the largest ``obj*`` count so the output schema can be fixed up front."""

    @daft.func(return_dtype=DataType.int64(), use_process=False)
    def _count(file: Hdf5File) -> int:
        with _open_for_scan(file) as h5:
            observation = _get(h5, _h5path("observation"))
            if observation is None:
                return 0
            return sum(1 for k in observation if _OBJECT_GROUP_RE.fullmatch(k))

    rows = episodes.select(_count(col("episode")).alias("n")).to_pylist()
    return max((r["n"] or 0) for r in rows) if rows else 0


# ---------------------------------------------------------------------------
# cameras()
# ---------------------------------------------------------------------------

#: RGBD cameras store three separate encoded streams under the camera group.
RGBD_STREAMS: tuple[str, ...] = ("color", "left", "right")

#: Recognized codecs, keyed by the magic bytes at the head of a payload.
_CODEC_MAGICS: tuple[tuple[bytes, str], ...] = (
    (b"\x1a\x45\xdf\xa3", "matroska"),
    (b"\xff\xd8\xff", "jpeg"),
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"RIFF", "riff"),
    (b"\x00\x00\x00\x01", "h26x-annexb"),
    (b"\x00\x00\x01", "h26x-annexb"),
)


def _sniff_codec(head: bytes) -> str:
    """Identify a payload's container/codec from its leading bytes.

    OmniSharing's ``data`` payloads are whole encoded streams, not per-frame
    blobs: ``RGB_Camera*`` holds H.265/HEVC in Annex-B byte-stream format and
    the ``RGBD_*`` sub-streams hold Matroska. The official data_structure.md
    only calls these a "1D compressed payload", so detection is done from the
    bytes rather than trusting a declared format.
    """
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return "mp4"
    for magic, name in _CODEC_MAGICS:
        if head.startswith(magic):
            return name
    return "unknown"


_CAMERA_DTYPE = DataType.struct(
    {
        "camera": DataType.string(),
        "kind": DataType.string(),
        "stream": DataType.string(),
        "h5path": DataType.string(),
        "codec": DataType.string(),
        "payload_bytes": DataType.int64(),
        "n_timestamps": DataType.int64(),
        "width": DataType.int64(),
        "height": DataType.int64(),
        "intrinsics": DataType.tensor(DataType.float32()),
        "extrinsics": DataType.tensor(DataType.float64()),
        "distortion": DataType.string(),
        "relative_to_who": DataType.string(),
        "calib_date": DataType.string(),
        "inner_extrinsic": DataType.string(),
        "is_checked_camera": DataType.bool(),
    }
)


@PublicAPI
def cameras(episodes: DataFrame) -> DataFrame:
    r"""Enumerate each episode's cameras with calibration and codec details.

    Emits one row per encoded video stream: one per ``RGB_Camera*`` and three
    per ``RGBD_*`` (``color``, ``left``, ``right``). Camera ids are
    **non-contiguous** in real captures, and the two families use different
    reference frames, so both are reported rather than inferred:

    - ``RGB_Camera*`` extrinsics are relative to another RGB camera.
    - ``RGBD_*`` extrinsics are relative to ``RGBD_0``.

    Only headers, calibration matrices and the first bytes of each payload are
    read, so this is cheap relative to decoding.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].

    Returns:
        DataFrame with one row per camera stream:

        - `episode_key`, `camera` (e.g. `"RGB_Camera4"`), `kind` (`"rgb"`/`"rgbd"`),
          `stream` (`""` for RGB, else `"color"`/`"left"`/`"right"`), `h5path`.
        - `codec`: detected from magic bytes, e.g. `"h26x-annexb"` for RGB and
          `"matroska"` for RGBD streams.
        - `payload_bytes`, `n_timestamps`, `width`, `height`.
        - `intrinsics` (3x3), `extrinsics` (4x4), `distortion`, `relative_to_who`,
          `calib_date`.
        - `inner_extrinsic`: RGBD-only JSON string with the stereo-to-color transform.
        - `is_checked_camera`: True when this camera matches the episode's
          `checked_cam_name` attribute.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> omnisharing.cameras(episodes).select("camera", "kind", "codec", "width", "height").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    @daft.func(return_dtype=DataType.list(_CAMERA_DTYPE), use_process=False)
    def _read_cameras(file: Hdf5File) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with _open_for_scan(file) as h5:
            image = _get(h5, _h5path("observation", "image"))
            if image is None:
                return rows
            checked = _attrs_of(image).get("checked_cam_name") or ""

            for camera in sorted(image):
                group = image[camera]
                is_rgbd = camera.upper().startswith("RGBD")
                inner = _get(group, "inner_extrinsic")
                inner_json = None
                if inner is not None:
                    raw_inner = inner[()]
                    blob = raw_inner[0] if getattr(raw_inner, "shape", ()) else raw_inner
                    inner_json = blob.decode("utf-8", "replace") if isinstance(blob, bytes) else str(blob)

                streams = RGBD_STREAMS if is_rgbd else ("",)
                for stream in streams:
                    holder = _get(group, stream) if stream else group
                    if holder is None:
                        continue
                    payload = _get(holder, "data")
                    if payload is None:
                        continue

                    head = bytes(payload[: min(16, int(payload.shape[0]))])
                    intr = _get(holder, "intrinsics")
                    intr_attrs = _attrs_of(intr) if intr is not None else {}
                    # RGB keeps extrinsics on the camera group. RGBD stores a
                    # single camera-level extrinsic under the `color` stream, so
                    # `left`/`right` fall back to it: all three share one pose,
                    # with the stereo offsets living in `inner_extrinsic`.
                    ext = (
                        _get(holder, "extrinsics")
                        or _get(group, "extrinsics")
                        or (_get(group, "color/extrinsics") if is_rgbd else None)
                    )
                    ext_attrs = _attrs_of(ext) if ext is not None else {}
                    ts = _get(group, f"{stream}_timestamp" if stream else "timestamp") or _get(group, "timestamp")

                    rows.append(
                        {
                            "camera": camera,
                            "kind": "rgbd" if is_rgbd else "rgb",
                            "stream": stream,
                            "h5path": f"{_h5path('observation', 'image', camera)}" + (f"/{stream}" if stream else ""),
                            "codec": _sniff_codec(head),
                            "payload_bytes": int(payload.shape[0]),
                            "n_timestamps": int(ts.shape[0]) if ts is not None and ts.shape else None,
                            "width": _opt_int(intr_attrs.get("width")),
                            "height": _opt_int(intr_attrs.get("height")),
                            "intrinsics": None if intr is None else intr[()],
                            "extrinsics": None if ext is None else ext[()],
                            "distortion": intr_attrs.get("distortion"),
                            "relative_to_who": ext_attrs.get("relative_to_who"),
                            "calib_date": ext_attrs.get("calib_date"),
                            "inner_extrinsic": inner_json,
                            "is_checked_camera": bool(checked) and checked in camera,
                        }
                    )
        return rows

    exploded = episodes.select("episode_key", _read_cameras(col("episode")).alias("cam")).explode("cam")
    return exploded.select("episode_key", unnest(col("cam")))


def _opt_int(value: Any) -> int | None:
    return None if value is None else int(value)


# ---------------------------------------------------------------------------
# camera_payloads() / camera_frames()
# ---------------------------------------------------------------------------

#: Codecs that PyAV can open straight from an in-memory buffer.
_DECODABLE_CODECS: frozenset[str] = frozenset({"h26x-annexb", "matroska", "mp4"})

#: Explicit demuxer/format hint per codec. Annex-B byte streams carry no
#: container, so PyAV must be told which elementary stream it is looking at.
_AV_FORMAT_HINT: dict[str, str] = {
    "h26x-annexb": "hevc",
    "matroska": "matroska",
    "mp4": "mp4",
}

#: Safety cap on frames decoded per stream, mirroring the LeRobot reader's budget.
_DECODE_FRAME_BUDGET = 20_000


def _camera_h5paths(camera: str, stream: str = "") -> str:
    parts = ["observation", "image", camera]
    if stream:
        parts.append(stream)
    parts.append("data")
    return _h5path(*parts)


@PublicAPI
def camera_payloads(
    episodes: DataFrame,
    cameras_: Sequence[tuple[str, str]] | Sequence[str],
) -> DataFrame:
    r"""Read raw encoded video payloads for specific camera streams.

    This is the always-available path: it hands back the encoded bytes plus the
    detected codec without needing PyAV or any codec support, so it works even
    for payloads this module cannot decode. Use
    [`camera_frames`][daft.datasets.omnisharing.camera_frames] when you want
    decoded images instead.

    Warning:
        A single stream is tens of megabytes. Select only the cameras you need
        and filter ``episodes`` first.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        cameras_: Streams to read. Either camera names (``"RGB_Camera0"``) or
            ``(camera, stream)`` pairs for RGBD sub-streams
            (``("RGBD_0", "color")``).

    Returns:
        The input DataFrame with, per requested stream:

        - `"{name}.payload"`: the encoded bytes.
        - `"{name}.codec"`: detected codec, e.g. `"h26x-annexb"` or `"matroska"`.

        where `name` is the camera, suffixed with `".{stream}"` for RGBD
        sub-streams. Missing streams are null.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> payloads = omnisharing.camera_payloads(episodes, ["RGB_Camera0"])  # doctest: +SKIP
        >>> payloads.select("RGB_Camera0.codec").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    targets = _normalize_camera_targets(cameras_)

    return_fields: dict[str, DataType] = {}
    for camera, stream in targets:
        name = _stream_label(camera, stream)
        return_fields[f"{name}.payload"] = DataType.binary()
        return_fields[f"{name}.codec"] = DataType.string()

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_payloads(file: Hdf5File) -> dict[str, Any]:
        out: dict[str, Any] = dict.fromkeys(return_fields)
        with file._open_h5py() as h5:
            for camera, stream in targets:
                node = _get(h5, _camera_h5paths(camera, stream))
                if node is None:
                    continue
                name = _stream_label(camera, stream)
                blob = node[()].tobytes()
                out[f"{name}.payload"] = blob
                out[f"{name}.codec"] = _sniff_codec(blob[:16])
        return out

    return _append_unnested(episodes, _read_payloads(col("episode")))


@PublicAPI
def camera_frames(
    episodes: DataFrame,
    cameras_: Sequence[tuple[str, str]] | Sequence[str],
    *,
    max_frames: int | None = 1,
    sample_every: int = 1,
    width: int | None = None,
    height: int | None = None,
) -> DataFrame:
    r"""Decode camera payloads into image columns.

    Payloads are decoded from memory with PyAV. ``RGB_Camera*`` streams are
    H.265/HEVC Annex-B elementary streams, which carry no container, so the
    decoder is told the format explicitly. ``RGBD_*`` sub-streams are Matroska
    and are demuxed normally.

    Warning:
        Decoding is expensive and each stream holds the whole episode. The
        default ``max_frames=1`` decodes a single frame per stream, which is
        enough for previews and thumbnails. Raise it deliberately.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        cameras_: Streams to decode. Either camera names (``"RGB_Camera0"``) or
            ``(camera, stream)`` pairs (``("RGBD_0", "color")``).
        max_frames: Maximum frames to decode per stream. Defaults to 1. Pass
            None to decode everything, bounded by an internal safety budget.
        sample_every: Keep every Nth decoded frame. Defaults to 1 (keep all).
        width: Target width; must be given together with ``height``.
        height: Target height; must be given together with ``width``.

    Returns:
        The input DataFrame with one ``"{name}.frames"`` list-of-image column
        per requested stream, and ``"{name}.codec"`` alongside it. Streams whose
        codec is not decodable yield null frames; use
        [`camera_payloads`][daft.datasets.omnisharing.camera_payloads] to get
        their raw bytes instead.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> frames = omnisharing.camera_frames(episodes, ["RGB_Camera0"], max_frames=2)  # doctest: +SKIP
        >>> frames.select("RGB_Camera0.frames").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    from daft.dependencies import av

    if not av.module_available():  # ty:ignore[unresolved-attribute]
        raise ImportError(
            "Decoding OmniSharing camera payloads requires PyAV. Install it with: pip install 'daft[video]'"
        )

    targets = _normalize_camera_targets(cameras_)

    if (width is None) != (height is None):
        raise ValueError("width and height must be provided together")
    if sample_every < 1:
        raise ValueError(f"sample_every must be >= 1, got {sample_every}")
    if max_frames is not None and max_frames < 1:
        raise ValueError(f"max_frames must be >= 1 or None, got {max_frames}")

    return_fields: dict[str, DataType] = {}
    for camera, stream in targets:
        name = _stream_label(camera, stream)
        return_fields[f"{name}.frames"] = DataType.list(DataType.image())
        return_fields[f"{name}.codec"] = DataType.string()

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _decode(file: Hdf5File) -> dict[str, Any]:
        out: dict[str, Any] = dict.fromkeys(return_fields)
        with file._open_h5py() as h5:
            for camera, stream in targets:
                node = _get(h5, _camera_h5paths(camera, stream))
                if node is None:
                    continue
                name = _stream_label(camera, stream)
                blob = node[()].tobytes()
                codec = _sniff_codec(blob[:16])
                out[f"{name}.codec"] = codec
                if codec not in _DECODABLE_CODECS:
                    continue
                out[f"{name}.frames"] = _decode_payload(
                    blob,
                    codec,
                    max_frames=max_frames,
                    sample_every=sample_every,
                    width=width,
                    height=height,
                )
        return out

    return _append_unnested(episodes, _decode(col("episode")))


def _decode_payload(
    blob: bytes,
    codec: str,
    *,
    max_frames: int | None,
    sample_every: int,
    width: int | None,
    height: int | None,
) -> list[Any]:
    """Decode an in-memory encoded stream into a list of RGB ndarrays."""
    import io

    from daft.dependencies import av, np, pil_image

    frames: list[Any] = []
    budget = _DECODE_FRAME_BUDGET if max_frames is None else min(max_frames * sample_every, _DECODE_FRAME_BUDGET)

    buffer = io.BytesIO(blob)
    open_kwargs = {"format": _AV_FORMAT_HINT[codec]} if codec in _AV_FORMAT_HINT else {}

    with av.open(buffer, "r", **open_kwargs) as container:
        if not container.streams.video:
            return frames
        video = container.streams.video[0]
        seen = 0
        for seen, frame in enumerate(container.decode(video)):
            if seen % sample_every == 0:
                array = frame.to_ndarray(format="rgb24")
                if width is not None and height is not None:
                    if not pil_image.module_available():  # ty:ignore[unresolved-attribute]
                        raise ImportError(
                            "Resizing decoded frames requires Pillow. Install it with: pip install 'daft[video]'"
                        )
                    img = pil_image.fromarray(array, mode="RGB").resize((width, height), pil_image.Resampling.BILINEAR)
                    array = np.asarray(img)
                frames.append(array)
            if max_frames is not None and len(frames) >= max_frames:
                break
            if seen + 1 >= budget:
                break
    return frames


def _normalize_camera_targets(
    cameras_: Sequence[tuple[str, str]] | Sequence[str],
) -> tuple[tuple[str, str], ...]:
    """Normalize camera selectors into ``(camera, stream)`` pairs.

    Naming an RGBD camera without a stream expands to all of its sub-streams,
    since an RGBD group has no payload of its own.
    """
    if isinstance(cameras_, str):
        cameras_ = [cameras_]
    if not cameras_:
        raise ValueError("cameras_ must name at least one camera stream")

    targets: list[tuple[str, str]] = []
    for entry in cameras_:
        if isinstance(entry, str):
            camera, stream = entry, ""
        elif isinstance(entry, (tuple, list)) and len(entry) == 2:
            camera, stream = str(entry[0]), str(entry[1])
        else:
            raise ValueError(f"Invalid camera selector {entry!r}. Expected a camera name or a (camera, stream) pair.")

        if stream and stream not in RGBD_STREAMS:
            raise ValueError(
                f"Unknown RGBD stream {stream!r} for {camera!r}. Expected one of: {', '.join(RGBD_STREAMS)}."
            )
        if not stream and camera.upper().startswith("RGBD"):
            targets.extend((camera, s) for s in RGBD_STREAMS)
            continue
        targets.append((camera, stream))

    return tuple(dict.fromkeys(targets))


def _stream_label(camera: str, stream: str) -> str:
    return f"{camera}.{stream}" if stream else camera


# ---------------------------------------------------------------------------
# depth_frames()
# ---------------------------------------------------------------------------


@PublicAPI
def depth_frames(
    episodes: DataFrame,
    cameras_: Sequence[str] | str,
    *,
    frame_indices: Sequence[int] | int = 0,
) -> DataFrame:
    r"""Read depth maps from an RGBD camera's ``aligned_depth`` dataset.

    Depth is stored as a ``uint16`` array already registered to the colour
    image, so a depth pixel and its colour pixel share coordinates and no
    reprojection is needed.

    Note:
        The depth unit is **undocumented**. Unlike the tactile and joint
        datasets, ``aligned_depth`` carries no attributes at all, so there is
        nothing in the file to derive a scale from. Values are returned exactly
        as stored; consult PaXini before treating them as millimetres.

    Note:
        ``aligned_depth`` appears in no published layout and is not present on
        every RGBD camera. In a sampled episode only ``RGBD_0`` had it, while
        ``RGBD_1`` and ``RGBD_2`` did not.

    Warning:
        A single frame is ``720 x 1280`` ``uint16``, about 1.8 MB, and a whole
        episode is roughly 380 MB per camera. ``frame_indices`` therefore
        defaults to frame ``0`` rather than reading everything.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        cameras_: RGBD camera name or names, e.g. ``"RGBD_0"``. Unlike
            [`camera_frames`][daft.datasets.omnisharing.camera_frames] these are
            camera names only: depth is stored once per camera rather than per
            colour/left/right sub-stream.
        frame_indices: Which frames to read, as a single index or a sequence.
            Frames are returned in the order requested. Defaults to ``0``.

    Returns:
        The input DataFrame with, per requested camera:

        - `"{camera}.depth"`: `Tensor[UInt16]` of shape
          `(len(frame_indices), height, width)`.
        - `"{camera}.depth_frame_indices"`: the indices actually read.

        Cameras without an ``aligned_depth`` dataset yield nulls.

    Raises:
        ValueError: If ``cameras_`` is empty or ``frame_indices`` is empty.
        IndexError: If an index is negative or beyond the stored frame count.
            Negative indices are rejected rather than wrapping around, so a
            stale index cannot silently read a different frame.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> depth = omnisharing.depth_frames(episodes, "RGBD_0", frame_indices=[0, 10])  # doctest: +SKIP
        >>> depth.select("episode_key", "RGBD_0.depth").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    names = (cameras_,) if isinstance(cameras_, str) else tuple(cameras_)
    if not names:
        raise ValueError("cameras_ must name at least one RGBD camera")
    names = tuple(dict.fromkeys(names))

    wanted = (frame_indices,) if isinstance(frame_indices, int) else tuple(frame_indices)
    if not wanted:
        raise ValueError("frame_indices must contain at least one index")
    negative = [i for i in wanted if i < 0]
    if negative:
        raise IndexError(
            f"frame_indices must be non-negative, got {negative}. "
            "Negative indices are rejected rather than wrapping around."
        )

    # Bounds are checked here rather than inside the UDF: an exception raised
    # during execution surfaces as an opaque Daft error, losing the message that
    # says which camera and which bound were involved.
    _check_depth_bounds(episodes, names, wanted)

    return_fields: dict[str, DataType] = {}
    for name in names:
        return_fields[f"{name}.depth"] = DataType.tensor(DataType.uint16())
        return_fields[f"{name}.depth_frame_indices"] = DataType.list(DataType.int64())

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_depth(file: Hdf5File) -> dict[str, Any]:
        from daft.dependencies import np

        out: dict[str, Any] = {}
        with file._open_h5py() as h5:
            for name in names:
                node = _get(h5, _h5path("observation", "image", name, "aligned_depth"))
                if node is None or not node.shape or node.shape[0] == 0:
                    # Absent or empty depth must not fail the whole episode. An
                    # empty array is used because an all-null tensor column trips
                    # a Daft concat error.
                    out[f"{name}.depth"] = np.empty((0,), dtype="uint16")
                    out[f"{name}.depth_frame_indices"] = None
                    continue

                available = int(node.shape[0])
                # Clamp instead of raising: bounds were already validated up
                # front, and a mid-execution raise would surface opaquely.
                usable = [i for i in wanted if i < available]
                if not usable:
                    out[f"{name}.depth"] = np.empty((0,), dtype="uint16")
                    out[f"{name}.depth_frame_indices"] = None
                    continue

                # Read frame by frame so only the requested slices are fetched.
                stacked = np.stack([np.asarray(node[i]) for i in usable])
                out[f"{name}.depth"] = stacked
                out[f"{name}.depth_frame_indices"] = list(usable)
        return out

    return _append_unnested(episodes, _read_depth(col("episode")))


def _check_depth_bounds(episodes: DataFrame, names: tuple[str, ...], wanted: tuple[int, ...]) -> None:
    """Raise IndexError up front if any requested frame is out of range.

    Probes frame counts from the first episode. A camera with no depth, or with
    a zero-length depth dataset, is skipped rather than treated as a bound: both
    mean "nothing to read here", which is reported as an empty result rather
    than an error.
    """
    probe_fields = {name: DataType.int64() for name in names}

    @daft.func(return_dtype=DataType.struct(probe_fields), use_process=False, unnest=True)
    def _depth_lengths(file: Hdf5File) -> dict[str, Any]:
        lengths: dict[str, Any] = {}
        with _open_for_scan(file) as h5:
            for name in names:
                node = _get(h5, _h5path("observation", "image", name, "aligned_depth"))
                lengths[name] = int(node.shape[0]) if node is not None and node.shape else None
        return lengths

    rows = _append_unnested(episodes.limit(1), _depth_lengths(col("episode"))).to_pylist()
    if not rows:
        return

    highest = max(wanted)
    for name in names:
        available = rows[0].get(name)
        if available and highest >= available:
            out_of_range = [i for i in wanted if i >= available]
            raise IndexError(
                f"{name}/aligned_depth has {available} frames; requested index/indices {out_of_range} out of range."
            )


# ---------------------------------------------------------------------------
# stereo_extrinsics()
# ---------------------------------------------------------------------------


@PublicAPI
def stereo_extrinsics(
    episodes: DataFrame,
    cameras_: Sequence[str] | str,
) -> DataFrame:
    r"""Parse an RGBD camera's stereo calibration into tensors.

    Each RGBD group stores an ``inner_extrinsic`` dataset holding a JSON blob,
    which [`cameras`][daft.datasets.omnisharing.cameras] passes through as an
    opaque string. The useful part is ``left_to_color``: the 4x4 transform from
    the left eye's frame into the colour camera's frame, needed to project the
    stereo pair into a common frame.

    Note:
        This is the *intra*-camera transform, distinct from the ``extrinsics``
        dataset that [`cameras`][daft.datasets.omnisharing.cameras] reports,
        which places the whole camera relative to a reference rig
        (``RGBD_0`` for depth cameras, ``RGB_Camera6`` for colour ones).

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw].
        cameras_: RGBD camera name or names, e.g. ``"RGBD_0"``.

    Returns:
        The input DataFrame with, per requested camera:

        - `"{camera}.left_to_color"`: `Tensor[Float64]` of shape `(4, 4)`.
        - `"{camera}.calib_date"`: calibration timestamp as a string.

        Cameras with no ``inner_extrinsic``, or whose JSON cannot be parsed,
        yield nulls rather than failing the read.

    Raises:
        ValueError: If ``cameras_`` is empty.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> calib = omnisharing.stereo_extrinsics(episodes, "RGBD_0")  # doctest: +SKIP
        >>> calib.select("episode_key", "RGBD_0.left_to_color").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    names = (cameras_,) if isinstance(cameras_, str) else tuple(cameras_)
    if not names:
        raise ValueError("cameras_ must name at least one RGBD camera")
    names = tuple(dict.fromkeys(names))

    return_fields: dict[str, DataType] = {}
    for name in names:
        return_fields[f"{name}.left_to_color"] = DataType.tensor(DataType.float64())
        return_fields[f"{name}.calib_date"] = DataType.string()

    @daft.func(return_dtype=DataType.struct(return_fields), use_process=False, unnest=True)
    def _read_stereo(file: Hdf5File) -> dict[str, Any]:
        from daft.dependencies import np

        out: dict[str, Any] = {}
        with _open_for_scan(file) as h5:
            for name in names:
                # An all-null tensor column trips a Daft concat error, so an
                # unavailable transform is an empty array rather than null.
                out[f"{name}.left_to_color"] = np.empty((0,), dtype="float64")
                out[f"{name}.calib_date"] = None

                node = _get(h5, _h5path("observation", "image", name, "inner_extrinsic"))
                if node is None:
                    continue

                raw_value = node[()]
                blob = raw_value[0] if getattr(raw_value, "shape", ()) else raw_value
                if isinstance(blob, bytes):
                    blob = blob.decode("utf-8", "replace")
                try:
                    payload = json.loads(blob)
                except (TypeError, ValueError):
                    # Calibration is optional metadata; a malformed blob must not
                    # take down the whole read.
                    continue
                if not isinstance(payload, dict):
                    continue

                out[f"{name}.calib_date"] = (
                    str(payload["calib_date"]) if payload.get("calib_date") is not None else None
                )
                matrix = payload.get("left_to_color")
                if matrix is None:
                    continue
                try:
                    as_array = np.asarray(matrix, dtype="float64")
                except (TypeError, ValueError):
                    continue
                if as_array.shape == (4, 4):
                    out[f"{name}.left_to_color"] = as_array
        return out

    return _append_unnested(episodes, _read_stereo(col("episode")))


# ---------------------------------------------------------------------------
# frames()
# ---------------------------------------------------------------------------

#: Per-hand signals addressable by :func:`frames`, in dotted form.
_FRAME_FIELDS: tuple[str, ...] = tuple(
    f"{branch}.{side}.{signal}"
    for branch in BRANCHES
    for side in SIDES
    for signal in _SIGNALS
    # tactile is only recorded under observation
    if not (branch == "action" and signal == "tactile")
)


@PublicAPI
def frames(
    episodes: DataFrame,
    fields: Sequence[str],
    *,
    align_cameras: Sequence[tuple[str, str]] | Sequence[str] = (),
) -> DataFrame:
    r"""Expand episodes into one row per frame, aligned on ``aligned_timestamp``.

    Applies the dataset's documented action/observation offset: ``action`` leads
    ``observation`` by one frame, so ``action[i]`` is the state at ``i + 1``,
    with the final action repeated to keep both the same length. Reading the raw
    arrays without this offset silently misaligns state and action.

    | Frame       | 0   | 1   | ... | n-2   | n-1   |
    | ----------- | --- | --- | --- | ----- | ----- |
    | observation | s0  | s1  | ... | s(n-2)| s(n-1)|
    | action      | s1  | s2  | ... | s(n-1)| s(n-1)|

    Warning:
        Camera streams do **not** share the observation clock. In a sampled
        episode the hands and RGBD cameras tick 207 times while the RGB cameras
        produce 208-254 frames, and ``RGB_Camera0`` had already been recording
        for 10 frames when observation frame 0 was captured. ``align_cameras``
        therefore matches by nearest timestamp and never by frame index.

    Note:
        ``fields`` is required. Frame-level expansion multiplies row count by the
        frame count, and a single tactile row is 3465 floats wide, so an
        unrestricted expansion is a memory hazard.

    Args:
        episodes: Episode-level DataFrame from
            [`raw`][daft.datasets.omnisharing.raw]. Filter it first.
        fields: Dotted ``"{branch}.{side}.{signal}"`` names to expand, where
            signal is ``joints``, ``handpose`` or ``tactile``. ``tactile`` exists
            only under ``observation``.
        align_cameras: Optional camera streams to align to each frame. Adds a
            ``"{name}.frame_index"`` and ``"{name}.timestamp_delta_us"`` column
            per stream, giving the nearest source frame and how far off it is.
            No video is decoded here.

    Returns:
        DataFrame with one row per frame: the episode's identity columns, a
        ``frame_index`` and ``timestamp`` column, one column per requested field
        holding that frame's vector, and camera alignment columns when
        ``align_cameras`` is used.

    Examples:
        >>> from daft.datasets import omnisharing
        >>> episodes = omnisharing.raw("paxini/Omnisharing_DB_SampleData").limit(1)  # doctest: +SKIP
        >>> per_frame = omnisharing.frames(  # doctest: +SKIP
        ...     episodes,
        ...     fields=["observation.lefthand.joints", "action.lefthand.joints"],
        ...     align_cameras=["RGB_Camera0"],
        ... )
        >>> per_frame.select("frame_index", "RGB_Camera0.frame_index").show()  # doctest: +SKIP
    """
    _require_episode_column(episodes)
    _require_h5py()

    if isinstance(fields, str):
        fields = [fields]
    fields = tuple(fields)
    if not fields:
        raise ValueError(
            "frames() requires an explicit `fields` whitelist: expanding every "
            "signal per frame is a memory hazard (tactile alone is thousands of "
            f"floats per frame). Valid fields are: {', '.join(_FRAME_FIELDS)}"
        )
    unknown = [f for f in fields if f not in _FRAME_FIELDS]
    if unknown:
        raise ValueError(f"Unknown frame field(s): {unknown}. Valid fields are: {', '.join(_FRAME_FIELDS)}")

    camera_targets = _normalize_camera_targets(align_cameras) if align_cameras else ()

    row_fields: dict[str, DataType] = {
        "frame_index": DataType.int64(),
        "timestamp": DataType.int64(),
    }
    for f in fields:
        row_fields[f] = DataType.tensor(DataType.float32())
    for camera, stream in camera_targets:
        name = _stream_label(camera, stream)
        row_fields[f"{name}.frame_index"] = DataType.int64()
        row_fields[f"{name}.timestamp_delta_us"] = DataType.int64()

    _ROW_DTYPE = DataType.struct(row_fields)

    @daft.func(return_dtype=DataType.list(_ROW_DTYPE), use_process=False)
    def _expand(file: Hdf5File) -> list[dict[str, Any]]:
        from daft.dependencies import np

        with file._open_h5py() as h5:
            timestamps_node = _get(h5, _h5path("observation", "aligned_timestamp"))
            arrays: dict[str, Any] = {}
            for f in fields:
                node = _get(h5, _field_to_h5path(f))
                if node is not None:
                    arrays[f] = node[()]

            if timestamps_node is not None:
                timestamps = np.asarray(timestamps_node[()])
            elif arrays:
                # Fall back to the length of whatever signal we could read.
                timestamps = np.zeros(len(next(iter(arrays.values()))), dtype="int64")
            else:
                return []

            n = int(timestamps.shape[0])

            camera_ts: dict[str, Any] = {}
            for camera, stream in camera_targets:
                group = _get(h5, _h5path("observation", "image", camera))
                if group is None:
                    continue
                ts = _get(group, f"{stream}_timestamp" if stream else "timestamp") or _get(group, "timestamp")
                if ts is not None:
                    camera_ts[_stream_label(camera, stream)] = np.asarray(ts[()])

            rows: list[dict[str, Any]] = []
            for i in range(n):
                row: dict[str, Any] = {
                    "frame_index": i,
                    "timestamp": int(timestamps[i]),
                }
                for f in fields:
                    data = arrays.get(f)
                    if data is None:
                        row[f] = None
                        continue
                    # action leads observation by one frame; repeat the last.
                    idx = min(i + 1, len(data) - 1) if f.startswith("action.") else i
                    row[f] = data[idx] if idx < len(data) else None

                for name, cts in camera_ts.items():
                    nearest = int(np.argmin(np.abs(cts - timestamps[i])))
                    row[f"{name}.frame_index"] = nearest
                    row[f"{name}.timestamp_delta_us"] = int(cts[nearest]) - int(timestamps[i])
                rows.append(row)
            return rows

    identity = [c for c in episodes.column_names if c != "episode"]
    exploded = episodes.select(*identity, _expand(col("episode")).alias("_frame")).explode("_frame")
    return exploded.select(*identity, unnest(col("_frame")))


__all__ = [
    "HANDPOSE_ORDER",
    "OBJECT_POSE_WIDTH",
    "RGBD_STREAMS",
    "SIDES",
    "STAGES",
    "audio",
    "camera_frames",
    "camera_payloads",
    "cameras",
    "depth_frames",
    "describe",
    "episode_metadata",
    "frames",
    "objects",
    "raw",
    "stereo_extrinsics",
    "tactile",
    "trajectory",
]
