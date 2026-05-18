"""LeRobot Dataset v3.0 helpers for `daft.datasets`.

This module reads the file-based LeRobot v3 layout (``meta/episodes``, ``data``,
``videos``) and exposes episode-level scans plus frame expansion utilities.

See https://huggingface.co/docs/lerobot/lerobot-dataset-v3 for format details.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.exceptions import DaftCoreException
from daft.expressions import Expression, col, lit
from daft.file import VideoFile
from daft.functions import lpad
from daft.functions.file_ import video_file
from daft.udf import func

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


# Column names used by Hugging Face LeRobot v3 metadata / data shards.
_DATA_CHUNK_CANDIDATES = ("data/chunk_index", "data_chunk_index")
_DATA_FILE_CANDIDATES = ("data/file_index", "data_file_index")

_DATA_PATH_COL = "lerobot_data_parquet_path"


def _is_probable_hf_repo_id(uri: str) -> bool:
    return bool(re.fullmatch(r"[\w.-]+/[\w.-]+", uri))


def _normalize_dataset_root(uri: str) -> str:
    """Return a canonical dataset root prefix (no trailing slash) for path joins."""
    u = uri.strip()
    if _is_probable_hf_repo_id(u):
        u = f"hf://datasets/{u}"
    return u.rstrip("/")


def _https_base_for_hf_datasets_root(root: str) -> str | None:
    if not root.startswith("hf://datasets/"):
        return None
    repo_id = root.removeprefix("hf://datasets/")
    return f"https://huggingface.co/datasets/{repo_id}/resolve/main"


def _pick_first_column(df: DataFrame, candidates: tuple[str, ...]) -> str:
    names = set(df.column_names)
    for c in candidates:
        if c in names:
            return c
    raise ValueError(
        "Expected one of columns "
        + ", ".join(repr(c) for c in candidates)
        + f" in episodes dataframe, but found columns: {sorted(names)}"
    )


def _data_parquet_path_expr(root_expr: Expression, chunk_col: str, file_col: str) -> Expression:
    """Build ``{root}/data/chunk-XXX/file-YYY.parquet``."""
    chunk_str = lpad(col(chunk_col).cast(DataType.string), 3, "0")
    file_str = lpad(col(file_col).cast(DataType.string), 3, "0")
    return (
        root_expr.cast(DataType.string) + lit("/data/chunk-") + chunk_str + lit("/file-") + file_str + lit(".parquet")
    )


def _video_mp4_path_expr(root_expr: Expression, video_key: str) -> Expression:
    """Build ``{root}/videos/{video_key}/chunk-XXX/file-YYY.mp4`` from episode parquet columns."""
    chunk_col = f"videos/{video_key}/chunk_index"
    file_col = f"videos/{video_key}/file_index"
    chunk_str = lpad(col(chunk_col).cast(DataType.string), 3, "0")
    file_str = lpad(col(file_col).cast(DataType.string), 3, "0")
    return (
        root_expr.cast(DataType.string)
        + lit(f"/videos/{video_key}/chunk-")
        + chunk_str
        + lit("/file-")
        + file_str
        + lit(".mp4")
    )


def _video_feature_keys(features: dict[str, Any]) -> tuple[str, ...]:
    keys: list[str] = []
    for name, meta in sorted(features.items()):
        if isinstance(meta, dict) and meta.get("dtype") == "video":
            keys.append(name)
    return tuple(keys)


@func(return_dtype=DataType.image())
def _decode_lerobot_video_timestamp(
    file: VideoFile,
    episode_from_timestamp_s: float,
    frame_timestamp_s: float,
    tolerance_s: float,
    image_width_i: int,
    image_height_i: int,
):
    """Pick the decoded frame closest in time to ``episode_from_timestamp_s + frame_timestamp_s``."""
    try:
        import av as av_mod
    except ImportError as err:
        raise ImportError("Decoding LeRobot MP4 shards requires PyAV. Install with `pip install av`.") from err
    try:
        from PIL import Image as PILImage
    except ImportError as err:
        raise ImportError(
            "Decoding LeRobot MP4 shards requires Pillow. Install with `pip install daft[video]` or `pip install pillow`."
        ) from err
    abs_ts = float(episode_from_timestamp_s) + float(frame_timestamp_s)
    tolerance = float(tolerance_s)
    width_i = int(image_width_i)
    height_i = int(image_height_i)
    width: int | None
    height: int | None
    if width_i > 0 and height_i > 0:
        width, height = width_i, height_i
    else:
        width, height = None, None

    loaded: list[tuple[float, Any]] = []
    decode_cap = 20_000
    decoded = 0

    with file.open() as f_open:
        with av_mod.open(f_open) as container:
            stream = container.streams.video[0]
            # Match LeRobot: seek backwards to preceding keyframe, then decode forwards.
            container.seek(max(0, int(abs_ts / av_mod.time_base)), backward=True)

            tail_s = max(0.1, tolerance * 50.0, 1.0 / 24.0)
            for vf in container.decode(stream):
                if vf.pts is None:
                    continue
                current_ts = float(vf.pts * stream.time_base)
                pil_img = PILImage.fromarray(vf.to_ndarray(format="rgb24"), mode="RGB")
                if width is not None and height is not None:
                    pil_img = pil_img.resize((width, height), PILImage.Resampling.NEAREST)

                loaded.append((current_ts, pil_img))
                decoded += 1

                if decoded >= decode_cap:
                    raise ValueError("Exceeded decode frame budget while aligning to parquet timestamps.")
                if current_ts >= abs_ts + tail_s:
                    break

    if not loaded:
        raise ValueError(f"No frames decoded from shard while seeking timestamp {abs_ts:.6f}s.")

    closest_ts, closest_img = min(loaded, key=lambda item: abs(item[0] - abs_ts))
    closest_dist = abs(closest_ts - abs_ts)
    if closest_dist > tolerance:
        raise ValueError(
            f"No frame matched timestamp {abs_ts:.6f}s within tolerance {tolerance} "
            f"(closest distance observed: {closest_dist})."
        )
    return closest_img


def _assert_episodes_have_video_cols(episodes: DataFrame, video_keys: tuple[str, ...]) -> None:
    names = episodes.column_names
    missing = [
        candidate
        for vk in video_keys
        for candidate in (
            f"videos/{vk}/chunk_index",
            f"videos/{vk}/file_index",
            f"videos/{vk}/from_timestamp",
        )
        if candidate not in names
    ]
    if missing:
        raise ValueError(
            "Episodes dataframe is missing LeRobot video index columns needed for decoding: "
            + ", ".join(repr(x) for x in missing)
        )


@PublicAPI
def read_episodes(dataset_uri: str, io_config: IOConfig | None = None) -> DataFrame:
    """Read LeRobot v3 episode metadata as a lazy DataFrame (one row per episode).

    This reads ``meta/episodes/**/*.parquet`` under the dataset root.

    Args:
        dataset_uri: Local directory, ``hf://datasets/org/name`` URI, or bare
            ``org/name`` which is treated as a Hub dataset id.
        io_config: Optional IO configuration for remote reads.

    Returns:
        Lazy episode metadata DataFrame.
    """
    root = _normalize_dataset_root(dataset_uri)
    return daft.read_parquet(f"{root}/meta/episodes/**/*.parquet", io_config=io_config)


@PublicAPI
def read_frames(dataset_uri: str, io_config: IOConfig | None = None) -> DataFrame:
    """Read all frame data from a LeRobot v3 dataset into a lazy DataFrame (one row per frame).

    This reads the ``data/chunk-XXX/file-YYY.parquet`` under the dataset root. If you only need a subset of the frames, use :func:`load_episode_frames` instead from a filtered episodes dataframe from :func:`read_episodes`.

    Args:
        dataset_uri: Same dataset root as passed to :func:`read_episodes` (local path,
            ``hf://datasets/org/name``, or bare ``org/name`` Hub id).
        io_config: Optional IO configuration for remote reads.

    Returns:
        Lazy DataFrame of frame metadata.
    """
    root = _normalize_dataset_root(dataset_uri)
    return daft.read_parquet(f"{root}/data/**/*.parquet", io_config=io_config)


@PublicAPI
def load_episode_frames(
    episodes: DataFrame,
    dataset_uri: str,
    *,
    io_config: IOConfig | None = None,
    columns: list[str] | None = None,
    decode_videos: bool = False,
    video_keys: list[str] | None = None,
    timestamp_tolerance_seconds: float = 1e-4,
    decode_image_width: int | None = None,
    decode_image_height: int | None = None,
) -> DataFrame:
    """Expand filtered episode rows into frame-level rows from ``data/`` Parquet shards.

    This executes a small eager step to discover distinct shard paths from the
    current logical plan, then lazily reads only those Parquet files and keeps
    rows whose ``episode_index`` appears in ``episodes``.

    Optionally decodes MP4 shards under ``videos/<feature_key>/chunk-XXX/file-YYY.mp4`` into
    :class:`~daft.datatype.DataType` ``image()`` columns keyed by LeRobot ``feature_key`` strings
    (typically ``dtype: "video"`` entries in ``meta/info.json``), using the episode-level
    ``videos/<key>/from_timestamp``, ``videos/<key>/chunk_index``, and ``videos/<key>/file_index``
    fields plus row ``timestamp`` values (matching how ``LeRobotDataset`` aligns frames).

    Preconditions:
    - ``episodes`` must include either ``data/chunk_index`` / ``data/file_index``
      (canonical LeRobot v3) or the ``data_chunk_index`` / ``data_file_index`` spelling.

    Args:
        episodes: Episode-level dataframe (typically filtered) from :func:`read_episodes`.
        dataset_uri: Same dataset root as passed to :func:`read_episodes` (local path,
            ``hf://datasets/org/name``, or bare ``org/name`` Hub id).
        io_config: Optional IO configuration for remote reads.
        columns: Optional projection of frame columns (passed to :meth:`daft.DataFrame.select`).
        decode_videos: When ``True``, add decoded camera images for each declared ``video_keys`` subset.
            Requires PyAV and Pillow plus per-episode columns ``videos/<key>/{chunk_index,file_index,from_timestamp}``.
        video_keys: Subset of video feature keys from ``meta/info.json`` (must have ``dtype: "video"``).
            When ``None`` and ``decode_videos`` is enabled, all video features are decoded.
        timestamp_tolerance_seconds: Maximum |PTS - (from_timestamp + timestamp)| in seconds (LeRobot default is ~1e-4).
        decode_image_width: If set with ``decode_image_height``, nearest-neighbor resize decoded frames.
        decode_image_height: See ``decode_image_width``.

    Returns:
        Lazy frame-level dataframe.
    """
    root = _normalize_dataset_root(dataset_uri)
    root_expr = lit(root)

    chunk_col = _pick_first_column(episodes, _DATA_CHUNK_CANDIDATES)
    file_col = _pick_first_column(episodes, _DATA_FILE_CANDIDATES)

    with_paths = episodes.with_column(
        _DATA_PATH_COL,
        _data_parquet_path_expr(root_expr, chunk_col, file_col),
    )

    paths = with_paths.select(_DATA_PATH_COL).distinct().to_pydict()[_DATA_PATH_COL]
    if len(paths) == 0:
        empty_cols = list(columns) if columns is not None else ["episode_index", "frame_index", "timestamp"]
        return daft.from_pydict({c: [] for c in empty_cols})

    frames = daft.read_parquet(paths, io_config=io_config)

    decode_w = decode_image_width or 0
    decode_h = decode_image_height or 0
    if (decode_image_width is None) != (decode_image_height is None):
        raise ValueError("decode_image_width and decode_image_height must both be set or both omitted.")

    if decode_videos:
        info = read_info(root)
        meta_vkeys = _video_feature_keys(info.get("features", {}))
        if video_keys is None:
            selected_vkeys = meta_vkeys
        else:
            unknown = sorted(set(video_keys) - set(meta_vkeys))
            if unknown:
                raise ValueError(
                    "video_keys contains keys not declared as dtype 'video' in meta/info.json: "
                    + ", ".join(repr(k) for k in unknown)
                )
            selected_vkeys = tuple(video_keys)

        if selected_vkeys:
            if "timestamp" not in frames.column_names:
                raise ValueError(
                    "decode_videos requires a `timestamp` column on frame rows (LeRobot v3 Parquet default)."
                )
            _assert_episodes_have_video_cols(episodes, selected_vkeys)

            join_cols = ["episode_index"]
            for vk in selected_vkeys:
                join_cols.extend(
                    [
                        f"videos/{vk}/chunk_index",
                        f"videos/{vk}/file_index",
                        f"videos/{vk}/from_timestamp",
                    ]
                )
            vid_meta = episodes.select(*join_cols).distinct()
            frames = frames.join(vid_meta, on="episode_index", how="inner")

            for vk in selected_vkeys:
                frames = frames.with_column(
                    vk,
                    _decode_lerobot_video_timestamp(
                        video_file(
                            _video_mp4_path_expr(root_expr, vk),
                            io_config=io_config,
                        ),
                        col(f"videos/{vk}/from_timestamp"),
                        col("timestamp"),
                        lit(timestamp_tolerance_seconds),
                        lit(decode_w),
                        lit(decode_h),
                    ),
                )

    if columns is not None:
        frames = frames.select(*columns)
    allowed = with_paths.select("episode_index").distinct()
    return frames.join(allowed, on="episode_index", how="inner")


@PublicAPI
def read_tasks(dataset_uri: str, io_config: IOConfig | None = None) -> DataFrame:
    """Load task metadata as a DataFrame.

    Prefers ``meta/tasks.parquet`` (current LeRobot default). Falls back to legacy
    ``meta/tasks.jsonl`` when the Parquet file is missing.
    """
    root = _normalize_dataset_root(dataset_uri)

    https_base = _https_base_for_hf_datasets_root(root)
    if https_base is not None:
        pq_url = f"{https_base}/meta/tasks.parquet"
        try:
            return daft.read_parquet(pq_url, io_config=io_config)
        except (OSError, DaftCoreException, FileNotFoundError):
            return daft.read_json(f"{root}/meta/tasks.jsonl", io_config=io_config)

    pq_path = Path(root) / "meta" / "tasks.parquet"
    if pq_path.is_file():
        return daft.read_parquet(str(pq_path), io_config=io_config)

    jsonl_path = Path(root) / "meta" / "tasks.jsonl"
    if jsonl_path.is_file():
        return daft.read_json(str(jsonl_path), io_config=io_config)

    raise FileNotFoundError(f"No tasks metadata found under {root}/meta (tasks.parquet or tasks.jsonl)")


__all__ = [
    "load_episode_frames",
    "read_episodes",
    "read_tasks",
]
