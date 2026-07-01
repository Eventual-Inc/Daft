"""LeRobot Dataset v3.0 helpers for `daft.datasets`.

This module reads the file-based LeRobot v3 layout (`meta/episodes`, `data`,
`videos`) and exposes episode-level scans plus frame expansion utilities.

See https://huggingface.co/docs/lerobot/lerobot-dataset-v3 for format details.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, TypedDict, cast

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.exceptions import DaftCoreException
from daft.expressions import col, lit
from daft.file import VideoFile
from daft.functions import lpad
from daft.functions.file_ import video_file
from daft.udf import func

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame


def _normalize_dataset_root(uri: str) -> str:
    """Return a canonical dataset root prefix (no trailing slash) for path joins."""
    u = uri.strip()
    # Input looks like a Hugging Face repo ID, i.e. "org/name"
    is_hf_repo_id = bool(re.fullmatch(r"[\w.-]+/[\w.-]+", uri))

    if is_hf_repo_id:
        u = f"hf://datasets/{u}"
    return u.rstrip("/")


def _decode_one_shard(
    file: VideoFile,
    abs_timestamps: list[tuple[int, float]],
    tolerance: float,
    width: int | None,
    height: int | None,
    av_mod: Any,
    pil_image: Any,
) -> list[tuple[int, Any]]:
    """Open a single shard once and return ``(row_index, PIL.Image)`` for every requested timestamp.

    All ``abs_timestamps`` refer to the same shard ``file``. We seek once to the
    keyframe preceding the earliest target, then decode forward a single time,
    keeping the closest decoded frame to each target. This is the batched
    equivalent of the old per-row decode: one open + one forward pass per shard
    instead of per frame.
    """
    targets = sorted(abs_timestamps, key=lambda t: t[1])  # ascending by timestamp
    earliest = targets[0][1]
    latest = targets[-1][1]
    tail_s = max(0.1, tolerance * 50.0, 1.0 / 24.0)
    decode_cap = 20_000

    # Best (distance, ndarray) seen so far for each target row; at most one frame
    # is retained per target, so memory stays bounded regardless of the span.
    best_dist = {row: float("inf") for row, _ in targets}
    best_arr: dict[int, Any] = {}
    decoded = 0

    with file.open() as f_open:
        with av_mod.open(f_open) as container:
            stream = container.streams.video[0]
            # Match LeRobot: seek backwards to preceding keyframe, then decode forwards.
            container.seek(max(0, int(earliest * av_mod.time_base)), backward=True)
            for vf in container.decode(stream):
                if vf.pts is None:
                    continue
                current_ts = float(vf.pts * stream.time_base)
                arr = vf.to_ndarray(format="rgb24")
                for row, target in targets:
                    dist = abs(current_ts - target)
                    if dist < best_dist[row]:
                        best_dist[row] = dist
                        best_arr[row] = arr

                decoded += 1
                if decoded >= decode_cap:
                    raise ValueError("Exceeded decode frame budget while aligning to parquet timestamps.")
                if current_ts >= latest + tail_s:
                    break

    if decoded == 0:
        raise ValueError(f"No frames decoded from shard while seeking timestamp {earliest:.6f}s.")

    out: list[tuple[int, Any]] = []
    for row, target in targets:
        if best_dist[row] > tolerance:
            raise ValueError(
                f"No frame matched timestamp {target:.6f}s within tolerance {tolerance} "
                f"(closest distance observed: {best_dist[row]})."
            )
        img = pil_image.fromarray(best_arr[row], mode="RGB")
        if width is not None and height is not None:
            img = img.resize((width, height), pil_image.Resampling.NEAREST)
        out.append((row, img))
    return out


@func.batch(return_dtype=DataType.image())
def _decode_lerobot_video_timestamp(
    files: Any,  # daft.Series of VideoFile
    episode_from_timestamps_s: Any,  # daft.Series of float
    frame_timestamps_s: Any,  # daft.Series of float
    tolerance_s: float,
    image_width_i: int,
    image_height_i: int,
) -> Any:  # a daft.Series-compatible list of PIL.Image, one per row
    """Decode the frame closest to ``from_timestamp + timestamp`` for each row.

    Batched over rows: rows sharing the same shard file are grouped so the shard
    is opened exactly once per batch instead of once per frame.
    """
    try:
        import av as av_mod
    except ImportError as err:
        raise ImportError("Decoding LeRobot MP4 shards requires PyAV. Install with `pip install av`.") from err
    try:
        from PIL import Image as pil_image
    except ImportError as err:
        raise ImportError(
            "Decoding LeRobot MP4 shards requires Pillow. Install with `pip install daft[video]` or `pip install pillow`."
        ) from err

    file_list = files.to_pylist()
    from_list = episode_from_timestamps_s.to_pylist()
    frame_list = frame_timestamps_s.to_pylist()

    tolerance = float(tolerance_s)
    width_i = int(image_width_i)
    height_i = int(image_height_i)
    width = width_i if width_i > 0 and height_i > 0 else None
    height = height_i if width_i > 0 and height_i > 0 else None

    # Group row indices by shard path so each shard is opened once.
    by_shard: dict[str, list[tuple[int, float]]] = {}
    shard_file: dict[str, VideoFile] = {}
    for i, file in enumerate(file_list):
        abs_ts = float(from_list[i]) + float(frame_list[i])
        by_shard.setdefault(file.path, []).append((i, abs_ts))
        shard_file[file.path] = file

    results: list[Any] = [None] * len(file_list)
    for path, targets in by_shard.items():
        for row, img in _decode_one_shard(shard_file[path], targets, tolerance, width, height, av_mod, pil_image):
            results[row] = img
    return results


class Feature(TypedDict):
    dtype: str


class LeRobotInfo(TypedDict):
    codebase_version: str
    data_path: str
    video_path: str
    fps: float
    features: dict[str, Feature]


def _read_info(normalized_uri: str, io_config: IOConfig | None = None) -> LeRobotInfo:
    with daft.open_file(f"{normalized_uri}/meta/info.json", io_config=io_config) as f:
        info = cast("LeRobotInfo", json.load(f))
        if info["codebase_version"] != "v3.0":
            raise ValueError("`daft.datasets.lerobot` currently only supports LeRobot datasets of v3 and above")
        return info


@PublicAPI
def read(
    dataset_uri: str,
    io_config: IOConfig | None = None,
    include_stats: bool = False,
    load_video_frames: str | list[str] | bool = False,
) -> DataFrame:
    """Read a LeRobot v3 dataset as a lazy DataFrame with one row per frame.

    Reads the per-episode metadata under ``meta/episodes`` and the per-frame
    sensor data under ``data``, joins them on ``episode_index``, and broadcasts
    each episode's metadata across its frames. Optionally decodes the matching
    video frame for one or more camera keys into an image column.

    Args:
        dataset_uri: Huggingface repo id (``org/name``), or a local / remote
            directory (``s3://...``, ``hf://datasets/...``).
        io_config: Optional IO configuration for remote reads.
        include_stats: If True, keep the per-episode ``stats/*`` columns
            (per-feature min/max/mean/std/quantiles). Defaults to False.
        load_video_frames: Which camera keys to decode into image columns,
            aligned to each frame's timestamp. Defaults to False (decode
            nothing). Pass True to decode every video feature, a single key
            (``"observation.image"``), or a list of keys. Decoding requires the
            optional ``av`` (PyAV) and ``Pillow`` dependencies.

    Returns:
        Lazy DataFrame with one row per frame: the frame's sensor columns, the
        broadcast episode metadata, and one image column per decoded video key.
    """
    root = _normalize_dataset_root(dataset_uri)
    info = _read_info(root, io_config=io_config)

    # Keep the per-episode video metadata (notably `videos/{key}/from_timestamp`,
    # the time within the shard where each episode's footage begins). We need it
    # to translate episode-local frame timestamps into absolute shard timestamps
    # when decoding, and drop these internal columns again before returning.
    episode_df = read_episodes(
        dataset_uri, io_config=io_config, include_stats=include_stats, include_video_metadata=True
    )
    df = load_episode_frames(episode_df, dataset_uri, io_config=io_config)

    # Load video frames into memory
    if load_video_frames is not False:
        if load_video_frames is True:
            video_keys = [name for name, feat_info in info["features"].items() if feat_info["dtype"] == "video"]
        elif isinstance(load_video_frames, str):
            video_keys = [load_video_frames]
        elif isinstance(load_video_frames, list) and all(isinstance(k, str) for k in load_video_frames):
            video_keys = load_video_frames
        else:
            raise ValueError(f"Invalid value provided for argument load_video_frames=`{load_video_frames}`")

        # An MP4 shard packs many episodes back to back, so the shard's internal
        # frame numbering is NOT the parquet's episode-local `frame_index` (which
        # resets to 0 each episode). Seeking by `frame_index` only happens to work
        # for the first episode in each shard. Instead, seek by absolute timestamp:
        # `from_timestamp` (where this episode begins in the shard) + the per-frame
        # episode-local `timestamp`. That keeps a single coordinate system end to end.
        fps = float(info["fps"])
        tolerance_s = 1.0 / fps / 2.0  # half a frame period: any closer frame is unambiguously "the" frame

        # Batch size trades decode parallelism (smaller batches -> more concurrent
        # tasks) against shard opens (rows sharing a shard are decoded in one open
        # per batch, so smaller batches -> more opens of the same shard).
        df = df.into_batches(16)
        for k in video_keys:
            df = df.with_column(
                k,
                _decode_lerobot_video_timestamp(
                    col(f"videos/{k}/video"),
                    col(f"videos/{k}/from_timestamp"),
                    col("timestamp"),
                    tolerance_s,
                    0,  # image_width: 0 disables resize (decode at native resolution)
                    0,  # image_height: 0 disables resize
                ),
            )
            df = df.exclude(f"videos/{k}/video")

    # Drop the internal per-episode video metadata we kept above (chunk/file index,
    # from/to timestamp). This restores read_episodes' default of hiding these.
    df = df.exclude(*(c for c in df.column_names if c.startswith("videos/") and not c.endswith("/video")))

    return df


@PublicAPI
def read_episodes(
    dataset_uri: str,
    io_config: IOConfig | None = None,
    include_meta: bool = False,
    include_stats: bool = False,
    include_video_metadata: bool = False,
) -> DataFrame:
    """Read LeRobot v3 episode metadata as a lazy DataFrame (one row per episode).

    This reads the `meta/episodes/**/*.parquet` path under the dataset root.

    Args:
        dataset_uri: Huggingface repo id (`org/name`),
            or a local / remote directory (`s3://...`, `hf://datasets/...`)
        io_config: Optional IO configuration for remote reads.
        include_meta: If True, keep the internal ``meta/episodes/*`` columns
            (the chunk/file indices locating each episode's own metadata shard).
            These are bookkeeping for random access into the sharded metadata
            and carry no analytical value once the rows are loaded. Defaults to
            False.
        include_stats: If True, keep the per-episode ``stats/*`` columns
            (per-feature min/max/mean/std/quantiles). Defaults to False.
        include_video_metadata: If True, keep the per-episode ``videos/{key}/*``
            columns (the chunk/file indices and from/to timestamps locating each
            episode's footage within its video shard). Defaults to False.

    Returns:
        Lazy DataFrame of episode metadata, one row per episode. Always includes
        a ``videos/{key}/video`` file-handle column per video feature; the
        ``include_*`` flags control which additional column families are kept.
    """
    root = _normalize_dataset_root(dataset_uri)
    info = _read_info(root, io_config=io_config)
    df = daft.read_parquet(f"{root}/meta/episodes/**/*.parquet", io_config=io_config)
    if not include_meta:
        df = df.exclude(*(c for c in df.column_names if c.startswith("meta/")))
    if not include_stats:
        df = df.exclude(*(c for c in df.column_names if c.startswith("stats/")))

    # Get the video keys
    video_keys = set(name for name, feat_info in info["features"].items() if feat_info["dtype"] == "video")

    for key in video_keys:
        file_name_expr = (
            lit(f"{root}/videos/{key}/chunk-")
            + lpad(col(f"videos/{key}/chunk_index").cast(DataType.string), 3, "0")
            + lit("/file-")
            + lpad(col(f"videos/{key}/file_index").cast(DataType.string), 3, "0")
            + lit(".mp4")
        )

        df = df.with_column(f"videos/{key}/video", video_file(file_name_expr, verify=False, io_config=io_config))

    if not include_video_metadata:
        df = df.exclude(*(c for c in df.column_names if c.startswith("videos/") and not c.endswith("/video")))

    return df


@PublicAPI
def load_episode_frames(
    episodes: DataFrame,
    dataset_uri: str,
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Expand an episode-level DataFrame into a frame-level DataFrame.

    Reads the per-frame parquet under ``data/**`` and joins it to the provided
    episode metadata on ``episode_index``, producing one row per frame. Episode
    metadata is broadcast across each episode's frames.

    Filter ``episodes`` before calling this to expand only the episodes you need;
    only the surviving episodes contribute to the join.

    Args:
        episodes: Episode-level DataFrame, typically from :func:`read_episodes`
            (optionally filtered). Must contain an ``episode_index`` column.
        dataset_uri: The same dataset identifier passed to :func:`read_episodes`
            (Huggingface repo id ``org/name``, or a local / remote directory such
            as ``s3://...`` or ``hf://datasets/...``).
        io_config: Optional IO configuration for remote reads.

    Returns:
        Lazy DataFrame with one row per frame.
    """
    root = _normalize_dataset_root(dataset_uri)

    frame_df = daft.read_parquet(f"{root}/data/**", io_config=io_config)
    df = episodes.join(frame_df, on=["episode_index"])
    df = df.exclude("data/chunk_index", "data/file_index")
    return df


@PublicAPI
def read_tasks(dataset_uri: str, io_config: IOConfig | None = None) -> DataFrame:
    """Load task metadata as a DataFrame.

    Prefers ``meta/tasks.parquet`` (current LeRobot default). Falls back to legacy
    ``meta/tasks.jsonl`` when the Parquet file is missing.
    """
    root = _normalize_dataset_root(dataset_uri)

    pq_url = f"{root}/meta/tasks.parquet"
    try:
        return daft.read_parquet(pq_url, io_config=io_config)
    except (OSError, DaftCoreException, FileNotFoundError):
        return daft.read_json(f"{root}/meta/tasks.jsonl", io_config=io_config)


__all__ = [
    "load_episode_frames",
    "read",
    "read_episodes",
    "read_tasks",
]
