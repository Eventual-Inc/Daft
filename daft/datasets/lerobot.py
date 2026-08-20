"""LeRobot Dataset helpers for `daft.datasets`.

Supports the episode-per-file v2.0/v2.1 layout (`meta/episodes.jsonl`,
`data/chunk-XXX/episode_YYYYYY.parquet`) and the file-based v3 layout
(`meta/episodes/**/*.parquet`, shared Parquet/MP4 shards).

See https://huggingface.co/docs/lerobot/lerobot-dataset-v3 for v3 details.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, TypedDict, cast

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.dependencies import av, pil_image
from daft.exceptions import DaftCoreException
from daft.expressions import col, lit
from daft.file import VideoFile
from daft.functions import format, lpad
from daft.functions.file_ import video_file
from daft.series import Series
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


# Safety backstop on frames decoded per shard per batch; clustered seeks stay far below it.
_DECODE_FRAME_BUDGET = 20_000

# Rows per decode batch.
_DECODE_BATCH_SIZE = 16

# Decode straight through target gaps up to this long; seek over anything longer.
# Small gaps are cheaper to decode through than to re-seek (a seek restarts from the
# preceding keyframe). But a shard packs many episodes back to back, so two targets
# in one batch can be minutes apart - decoding through such a gap would waste work
# and could exhaust the frame budget, so we seek instead.
_RESEEK_GAP_S = 10.0


def _decode_one_shard(
    file: VideoFile,
    abs_timestamps: list[tuple[int, float]],
    tolerance: float,
    width: int | None,
    height: int | None,
) -> list[tuple[int, Any]]:
    """Open a single shard once and return ``(row_index, PIL.Image)`` for every requested timestamp.

    All ``abs_timestamps`` refer to the same shard ``file``. The shard is opened
    once; targets are grouped into clusters of nearby timestamps, and each cluster
    is one seek to the preceding keyframe plus one forward decode, keeping the
    closest decoded frame to each target. This is the batched equivalent of the
    old per-row decode: one open + one seek per cluster instead of per frame.
    """
    targets = sorted(abs_timestamps, key=lambda t: t[1])  # ascending by timestamp
    clusters: list[list[tuple[int, float]]] = [[targets[0]]]
    for t in targets[1:]:
        if t[1] - clusters[-1][-1][1] > _RESEEK_GAP_S:
            clusters.append([t])
        else:
            clusters[-1].append(t)

    tail_s = max(0.1, tolerance * 50.0, 1.0 / 24.0)

    # Best (distance, ndarray) seen so far for each target row; at most one frame
    # is retained per target, so memory stays bounded regardless of the span.
    best_dist = {row: float("inf") for row, _ in targets}
    best_arr: dict[int, Any] = {}
    decoded = 0

    with file.open() as f_open, av.open(f_open) as container:
        stream = container.streams.video[0]
        for cluster in clusters:
            earliest = cluster[0][1]
            latest = cluster[-1][1]
            # Match LeRobot: seek backwards to preceding keyframe, then decode forwards.
            container.seek(max(0, int(earliest * av.time_base)), backward=True)
            for vf in container.decode(stream):
                if vf.pts is None:
                    continue
                current_ts = float(vf.pts * stream.time_base)
                arr = None  # convert lazily: most frames improve no target
                for row, target in cluster:
                    dist = abs(current_ts - target)
                    if dist < best_dist[row]:
                        if arr is None:
                            arr = vf.to_ndarray(format="rgb24")
                        best_dist[row] = dist
                        best_arr[row] = arr

                decoded += 1
                if decoded >= _DECODE_FRAME_BUDGET:
                    raise ValueError("Exceeded decode frame budget while aligning to parquet timestamps.")
                if current_ts >= latest + tail_s:
                    break

    if decoded == 0:
        raise ValueError(f"No frames decoded from shard while seeking timestamp {targets[0][1]:.6f}s.")

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


@func.batch(return_dtype=DataType.image(), batch_size=_DECODE_BATCH_SIZE)
def _decode_lerobot_video_timestamp(
    files: Series,  # VideoFile per row
    episode_from_timestamps_s: Series,  # float per row
    frame_timestamps_s: Series,  # float per row
    tolerance_s: float,
    image_width_i: int,
    image_height_i: int,
) -> Any:  # a list with one PIL.Image per row
    """Decode the frame closest to ``from_timestamp + timestamp`` for each row.

    Batched over rows: rows sharing the same shard file are grouped so the shard
    is opened exactly once per batch instead of once per frame.
    """
    if not av.module_available():
        raise ImportError("Decoding LeRobot MP4 shards requires PyAV. Install with `pip install av`.")
    if not pil_image.module_available():
        raise ImportError(
            "Decoding LeRobot MP4 shards requires Pillow. Install with `pip install daft[video]` or `pip install pillow`."
        )

    file_list = files.to_pylist()
    from_list = episode_from_timestamps_s.to_pylist()
    frame_list = frame_timestamps_s.to_pylist()

    tolerance = float(tolerance_s)
    width_i = int(image_width_i)
    height_i = int(image_height_i)
    width = width_i if width_i > 0 and height_i > 0 else None
    height = height_i if width_i > 0 and height_i > 0 else None

    # Group row indices by shard path so each shard is opened once. This assumes
    # files with the same path are interchangeable - true for the plain-URL
    # VideoFiles that read() constructs, but not for byte-range slices or files
    # with differing io_configs, which would need the full file identity as key.
    by_shard: dict[str, list[tuple[int, float]]] = {}
    shard_file: dict[str, VideoFile] = {}
    for i, file in enumerate(file_list):
        abs_ts = float(from_list[i]) + float(frame_list[i])
        by_shard.setdefault(file.path, []).append((i, abs_ts))
        shard_file[file.path] = file

    results: list[Any] = [None] * len(file_list)
    for path, targets in by_shard.items():
        for row, img in _decode_one_shard(shard_file[path], targets, tolerance, width, height):
            results[row] = img
    return results


class Feature(TypedDict):
    dtype: str


class LeRobotInfo(TypedDict, total=False):
    codebase_version: str
    data_path: str
    video_path: str
    fps: float
    features: dict[str, Feature]
    chunks_size: int


_PATH_PLACEHOLDER = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([^}]+))?\}")
_VERSION = re.compile(r"^v(\d+)(?:\.(\d+))?$")

_DEFAULT_CHUNKS_SIZE = 1000
_V2_VIDEO_PATH = "videos/chunk-{episode_chunk:03d}/{video_key}/episode_{episode_index:06d}.mp4"


def _parse_version(codebase_version: str) -> tuple[int, int]:
    """Parse ``v2.1`` / ``v3.0`` into ``(major, minor)``."""
    m = _VERSION.fullmatch(codebase_version.strip())
    if not m:
        raise ValueError(
            f"Unrecognized LeRobot codebase_version {codebase_version!r}; expected a string like 'v2.1' or 'v3.0'."
        )
    return int(m.group(1)), int(m.group(2) or 0)


def _read_info(normalized_uri: str, io_config: IOConfig | None = None) -> LeRobotInfo:
    with daft.open_file(f"{normalized_uri}/meta/info.json", io_config=io_config) as f:
        info = cast("LeRobotInfo", json.load(f))
        version = info.get("codebase_version", "")
        major, _minor = _parse_version(version)
        if major not in (2, 3):
            raise ValueError(
                f"`daft.datasets.lerobot` currently supports LeRobot datasets v2.0, v2.1, and v3.x (got {version!r})"
            )
        return info


def _is_v3(info: LeRobotInfo) -> bool:
    return _parse_version(info["codebase_version"])[0] >= 3


def _video_keys(info: LeRobotInfo) -> list[str]:
    return [name for name, feat_info in info.get("features", {}).items() if feat_info["dtype"] == "video"]


def _format_path_template(
    template: str,
    *,
    root: str,
    bindings: dict[str, Any],
) -> Any:
    """Turn a LeRobot ``info.json`` path template into a Daft string expression.

    Numeric placeholders such as ``{episode_index:06d}`` are padded; string
    bindings (for example ``video_key``) are spliced in as literals.
    """
    format_string = ""
    args: list[Any] = []
    last = 0
    for m in _PATH_PLACEHOLDER.finditer(template):
        format_string += template[last : m.start()]
        name, spec = m.group(1), m.group(2)
        if name not in bindings:
            raise ValueError(f"Unknown placeholder {{{name}}} in LeRobot path template {template!r}")
        val = bindings[name]
        if isinstance(val, str):
            format_string += val
        else:
            if spec and spec.endswith("d"):
                width = int(spec[:-1]) if spec[:-1] else 0
                val = lpad(val.cast(DataType.string), width, "0")
            else:
                val = val.cast(DataType.string)
            format_string += "{}"
            args.append(val)
        last = m.end()
    format_string += template[last:]
    prefix = root.rstrip("/") + "/"
    if not args:
        return lit(prefix + format_string)
    return format(prefix + format_string, *args)


def _v2_path_bindings(info: LeRobotInfo, video_key: str | None = None) -> dict[str, Any]:
    chunks_size = int(info.get("chunks_size") or _DEFAULT_CHUNKS_SIZE)
    episode_index = col("episode_index")
    chunk = episode_index // lit(chunks_size)
    bindings: dict[str, Any] = {
        "episode_index": episode_index,
        "episode_chunk": chunk,
        "chunk_index": chunk,
    }
    if video_key is not None:
        bindings["video_key"] = video_key
    return bindings


@PublicAPI
def read(
    dataset_uri: str,
    io_config: IOConfig | None = None,
    include_stats: bool = False,
    load_video_frames: str | list[str] | bool = False,
) -> DataFrame:
    """Read a LeRobot v2 or v3 dataset as a lazy DataFrame with one row per frame.

    Reads per-episode metadata and the per-frame sensor data under ``data``,
    joins them on ``episode_index``, and broadcasts each episode's metadata
    across its frames. Optionally decodes the matching video frame for one or
    more camera keys into an image column.

    v2.0/v2.1 datasets store one Parquet/MP4 file per episode. v3 packs many
    episodes into shared shards.

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
            video_keys = _video_keys(info)
        elif isinstance(load_video_frames, str):
            video_keys = [load_video_frames]
        elif isinstance(load_video_frames, list) and all(isinstance(k, str) for k in load_video_frames):
            video_keys = load_video_frames
        else:
            raise ValueError(f"Invalid value provided for argument load_video_frames=`{load_video_frames}`")

        # Seek by absolute timestamp inside the MP4: `from_timestamp` (where this
        # episode begins in the file) + the per-frame episode-local `timestamp`.
        # v3 shards pack many episodes back to back, so `from_timestamp` is the
        # offset within the shard. v2 stores one MP4 per episode, so
        # `from_timestamp` is 0 and the episode-local timestamp is already
        # absolute in that file. Seeking by `frame_index` is wrong for v3 (it
        # only happens to work for the first episode in each shard).
        fps = float(info["fps"])
        tolerance_s = 1.0 / fps / 2.0  # half a frame period: any closer frame is unambiguously "the" frame

        df = df.into_batches(_DECODE_BATCH_SIZE)
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
    """Read LeRobot episode metadata as a lazy DataFrame (one row per episode).

    v3 reads ``meta/episodes/**/*.parquet``. v2.0/v2.1 reads
    ``meta/episodes.jsonl``. Extra per-episode fields present in that metadata
    are kept as columns.

    Args:
        dataset_uri: Huggingface repo id (`org/name`),
            or a local / remote directory (`s3://...`, `hf://datasets/...`)
        io_config: Optional IO configuration for remote reads.
        include_meta: If True, keep the internal ``meta/episodes/*`` columns
            (v3 chunk/file indices locating each episode's own metadata shard).
            No effect on v2, which has no such bookkeeping columns. Defaults to
            False.
        include_stats: If True, keep per-episode statistics. On v3 these are
            ``stats/*`` columns in the episode parquet; on v2.1 they are joined
            from ``meta/episodes_stats.jsonl``. Defaults to False.
        include_video_metadata: If True, keep the per-episode ``videos/{key}/*``
            columns (chunk/file indices and from/to timestamps locating each
            episode's footage). On v2, ``from_timestamp`` is 0 because each
            episode has its own MP4. Defaults to False.

    Returns:
        Lazy DataFrame of episode metadata, one row per episode. Always includes
        a ``videos/{key}/video`` file-handle column per video feature; the
        ``include_*`` flags control which additional column families are kept.
    """
    root = _normalize_dataset_root(dataset_uri)
    info = _read_info(root, io_config=io_config)
    if _is_v3(info):
        return _read_episodes_v3(
            root,
            info,
            io_config=io_config,
            include_meta=include_meta,
            include_stats=include_stats,
            include_video_metadata=include_video_metadata,
        )
    return _read_episodes_v2(
        root,
        info,
        io_config=io_config,
        include_stats=include_stats,
        include_video_metadata=include_video_metadata,
    )


def _read_episodes_v3(
    root: str,
    info: LeRobotInfo,
    *,
    io_config: IOConfig | None,
    include_meta: bool,
    include_stats: bool,
    include_video_metadata: bool,
) -> DataFrame:
    df = daft.read_parquet(f"{root}/meta/episodes/**/*.parquet", io_config=io_config)
    if not include_meta:
        df = df.exclude(*(c for c in df.column_names if c.startswith("meta/")))
    if not include_stats:
        df = df.exclude(*(c for c in df.column_names if c.startswith("stats/")))

    for key in _video_keys(info):
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


def _read_episodes_v2(
    root: str,
    info: LeRobotInfo,
    *,
    io_config: IOConfig | None,
    include_stats: bool,
    include_video_metadata: bool,
) -> DataFrame:
    df = daft.read_json(f"{root}/meta/episodes.jsonl", io_config=io_config)

    if include_stats:
        try:
            stats_df = daft.read_json(f"{root}/meta/episodes_stats.jsonl", io_config=io_config)
            df = df.join(stats_df, on="episode_index")
        except (OSError, DaftCoreException, FileNotFoundError):
            pass

    video_path = info.get("video_path") or _V2_VIDEO_PATH
    bindings = _v2_path_bindings(info)
    for key in _video_keys(info):
        bindings["video_key"] = key
        file_name_expr = _format_path_template(video_path, root=root, bindings=bindings)
        df = df.with_column(f"videos/{key}/video", video_file(file_name_expr, verify=False, io_config=io_config))
        # One MP4 per episode: episode-local timestamps are already absolute.
        df = df.with_column(f"videos/{key}/from_timestamp", lit(0.0))

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

    Reads the per-frame parquet under ``data/**`` (v3 shared shards or v2
    per-episode files) and joins it to the provided episode metadata on
    ``episode_index``, producing one row per frame. Episode metadata is
    broadcast across each episode's frames.

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
