"""LeRobot Dataset v3.0 helpers for `daft.datasets`.

This module reads the file-based LeRobot v3 layout (`meta/episodes`, `data`,
`videos`) and exposes episode-level scans plus frame expansion utilities.

See https://huggingface.co/docs/lerobot/lerobot-dataset-v3 for format details.
"""

from __future__ import annotations

import re
import json
from typing import TYPE_CHECKING, Any, TypedDict, cast

import daft
from daft.api_annotations import PublicAPI
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.file import VideoFile
from daft.exceptions import DaftCoreException
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


class Feature(TypedDict):
    dtype: str

class LeRobotInfo(TypedDict):
    codebase_version: str
    data_path: str
    video_path: str
    features: dict[str, Feature]


def _read_info(normalized_uri: str, io_config: IOConfig | None = None) -> LeRobotInfo:
    with daft.open_file(f"{normalized_uri}/meta/info.json", io_config=io_config) as f:
        info = cast(LeRobotInfo, json.load(f))
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
    """Read LeRobot v3 episode metadata as a lazy DataFrame (one row per frame with episode metadata)."""
    root = _normalize_dataset_root(dataset_uri)

    episode_df = daft.datasets.lerobot.read_episodes(dataset_uri, io_config=io_config, include_stats=include_stats)
    frame_df = daft.read_parquet(f"{root}/data/**")
    df = episode_df.join(frame_df, on=["episode_index"])
    df = df.exclude("data/chunk_index", "data/file_index")

    # Load video frames into memory
    if load_video_frames is not False:
        if load_video_frames is True:
            video_keys = []  # TODO
        elif isinstance(load_video_frames, str):
            video_keys = [load_video_frames]
        elif isinstance(load_video_frames, list) and all(isinstance(k, str) for k in load_video_frames):
            video_keys = load_video_frames
        else:
            raise ValueError("TODO")

        # To increase parallelism, reduce batch size
        df = df.into_batches(16)  # TODO: Set it in the batch UDF instead?
        for k in video_keys:
            # TODO: Optimize by using a batch UDF to avoid opening the same video multiple times
            df = df.with_column(k, get_video_frame_by_idx(f"videos/{k}/video", col("frame_idx")))
            df = df.exclude(f"videos/{k}/video")

        # TODO: What about raw images, what do i do about them? Is that a thing in LeRobot v3

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

    Returns:
        Lazy DataFrame of episode metadata.
    """
    root = _normalize_dataset_root(dataset_uri)
    info = _read_info(root, io_config=io_config)

    # TODO: What is the `meta` episodes into used for? How is it different from the `videos` info?
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
