"""LeRobot Dataset v3.0 helpers for `daft.datasets`.

This module reads the file-based LeRobot v3 layout (``meta/episodes``, ``data``,
``videos``) and exposes episode-level scans plus frame expansion utilities.

See https://huggingface.co/docs/lerobot/lerobot-dataset-v3 for format details.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any

from daft.api_annotations import PublicAPI
from daft.convert import from_pydict, from_pylist
from daft.datatype import DataType
from daft.exceptions import DaftCoreException
from daft.expressions import Expression, col, lit
from daft.functions import lpad
from daft.io import read_parquet

if TYPE_CHECKING:
    from daft.daft import IOConfig
    from daft.dataframe import DataFrame

__all__ = [
    "episodes",
    "load_episode_frames",
    "read_info",
    "read_stats",
    "read_tasks",
]

# Column names used by Hugging Face LeRobot v3 metadata / data shards.
_DATA_CHUNK_CANDIDATES = ("data/chunk_index", "data_chunk_index")
_DATA_FILE_CANDIDATES = ("data/file_index", "data_file_index")

_LEROBOT_ROOT_COL = "lerobot_dataset_root"
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


def _read_json_object(root: str, relative_path: str) -> dict[str, Any]:
    """Load a small JSON object from ``{root}/{relative_path}`` (local or hf://datasets)."""
    https_base = _https_base_for_hf_datasets_root(root)
    if https_base is not None:
        url = f"{https_base}/{relative_path.lstrip('/')}"
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise FileNotFoundError(f"Failed to download JSON from {url!r}: {e}") from e

    path = Path(root) / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"Missing file at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl_records_local(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing file at {path}")
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _read_jsonl_records_remote(url: str) -> list[dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            text = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise FileNotFoundError(f"Failed to download JSONL from {url!r}: {e}") from e
    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


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
    chunk_str = lpad(col(chunk_col).cast(DataType.string()), 3, "0")
    file_str = lpad(col(file_col).cast(DataType.string()), 3, "0")
    return (
        root_expr.cast(DataType.string()) + lit("/data/chunk-") + chunk_str + lit("/file-") + file_str + lit(".parquet")
    )


@PublicAPI
def episodes(
    dataset_uri: str,
    io_config: IOConfig | None = None,
    *,
    dataset_path_column: str | None = None,
) -> DataFrame:
    """Load LeRobot v3 episode metadata as a lazy DataFrame (one row per episode).

    This reads ``meta/episodes/**/*.parquet`` under the dataset root and adds
    ``lerobot_dataset_root`` so downstream helpers can build ``data/`` and
    ``videos/`` paths without threading the root string manually.

    Args:
        dataset_uri: Local directory, ``hf://datasets/org/name`` URI, or bare
            ``org/name`` which is treated as a Hub dataset id.
        io_config: Optional IO configuration for remote reads.
        dataset_path_column: If set, include the resolved dataset root string in
            a column with this name (in addition to ``lerobot_dataset_root``).

    Returns:
        Lazy episode metadata DataFrame.
    """
    root = _normalize_dataset_root(dataset_uri)
    meta_glob = f"{root}/meta/episodes/**/*.parquet"
    df = read_parquet(meta_glob, io_config=io_config)
    df = df.with_column(_LEROBOT_ROOT_COL, lit(root))
    if dataset_path_column is not None:
        df = df.with_column(dataset_path_column, lit(root))
    return df


@PublicAPI
def load_episode_frames(
    episodes: DataFrame,
    *,
    io_config: IOConfig | None = None,
    columns: list[str] | None = None,
) -> DataFrame:
    """Expand filtered episode rows into frame-level rows from ``data/`` Parquet shards.

    This executes a small eager step to discover distinct shard paths from the
    current logical plan, then lazily reads only those Parquet files and keeps
    rows whose ``episode_index`` appears in ``episodes``.

    Preconditions:

    - ``episodes`` must include ``lerobot_dataset_root`` (added by :func:`episodes`)
      plus either ``data/chunk_index`` / ``data/file_index`` (canonical LeRobot v3)
      or the ``data_chunk_index`` / ``data_file_index`` spelling.

    Args:
        episodes: Episode-level dataframe (typically filtered) from :func:`episodes`.
        io_config: Optional IO configuration for remote reads.
        columns: Optional projection of frame columns (passed to :meth:`daft.DataFrame.select`).

    Returns:
        Lazy frame-level dataframe.
    """
    if _LEROBOT_ROOT_COL not in episodes.column_names:
        raise ValueError(
            f"Missing {_LEROBOT_ROOT_COL!r} column on episodes dataframe. "
            "Construct episodes via daft.datasets.lerobot.episodes(...)."
        )

    chunk_col = _pick_first_column(episodes, _DATA_CHUNK_CANDIDATES)
    file_col = _pick_first_column(episodes, _DATA_FILE_CANDIDATES)

    with_paths = episodes.with_column(
        _DATA_PATH_COL,
        _data_parquet_path_expr(col(_LEROBOT_ROOT_COL), chunk_col, file_col),
    )

    paths = with_paths.select(_DATA_PATH_COL).distinct().to_pydict()[_DATA_PATH_COL]
    if len(paths) == 0:
        empty_cols = list(columns) if columns is not None else ["episode_index", "frame_index", "timestamp"]
        return from_pydict({c: [] for c in empty_cols})

    frames = read_parquet(paths, io_config=io_config)
    if columns is not None:
        frames = frames.select(*columns)
    allowed = with_paths.select("episode_index").distinct()
    return frames.join(allowed, on="episode_index", how="inner")


@PublicAPI
def read_info(dataset_uri: str) -> dict[str, Any]:
    """Load ``meta/info.json`` for a LeRobot v3 dataset."""
    root = _normalize_dataset_root(dataset_uri)
    return _read_json_object(root, "meta/info.json")


@PublicAPI
def read_stats(dataset_uri: str) -> dict[str, Any]:
    """Load ``meta/stats.json`` for a LeRobot v3 dataset."""
    root = _normalize_dataset_root(dataset_uri)
    return _read_json_object(root, "meta/stats.json")


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
            return read_parquet(pq_url, io_config=io_config)
        except (OSError, DaftCoreException, FileNotFoundError):
            url = f"{https_base}/meta/tasks.jsonl"
            return from_pylist(_read_jsonl_records_remote(url))

    pq_path = Path(root) / "meta" / "tasks.parquet"
    if pq_path.is_file():
        return read_parquet(str(pq_path), io_config=io_config)

    jsonl_path = Path(root) / "meta" / "tasks.jsonl"
    if jsonl_path.is_file():
        return from_pylist(_read_jsonl_records_local(jsonl_path))

    raise FileNotFoundError(f"No tasks metadata found under {root}/meta (tasks.parquet or tasks.jsonl)")
