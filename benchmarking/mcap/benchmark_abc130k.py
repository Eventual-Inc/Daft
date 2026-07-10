"""Benchmark MCAP metadata and reader pushdowns on a pinned ABC-130k episode.

The benchmark deliberately starts from an exact episode URI. Dataset discovery is
reported separately through ``daft.from_glob_path`` and ``daft.datasets.abc.raw``;
message stages compare direct ``daft.read_mcap`` with the bounded
``daft.datasets.abc.messages`` wrapper.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

ABC_REVISION = "29136bc9b9e38d320b00ffcddbbe4cd0e3278c58"
SMOKE_EPISODE_URI = (
    f"hf://datasets/XDOF/ABC-130k@{ABC_REVISION}/data/train/"
    "clip_the_socks_to_the_hanger/episode_5b33995f-ba4a-49f8-bfb7-c6c034df0865/episode.mcap"
)
DEFAULT_EPISODE_URI = (
    f"hf://datasets/XDOF/ABC-130k@{ABC_REVISION}/data/train/"
    "clip_the_socks_to_the_hanger/episode_0ab8f08a-eb80-40f4-99f7-3d83db25df83/episode.mcap"
)
MIB = 1024 * 1024


class BenchmarkSkip(RuntimeError):
    """A missing external prerequisite, rather than a benchmark failure."""


@dataclass
class StagePayload:
    rows: int | None = None
    source_file_bytes: int | None = None
    arrow_bytes: int | None = None
    payload_value_bytes: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    stage: str
    iteration: int
    elapsed_seconds: float
    rows: int | None = None
    source_file_bytes: int | None = None
    arrow_bytes: int | None = None
    payload_value_bytes: int | None = None
    rows_per_second: float | None = None
    payload_mib_per_second: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FilterWindow:
    topics: tuple[str, ...]
    start_time: int
    end_time: int
    topic_selection: str
    time_selection: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=DEFAULT_EPISODE_URI, help="Exact, preferably revision-pinned MCAP URI")
    parser.add_argument(
        "--mode",
        choices=("remote-cold", "remote-warm", "local"),
        default="remote-warm",
        help="Cold uses a fresh process-local HF cache; warm runs an unrecorded warmup; local uses --local-mirror",
    )
    parser.add_argument(
        "--local-mirror",
        type=Path,
        help="Local episode.mcap, or an ABC-130k mirror root containing data/<split>/...",
    )
    parser.add_argument("--topic", action="append", dest="topics", help="Topic to select; repeat for multiple topics")
    parser.add_argument(
        "--video-topic",
        help="Foxglove CompressedVideo topic to benchmark; defaults to the first matching indexed schema",
    )
    parser.add_argument(
        "--video-frame-limit",
        type=int,
        default=32,
        help="Maximum decoded frames to materialize per video stage (default: 32)",
    )
    parser.add_argument("--start-time", type=int, help="Inclusive MCAP log-time bound")
    parser.add_argument("--end-time", type=int, help="Exclusive MCAP log-time bound")
    parser.add_argument(
        "--time-window-fraction",
        type=float,
        default=0.01,
        help="Auto-selected centered time-window fraction when bounds are omitted (default: 0.01)",
    )
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--repeat", type=int, default=1, help="Recorded repetitions of every message stage")
    parser.add_argument("--threads", type=int, help="Native runner worker threads")
    parser.add_argument("--no-xet", action="store_true", help="Use Hugging Face HTTP rather than Xet")
    parser.add_argument(
        "--payload-scope",
        choices=("filtered", "full"),
        default="filtered",
        help="Materialize payloads for the selective query or the complete episode",
    )
    parser.add_argument("--payload-limit", type=int, default=0, help="Optional row limit for payload materialization")
    parser.add_argument("--include-plan", action="store_true", help="Include Daft's optimized planner output in JSON")
    parser.add_argument("--output", type=Path, help="Also write the JSON report to this path")
    parser.add_argument("--keep-cold-cache", action="store_true", help="Do not remove the temporary cold cache")
    args = parser.parse_args()

    if (args.start_time is None) != (args.end_time is None):
        parser.error("--start-time and --end-time must be supplied together")
    if args.start_time is not None and args.start_time >= args.end_time:
        parser.error("--start-time must be less than --end-time")
    if not 0 < args.time_window_fraction <= 1:
        parser.error("--time-window-fraction must be in (0, 1]")
    if args.batch_size <= 0 or args.repeat <= 0 or args.payload_limit < 0 or args.video_frame_limit <= 0:
        parser.error(
            "--batch-size, --repeat, and --video-frame-limit must be positive; --payload-limit cannot be negative"
        )
    if args.mode == "local" and args.local_mirror is None:
        parser.error("--mode local requires --local-mirror")
    return args


def configure_cold_cache(args: argparse.Namespace) -> Path | None:
    """Set cache variables before importing Daft or its HF/Xet clients."""
    if args.mode != "remote-cold":
        return None
    root = Path(tempfile.mkdtemp(prefix="daft-mcap-abc130k-cold-"))
    os.environ["HF_HOME"] = str(root / "hf-home")
    os.environ["HF_HUB_CACHE"] = str(root / "hub")
    os.environ["HF_XET_CACHE"] = str(root / "xet")
    return root


def local_episode_path(uri: str, mirror: Path) -> str:
    mirror = mirror.expanduser().resolve()
    if mirror.is_file():
        return str(mirror)
    if not mirror.is_dir():
        raise BenchmarkSkip(f"Local mirror does not exist: {mirror}")
    if "@" not in uri or "/" not in uri.split("@", 1)[1]:
        raise BenchmarkSkip("A mirror directory requires a revision-pinned hf:// URI")
    relative = uri.split("@", 1)[1].split("/", 1)[1]
    episode = mirror / relative
    if not episode.is_file():
        raise BenchmarkSkip(f"Pinned episode is absent from the local mirror: {episode}")
    return str(episode)


def abc_identity(path: str) -> dict[str, str]:
    parts = path.rstrip("/").split("/")
    try:
        data_index = parts.index("data")
        return {
            "split": parts[data_index + 1],
            "task": parts[data_index + 2],
            "episode_id": parts[data_index + 3],
            "filename": parts[data_index + 4],
        }
    except (ValueError, IndexError):
        return {}


def hf_revision(path: str) -> str | None:
    if not path.startswith("hf://") or "@" not in path:
        return None
    return path.split("@", 1)[1].split("/", 1)[0]


def abc_dataset_context(path: str) -> dict[str, str] | None:
    """Return the dataset root and path-derived identity for an ABC episode."""
    identity = abc_identity(path)
    if not identity or "/data/" not in path:
        return None
    return {
        "root": path.split("/data/", 1)[0],
        "split": identity["split"],
        "task": identity["task"],
        "episode_id": identity["episode_id"].removeprefix("episode_"),
    }


def git_revision() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def is_gated_access_error(error: BaseException) -> bool:
    messages: list[str] = []
    current: BaseException | None = error
    while current is not None:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    text = " ".join(messages)
    return any(
        marker in text
        for marker in (
            "gated repo",
            "gated dataset",
            "401",
            "403",
            "unauthorized",
            "access to this resource is restricted",
        )
    )


def timed_stage(stage: str, iteration: int, function: Callable[[], StagePayload]) -> StageResult:
    print(f"[{iteration}] {stage} ...", file=sys.stderr, flush=True)
    started = time.perf_counter()
    payload = function()
    elapsed = time.perf_counter() - started
    rows_per_second = None if payload.rows is None or elapsed == 0 else payload.rows / elapsed
    payload_rate = (
        None if payload.payload_value_bytes is None or elapsed == 0 else payload.payload_value_bytes / MIB / elapsed
    )
    result = StageResult(
        stage=stage,
        iteration=iteration,
        elapsed_seconds=elapsed,
        rows=payload.rows,
        source_file_bytes=payload.source_file_bytes,
        arrow_bytes=payload.arrow_bytes,
        payload_value_bytes=payload.payload_value_bytes,
        rows_per_second=rows_per_second,
        payload_mib_per_second=payload_rate,
        details=payload.details,
    )
    print(f"[{iteration}] {stage}: {elapsed:.3f}s, rows={payload.rows}", file=sys.stderr, flush=True)
    return result


def choose_filter(metadata: dict[str, Any], args: argparse.Namespace) -> FilterWindow:
    channels = metadata.get("channels", [])
    if args.topics:
        topics = tuple(dict.fromkeys(args.topics))
        topic_selection = "explicit"
    else:
        counted = [
            channel
            for channel in channels
            if channel.get("topic") and isinstance(channel.get("message_count"), int) and channel["message_count"] > 0
        ]
        if counted:
            # A high-rate channel is likely to intersect the centered time window;
            # time selectivity still comes from the default 1% interval.
            selected = max(counted, key=lambda channel: (channel["message_count"], -channel["id"]))
            topics = (selected["topic"],)
            topic_selection = "busiest_nonempty_indexed_channel"
        elif channels:
            topics = (channels[0]["topic"],)
            topic_selection = "first_channel_without_counts"
        else:
            raise BenchmarkSkip("The MCAP summary has no channels; pass --topic only after checking the file")

    if args.start_time is not None:
        start_time, end_time = args.start_time, args.end_time
        time_selection = "explicit"
    else:
        stats = metadata.get("statistics")
        if stats is None:
            raise BenchmarkSkip("The MCAP summary has no statistics; pass --start-time and --end-time")
        file_start = int(stats["message_start_time"])
        # MCAP statistics report inclusive extrema; read_mcap's end bound is exclusive.
        file_end_exclusive = int(stats["message_end_time"]) + 1
        duration = max(1, file_end_exclusive - file_start)
        width = max(1, round(duration * args.time_window_fraction))
        start_time = file_start + (duration - width) // 2
        end_time = min(file_end_exclusive, start_time + width)
        time_selection = f"centered_{args.time_window_fraction:g}_fraction"

    return FilterWindow(
        topics=topics,
        start_time=int(start_time),
        end_time=int(end_time),
        topic_selection=topic_selection,
        time_selection=time_selection,
    )


def choose_video_topic(metadata: dict[str, Any], args: argparse.Namespace) -> tuple[str | None, str]:
    if args.video_topic:
        return args.video_topic, "explicit"
    for channel in metadata.get("channels", []):
        if channel.get("topic") and channel.get("schema_name") == "foxglove.CompressedVideo":
            return str(channel["topic"]), "first_indexed_foxglove_compressed_video"
    return None, "no_indexed_foxglove_compressed_video_channel"


def arrow_filter_count(table: Any, window: FilterWindow) -> int:
    import pyarrow as pa
    import pyarrow.compute as pc

    topic_set = pa.array(window.topics, type=table["topic"].type)
    mask = pc.is_in(table["topic"], value_set=topic_set)
    mask = pc.and_(mask, pc.greater_equal(table["log_time"], pa.scalar(window.start_time)))
    mask = pc.and_(mask, pc.less(table["log_time"], pa.scalar(window.end_time)))
    return int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)


def payload_sizes(table: Any) -> tuple[int, str]:
    values = table["data"].to_pylist()
    payload_bytes = 0
    kinds: set[str] = set()
    for value in values:
        if value is None:
            kinds.add("null")
        elif isinstance(value, str):
            kinds.add("string")
            payload_bytes += len(value.encode("utf-8"))
        else:
            kinds.add("binary")
            payload_bytes += len(value)
    return payload_bytes, "+".join(sorted(kinds)) or "empty"


def build_io_config(daft: Any, path: str, args: argparse.Namespace) -> Any:
    if not path.startswith("hf://"):
        return None
    token = os.environ.get("HF_TOKEN")
    if "XDOF/ABC-130k" in path and not token:
        raise BenchmarkSkip(
            "ABC-130k is gated. Accept its Hugging Face terms, then export HF_TOKEN before running; no data was read."
        )
    return daft.io.IOConfig(hf=daft.io.HuggingFaceConfig(token=token, use_xet=not args.no_xet))


def read_df(daft: Any, path: str, io_config: Any, args: argparse.Namespace, **filters: Any) -> Any:
    return daft.read_mcap(
        path,
        io_config=io_config,
        batch_size=args.batch_size,
        **filters,
    )


def planner_query(daft: Any, path: str, io_config: Any, args: argparse.Namespace, window: FilterWindow) -> Any:
    predicate = daft.col("topic").is_in(list(window.topics))
    predicate = predicate & (daft.col("log_time") >= window.start_time)
    predicate = predicate & (daft.col("log_time") < window.end_time)
    return read_df(daft, path, io_config, args).where(predicate)


def metadata_projection(dataframe: Any) -> Any:
    columns = ["topic", "log_time"]
    if "source_path" in dataframe.column_names:
        columns.append("source_path")
    return dataframe.select(*columns)


def provenance_details(table: Any) -> dict[str, Any]:
    if "source_path" not in table.column_names:
        return {"source_path_column": False}
    import pyarrow.compute as pc

    return {
        "source_path_column": True,
        "source_paths": pc.unique(table["source_path"]).to_pylist(),
    }


def object_scan(daft: Any, path: str, io_config: Any) -> tuple[StagePayload, int]:
    table = daft.from_glob_path(path, io_config=io_config).to_arrow()
    if table.num_rows == 0:
        raise BenchmarkSkip(f"No object matched the exact benchmark path: {path}")
    sizes = [value for value in table["size"].to_pylist() if value is not None]
    source_bytes = int(sum(sizes))
    payload = StagePayload(
        rows=table.num_rows,
        source_file_bytes=source_bytes,
        arrow_bytes=table.nbytes,
        details={
            "objects": table.select(
                [name for name in ("path", "size", "rows") if name in table.column_names]
            ).to_pylist()
        },
    )
    return payload, source_bytes


def metadata_sniff(daft: Any, path: str, io_config: Any) -> tuple[StagePayload, dict[str, Any]]:
    metadata = daft.McapFile(path, io_config=io_config).summary()
    statistics = metadata.get("statistics")
    details = {
        "has_summary": metadata.get("has_summary"),
        "has_chunk_indexes": metadata.get("has_chunk_indexes"),
        "has_message_indexes": metadata.get("has_message_indexes"),
        "schema_count": len(metadata.get("schemas", [])),
        "channel_count": len(metadata.get("channels", [])),
        "chunk_count": len(metadata.get("chunks", [])),
        "message_count": None if statistics is None else statistics.get("message_count"),
        "message_start_time": None if statistics is None else statistics.get("message_start_time"),
        "message_end_time": None if statistics is None else statistics.get("message_end_time"),
        "topics": [channel.get("topic") for channel in metadata.get("channels", [])],
    }
    return StagePayload(source_file_bytes=metadata.get("file_size"), details=details), metadata


def abc_catalog_discovery(
    daft: Any,
    path: str,
    io_config: Any,
    source_bytes: int,
) -> tuple[StagePayload, Any]:
    """Discover one task catalog, then retain the exact benchmark episode."""
    from daft.datasets import abc

    context = abc_dataset_context(path)
    if context is None:
        raise ValueError("ABC catalog discovery requires a path under data/<split>/<task>/episode_<id>")

    catalog = abc.raw(
        context["root"],
        split=cast(Literal["train", "val"], context["split"]),
        tasks=[context["task"]],
        include_annotations=True,
        io_config=io_config,
    )
    bounded = catalog.where(daft.col("episode_id") == context["episode_id"]).limit(1).collect()
    scalar_columns = [
        name
        for name in (
            "episode_id",
            "split",
            "task_slug",
            "episode_dir",
            "episode_path",
            "episode_size",
            "annotated",
            "annotation_path",
            "annotation_size",
        )
        if name in bounded.column_names
    ]
    table = bounded.select(*scalar_columns).to_arrow()
    if table.num_rows != 1:
        raise RuntimeError(
            "The task-scoped ABC catalog did not resolve the exact benchmark episode: "
            f"expected 1 row, received {table.num_rows}"
        )
    return (
        StagePayload(
            rows=table.num_rows,
            source_file_bytes=source_bytes,
            arrow_bytes=table.nbytes,
            details={
                "api": "daft.datasets.abc.raw",
                "catalog_root": context["root"],
                "direct_reader_revision": hf_revision(path),
                "catalog_root_revision": hf_revision(context["root"]),
                "downstream_episode_path": path,
                "catalog_and_reader_share_root": context["root"] == path.split("/data/", 1)[0],
                "discovery_scope": {"split": context["split"], "tasks": [context["task"]]},
                "bounded_selection": {"episode_id": context["episode_id"], "limit": 1},
                "columns": scalar_columns,
                "episode": table.to_pylist()[0],
                "payload_read": False,
            },
        ),
        bounded,
    )


def abc_bounded_metadata(catalog: Any, source_bytes: int) -> StagePayload:
    """Range-read normalized metadata only for the already bounded catalog."""
    from daft.datasets import abc

    dataframe = abc.metadata(catalog)
    metadata_columns = [
        name
        for name in (
            "episode_id",
            "split",
            "task_slug",
            "session_id",
            "task_name",
            "duration_seconds",
            "message_count",
            "message_start_time",
            "message_end_time",
            "chunk_count",
            "topics",
            "video_topics",
            "indexed",
        )
        if name in dataframe.column_names
    ]
    table = dataframe.select(*metadata_columns).to_arrow()
    if table.num_rows != 1:
        raise RuntimeError(f"Expected metadata for one bounded ABC episode, received {table.num_rows} rows")
    return StagePayload(
        rows=table.num_rows,
        source_file_bytes=source_bytes,
        arrow_bytes=table.nbytes,
        details={
            "api": "daft.datasets.abc.metadata",
            "input_episode_rows": 1,
            "columns": metadata_columns,
            "metadata": table.to_pylist()[0],
            "payload_read": False,
        },
    )


def run_iteration(
    daft: Any,
    path: str,
    io_config: Any,
    args: argparse.Namespace,
    window: FilterWindow,
    source_bytes: int,
    iteration: int,
    video_topic: str | None,
    abc_catalog: Any | None,
) -> list[StageResult]:
    results: list[StageResult] = []

    def full_count() -> StagePayload:
        rows = read_df(daft, path, io_config, args).count_rows()
        return StagePayload(rows=rows, source_file_bytes=source_bytes)

    results.append(timed_stage("read_mcap_full_count", iteration, full_count))

    baseline_table: Any = None

    def baseline_scan() -> StagePayload:
        nonlocal baseline_table
        baseline_table = metadata_projection(read_df(daft, path, io_config, args)).to_arrow()
        selected_rows = arrow_filter_count(baseline_table, window)
        return StagePayload(
            rows=selected_rows,
            source_file_bytes=source_bytes,
            arrow_bytes=baseline_table.nbytes,
            details={
                "rows_scanned": baseline_table.num_rows,
                "filter_execution": "post_scan_pyarrow",
                **provenance_details(baseline_table),
            },
        )

    baseline = timed_stage("read_mcap_unpushed_filter_baseline", iteration, baseline_scan)
    results.append(baseline)

    def filtered_read(stage: str, **filters: Any) -> StageResult:
        def execute() -> StagePayload:
            table = metadata_projection(read_df(daft, path, io_config, args, **filters)).to_arrow()
            return StagePayload(
                rows=table.num_rows,
                source_file_bytes=source_bytes,
                arrow_bytes=table.nbytes,
                details={"reader_filters": filters, **provenance_details(table)},
            )

        return timed_stage(stage, iteration, execute)

    results.append(filtered_read("read_mcap_topic_filter", topics=list(window.topics)))
    results.append(filtered_read("read_mcap_time_filter", start_time=window.start_time, end_time=window.end_time))
    reader_filtered = filtered_read(
        "read_mcap_topic_time_filter",
        topics=list(window.topics),
        start_time=window.start_time,
        end_time=window.end_time,
    )
    results.append(reader_filtered)

    def planned_read() -> StagePayload:
        query = metadata_projection(planner_query(daft, path, io_config, args, window))
        table = query.to_arrow()
        details: dict[str, Any] = {"filter_execution": "daft_where", **provenance_details(table)}
        return StagePayload(
            rows=table.num_rows,
            source_file_bytes=source_bytes,
            arrow_bytes=table.nbytes,
            details=details,
        )

    planner_filtered = timed_stage("read_mcap_planner_topic_time_filter", iteration, planned_read)
    if args.include_plan:
        plan = io.StringIO()
        metadata_projection(planner_query(daft, path, io_config, args, window)).explain(show_all=True, file=plan)
        planner_filtered.details["query_plan"] = plan.getvalue()
    results.append(planner_filtered)

    expected_rows = baseline.rows
    if expected_rows == 0 and (window.topic_selection != "explicit" or window.time_selection != "explicit"):
        raise RuntimeError(
            "The automatically selected topic/time window returned zero rows; pass explicit --topic, --start-time, "
            "and --end-time bounds before comparing speedups"
        )
    if reader_filtered.rows != expected_rows or planner_filtered.rows != expected_rows:
        raise RuntimeError(
            "Filtered row counts disagree: "
            f"baseline={expected_rows}, reader={reader_filtered.rows}, planner={planner_filtered.rows}"
        )

    if abc_catalog is not None:

        def dataset_messages() -> StagePayload:
            from daft.datasets import abc

            table = metadata_projection(
                abc.messages(
                    abc_catalog,
                    topics=list(window.topics),
                    start_time=window.start_time,
                    end_time=window.end_time,
                    batch_size=args.batch_size,
                    io_config=io_config,
                )
            ).to_arrow()
            return StagePayload(
                rows=table.num_rows,
                source_file_bytes=source_bytes,
                arrow_bytes=table.nbytes,
                details={
                    "api": "daft.datasets.abc.messages",
                    "input_episode_rows": 1,
                    "reader_filters": {
                        "topics": list(window.topics),
                        "start_time": window.start_time,
                        "end_time": window.end_time,
                    },
                    **provenance_details(table),
                },
            )

        wrapper_filtered = timed_stage("abc_messages_topic_time_filter", iteration, dataset_messages)
        if wrapper_filtered.rows != expected_rows:
            raise RuntimeError(
                "ABC messages wrapper row count disagrees with the equivalent direct reader: "
                f"baseline={expected_rows}, wrapper={wrapper_filtered.rows}"
            )
        results.append(wrapper_filtered)

    def materialize_payload() -> StagePayload:
        filters: dict[str, Any] = {}
        if args.payload_scope == "filtered":
            filters = {
                "topics": list(window.topics),
                "start_time": window.start_time,
                "end_time": window.end_time,
            }
        dataframe = read_df(daft, path, io_config, args, **filters).select("data")
        if args.payload_limit:
            dataframe = dataframe.limit(args.payload_limit)
        table = dataframe.to_arrow()
        payload_bytes, representation = payload_sizes(table)
        return StagePayload(
            rows=table.num_rows,
            source_file_bytes=source_bytes,
            arrow_bytes=table.nbytes,
            payload_value_bytes=payload_bytes,
            details={
                "scope": args.payload_scope,
                "limit": args.payload_limit or None,
                "payload_representation": representation,
            },
        )

    results.append(timed_stage("read_mcap_payload_materialization", iteration, materialize_payload))

    if video_topic is not None:

        def decode_video_metadata() -> StagePayload:
            table = (
                read_df(
                    daft,
                    path,
                    io_config,
                    args,
                    topics=[video_topic],
                    start_time=window.start_time,
                    end_time=window.end_time,
                    decode_video=True,
                )
                .select("source_path", "topic", "log_time", "frame_id", "format", "frame_index")
                .limit(args.video_frame_limit)
                .to_arrow()
            )
            return StagePayload(
                rows=table.num_rows,
                source_file_bytes=source_bytes,
                arrow_bytes=table.nbytes,
                details={
                    "topic": video_topic,
                    "start_time": window.start_time,
                    "end_time": window.end_time,
                    "limit": args.video_frame_limit,
                    "image_materialized": False,
                    **provenance_details(table),
                },
            )

        results.append(timed_stage("read_mcap_video_decode_metadata", iteration, decode_video_metadata))

        def decode_video_frames() -> StagePayload:
            table = (
                read_df(
                    daft,
                    path,
                    io_config,
                    args,
                    topics=[video_topic],
                    start_time=window.start_time,
                    end_time=window.end_time,
                    decode_video=True,
                )
                .select("source_path", "topic", "log_time", "frame_id", "format", "frame_index", "data")
                .limit(args.video_frame_limit)
                .to_arrow()
            )
            return StagePayload(
                rows=table.num_rows,
                source_file_bytes=source_bytes,
                arrow_bytes=table.nbytes,
                details={
                    "topic": video_topic,
                    "start_time": window.start_time,
                    "end_time": window.end_time,
                    "limit": args.video_frame_limit,
                    "image_materialized": True,
                    **provenance_details(table),
                },
            )

        results.append(timed_stage("read_mcap_video_decode_frames", iteration, decode_video_frames))
    return results


def median_by_stage(results: list[StageResult]) -> dict[str, float]:
    grouped: dict[str, list[float]] = {}
    for result in results:
        grouped.setdefault(result.stage, []).append(result.elapsed_seconds)
    return {stage: statistics.median(times) for stage, times in grouped.items()}


def make_report(args: argparse.Namespace, cold_cache: Path | None) -> dict[str, Any]:
    # Import only after cold-cache environment variables have been installed.
    import daft

    daft.set_runner_native(args.threads)
    if args.mode == "local":
        path = local_episode_path(args.uri, args.local_mirror)
    else:
        path = args.uri
    io_config = build_io_config(daft, path, args)

    metadata: dict[str, Any]

    object_result_holder: dict[str, Any] = {}

    def scan_object() -> StagePayload:
        payload, source_size = object_scan(daft, path, io_config)
        object_result_holder["source_size"] = source_size
        return payload

    object_result = timed_stage("object_metadata_scan", 0, scan_object)
    source_bytes = int(object_result_holder["source_size"])

    metadata_holder: dict[str, Any] = {}

    def sniff_metadata() -> StagePayload:
        payload, value = metadata_sniff(daft, path, io_config)
        metadata_holder["metadata"] = value
        return payload

    metadata_result = timed_stage("mcap_summary_sniff", 0, sniff_metadata)
    metadata = metadata_holder["metadata"]
    window = choose_filter(metadata, args)
    video_topic, video_topic_selection = choose_video_topic(metadata, args)

    dataset_api_results: list[StageResult] = []
    catalog: Any | None = None
    dataset_context = abc_dataset_context(path)
    if dataset_context is not None:
        catalog_holder: dict[str, Any] = {}

        def discover_catalog() -> StagePayload:
            payload, bounded = abc_catalog_discovery(daft, path, io_config, source_bytes)
            catalog_holder["catalog"] = bounded
            return payload

        dataset_api_results.append(timed_stage("abc_catalog_discovery", 0, discover_catalog))
        catalog = catalog_holder["catalog"]
        dataset_api_results.append(
            timed_stage("abc_bounded_metadata", 0, lambda: abc_bounded_metadata(catalog, source_bytes))
        )

    if args.mode == "remote-warm":
        print("warming every read_mcap stage once (results discarded) ...", file=sys.stderr, flush=True)
        run_iteration(
            daft,
            path,
            io_config,
            args,
            window,
            source_bytes,
            iteration=-1,
            video_topic=video_topic,
            abc_catalog=catalog,
        )

    message_results: list[StageResult] = []
    for iteration in range(1, args.repeat + 1):
        message_results.extend(
            run_iteration(
                daft,
                path,
                io_config,
                args,
                window,
                source_bytes,
                iteration,
                video_topic=video_topic,
                abc_catalog=catalog,
            )
        )

    medians = median_by_stage(message_results)
    baseline = medians["read_mcap_unpushed_filter_baseline"]
    reader = medians["read_mcap_topic_time_filter"]
    planner = medians["read_mcap_planner_topic_time_filter"]
    speedups = {
        "reader_pushdown_vs_unpushed": None if reader == 0 else baseline / reader,
        "planner_pushdown_vs_unpushed": None if planner == 0 else baseline / planner,
    }
    comparisons = {}
    if "abc_messages_topic_time_filter" in medians:
        comparisons["abc_messages_wrapper_to_direct_reader_latency_ratio"] = (
            None if reader == 0 else medians["abc_messages_topic_time_filter"] / reader
        )

    return {
        "status": "ok",
        "benchmark": "daft.read_mcap ABC-130k pushdown",
        "created_unix_seconds": time.time(),
        "mode": args.mode,
        "uri": args.uri,
        "resolved_path": path,
        "dataset_identity": abc_identity(args.uri),
        "revision": hf_revision(args.uri),
        "runtime": {
            "git_revision": git_revision(),
            "daft_version": getattr(daft, "__version__", None),
            "python": sys.version,
        },
        "hf": {
            "use_xet": not args.no_xet,
            "token_present": bool(os.environ.get("HF_TOKEN")),
            "isolated_cold_cache": None if cold_cache is None else str(cold_cache),
            "cold_cache_scope": "process start; later stages may reuse ranges fetched by earlier stages",
        },
        "runner": {"type": "native", "threads": args.threads},
        "batch_size": args.batch_size,
        "repeat": args.repeat,
        "filter": asdict(window),
        "video_decode": {
            "topic": video_topic,
            "topic_selection": video_topic_selection,
            "frame_limit": args.video_frame_limit,
            "stages_enabled": video_topic is not None,
        },
        "dataset_api": {
            "status": "measured" if dataset_context is not None else "not_applicable",
            "context": dataset_context,
            "catalog_is_bounded_before_metadata_and_messages": dataset_context is not None,
        },
        "byte_semantics": {
            "source_file_bytes": "object size, not measured network transfer",
            "arrow_bytes": "materialized Arrow table size",
            "payload_value_bytes": "raw binary length, or UTF-8 allocation length for legacy string payloads",
        },
        "setup_stages": [asdict(object_result), asdict(metadata_result)],
        "dataset_api_stages": [asdict(result) for result in dataset_api_results],
        "message_stages": [asdict(result) for result in message_results],
        "median_seconds": medians,
        "speedups": speedups,
        "comparisons": comparisons,
    }


def main() -> int:
    args = parse_args()
    cold_cache = configure_cold_cache(args)
    try:
        report = make_report(args, cold_cache)
    except BenchmarkSkip as error:
        print(json.dumps({"status": "skipped", "reason": str(error)}, indent=2), file=sys.stderr)
        return 0
    except Exception as error:
        if is_gated_access_error(error):
            print(
                json.dumps(
                    {
                        "status": "skipped",
                        "reason": "Hugging Face denied ABC-130k access. Accept the dataset terms and refresh HF_TOKEN.",
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return 0
        raise
    finally:
        if cold_cache is not None and not args.keep_cold_cache:
            shutil.rmtree(cold_cache, ignore_errors=True)

    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
