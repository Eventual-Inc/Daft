from __future__ import annotations

import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import daft
from daft.api_annotations import PublicAPI
from daft.daft import io_glob
from daft.dependencies import pafs
from daft.filesystem import _resolve_paths_and_filesystem, get_protocol_from_path
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft import DataFrame
    from daft.io import IOConfig
    from daft.io.pushdowns import Pushdowns


TopicToStartTime = dict[str, int]
TopicStartTimeResolver = Callable[[str], TopicToStartTime]


# mcap format details see: https://github.com/foxglove/mcap
# reader: https://github.com/foxglove/mcap/blob/17d9324367ab7486ce4a3cd300e40a0b09cfb799/python/mcap-ros2-support/mcap_ros2/reader.py


def normalize_storage_path(path: str, io_config: IOConfig | None = None) -> str:
    """Normalize storage path: infer and add protocol prefix based on IO configuration.

    1. Keep existing protocol paths unchanged
    2. Add protocol prefix for protocol-less paths based on io_config
    3. Preserve local paths as-is
    """
    protocol = get_protocol_from_path(path)
    if protocol != "file":
        return path

    if io_config:
        if io_config.s3:
            return f"s3://{path.lstrip('/')}"
        elif io_config.azure:
            return f"abfs://{path.lstrip('/')}"
        elif io_config.gcs:
            return f"gs://{path.lstrip('/')}"

    return path


def list_files(
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    resolved_path: str | None = None,
    fs: pafs.FileSystem | None = None,
) -> list[str]:
    if isinstance(root_dir, pathlib.Path):
        root_dir = str(root_dir)

    # Special case for handling HuggingFace paths
    # TODO: Remove once we remove fsspec-based filesystem resolution
    if get_protocol_from_path(root_dir) == "hf":
        glob_path = root_dir if "*" in root_dir else root_dir.rstrip("/")
        if not glob_path.endswith(".mcap"):
            glob_path = f"{glob_path}/**/*.mcap" if "**" not in glob_path else glob_path
        files = io_glob(glob_path, io_config=io_config)
        return [f["path"] for f in files if f["type"] == "File"]

    if resolved_path is None or fs is None:
        [resolved_path], fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)

    try:
        file_info = fs.get_file_info(resolved_path)
        if file_info.type == pafs.FileType.File:
            return [resolved_path]
    except FileNotFoundError:
        return []

    selector = pafs.FileSelector(resolved_path, recursive=True)

    try:
        file_infos = fs.get_file_info(selector)
    except NotADirectoryError:
        return [resolved_path]
    except FileNotFoundError:
        return []

    return [file_info.path for file_info in file_infos if file_info.type == pafs.FileType.File]


@dataclass
class _MCAPTopicStats:
    file_path: str
    topic: str
    schema_name: str | None
    schema_encoding: str | None
    message_encoding: str | None
    message_count: int = 0
    first_log_time: int | None = None
    last_log_time: int | None = None
    first_publish_time: int | None = None
    last_publish_time: int | None = None
    min_message_size: int | None = None
    max_message_size: int | None = None
    total_message_size: int = 0

    def add(self, log_time: int, publish_time: int, message_size: int) -> None:
        self.message_count += 1
        self.first_log_time = log_time if self.first_log_time is None else min(self.first_log_time, log_time)
        self.last_log_time = log_time if self.last_log_time is None else max(self.last_log_time, log_time)
        self.first_publish_time = (
            publish_time if self.first_publish_time is None else min(self.first_publish_time, publish_time)
        )
        self.last_publish_time = (
            publish_time if self.last_publish_time is None else max(self.last_publish_time, publish_time)
        )
        self.min_message_size = (
            message_size if self.min_message_size is None else min(self.min_message_size, message_size)
        )
        self.max_message_size = (
            message_size if self.max_message_size is None else max(self.max_message_size, message_size)
        )
        self.total_message_size += message_size

    def as_row(self) -> dict[str, object]:
        avg_message_size = self.total_message_size / self.message_count if self.message_count else None
        return {
            "file_path": self.file_path,
            "topic": self.topic,
            "schema_name": self.schema_name,
            "schema_encoding": self.schema_encoding,
            "message_encoding": self.message_encoding,
            "message_count": self.message_count,
            "first_log_time": self.first_log_time,
            "last_log_time": self.last_log_time,
            "first_publish_time": self.first_publish_time,
            "last_publish_time": self.last_publish_time,
            "min_message_size": self.min_message_size,
            "max_message_size": self.max_message_size,
            "total_message_size": self.total_message_size,
            "avg_message_size": avg_message_size,
        }


def _empty_mcap_inspection() -> DataFrame:
    import pyarrow as pa

    from daft.convert import from_arrow

    schema = pa.schema(
        [
            pa.field("file_path", pa.string()),
            pa.field("topic", pa.string()),
            pa.field("schema_name", pa.string()),
            pa.field("schema_encoding", pa.string()),
            pa.field("message_encoding", pa.string()),
            pa.field("message_count", pa.int64()),
            pa.field("first_log_time", pa.int64()),
            pa.field("last_log_time", pa.int64()),
            pa.field("first_publish_time", pa.int64()),
            pa.field("last_publish_time", pa.int64()),
            pa.field("min_message_size", pa.int64()),
            pa.field("max_message_size", pa.int64()),
            pa.field("total_message_size", pa.int64()),
            pa.field("avg_message_size", pa.float64()),
        ]
    )
    return from_arrow(pa.Table.from_pylist([], schema=schema))


def _inspect_mcap_rows(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
) -> list[dict[str, object]]:
    import importlib

    make_reader = importlib.import_module("mcap.reader").make_reader
    file_paths = [normalize_storage_path(file_path, io_config) for file_path in list_files(path, io_config)]
    if not file_paths:
        raise FileNotFoundError(f"Path not found: {path}")

    stats: dict[tuple[str, str, str | None, str | None, str | None], _MCAPTopicStats] = {}
    for file_path in file_paths:
        with daft.open_file(file_path, "rb", io_config=io_config) as f:
            reader = make_reader(f, decoder_factories=[])
            for schema, channel, message in reader.iter_messages(
                topics=topics,
                start_time=start_time,
                end_time=end_time,
                log_time_order=False,
            ):
                schema_name = getattr(schema, "name", None)
                schema_encoding = getattr(schema, "encoding", None)
                message_encoding = channel.message_encoding
                key = (file_path, channel.topic, schema_name, schema_encoding, message_encoding)
                topic_stats = stats.get(key)
                if topic_stats is None:
                    topic_stats = _MCAPTopicStats(
                        file_path=file_path,
                        topic=channel.topic,
                        schema_name=schema_name,
                        schema_encoding=schema_encoding,
                        message_encoding=message_encoding,
                    )
                    stats[key] = topic_stats
                topic_stats.add(
                    log_time=message.log_time,
                    publish_time=message.publish_time,
                    message_size=len(message.data),
                )

    rows = [topic_stats.as_row() for topic_stats in stats.values()]
    rows.sort(key=lambda row: (str(row["file_path"]), str(row["topic"])))
    return rows


@PublicAPI
def inspect_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
) -> DataFrame:
    """Inspect MCAP topic/channel statistics without decoding message payloads.

    Args:
        path: MCAP file or directory path.
        io_config: IO configuration to use for remote storage.
        start_time: Start time to filter messages (same unit as MCAP message.log_time).
        end_time: End time to filter messages (same unit as MCAP message.log_time).
        topics: List of topics to inspect.

    Returns:
        DataFrame: One row per file/topic/schema/message encoding with message counts,
            log-time bounds, publish-time bounds, and payload-size statistics.
    """
    from daft.convert import from_pylist

    rows = _inspect_mcap_rows(
        path,
        io_config=io_config,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
    )
    if not rows:
        return _empty_mcap_inspection()

    return from_pylist(rows)


def _empty_mcap_read_plan() -> DataFrame:
    import pyarrow as pa

    from daft.convert import from_arrow

    schema = pa.schema(
        [
            pa.field("file_path", pa.string()),
            pa.field("topic", pa.string()),
            pa.field("schema_name", pa.string()),
            pa.field("schema_encoding", pa.string()),
            pa.field("message_encoding", pa.string()),
            pa.field("window_index", pa.int64()),
            pa.field("window_count", pa.int64()),
            pa.field("start_time", pa.int64()),
            pa.field("end_time", pa.int64()),
            pa.field("estimated_message_count", pa.int64()),
            pa.field("estimated_payload_bytes", pa.int64()),
        ]
    )
    return from_arrow(pa.Table.from_pylist([], schema=schema))


def _required_int(row: dict[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, int):
        return value
    raise ValueError(f"MCAP manifest field {key!r} must be an integer, got {value!r}")


@PublicAPI
def plan_mcap_reads(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    max_messages_per_task: int = 100_000,
) -> DataFrame:
    """Plan topic/time read windows for large MCAP logs.

    The planner first inspects matching MCAP messages, then splits each file/topic
    span into approximate time windows. Each output row can be used as a scoped
    `read_mcap` call by passing `file_path`, `[topic]`, `start_time`, and `end_time`.

    Args:
        path: MCAP file or directory path.
        io_config: IO configuration to use for remote storage.
        start_time: Optional lower log-time bound.
        end_time: Optional upper log-time bound.
        topics: Optional topics to plan.
        max_messages_per_task: Target maximum messages per planned read window.

    Returns:
        DataFrame: One row per planned file/topic/time window.
    """
    import math

    from daft.convert import from_pylist

    if max_messages_per_task <= 0:
        raise ValueError("max_messages_per_task must be greater than 0")

    manifest_rows = _inspect_mcap_rows(
        path,
        io_config=io_config,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
    )
    if not manifest_rows:
        return _empty_mcap_read_plan()

    plan_rows: list[dict[str, object]] = []
    for manifest in manifest_rows:
        message_count = _required_int(manifest, "message_count")
        first_log_time = manifest["first_log_time"]
        last_log_time = manifest["last_log_time"]
        if message_count <= 0 or first_log_time is None or last_log_time is None:
            continue

        window_count = max(1, math.ceil(message_count / max_messages_per_task))
        start_bound = _required_int(manifest, "first_log_time")
        end_bound = _required_int(manifest, "last_log_time") + 1
        window_width = max(1, math.ceil((end_bound - start_bound) / window_count))
        base_count, remainder = divmod(message_count, window_count)
        total_payload_size = _required_int(manifest, "total_message_size")

        for window_index in range(window_count):
            window_start = start_bound + window_index * window_width
            window_end = min(end_bound, window_start + window_width)
            estimated_message_count = base_count + (1 if window_index < remainder else 0)
            estimated_payload_bytes = round(total_payload_size * (estimated_message_count / message_count))
            plan_rows.append(
                {
                    "file_path": manifest["file_path"],
                    "topic": manifest["topic"],
                    "schema_name": manifest["schema_name"],
                    "schema_encoding": manifest["schema_encoding"],
                    "message_encoding": manifest["message_encoding"],
                    "window_index": window_index,
                    "window_count": window_count,
                    "start_time": window_start,
                    "end_time": window_end,
                    "estimated_message_count": estimated_message_count,
                    "estimated_payload_bytes": estimated_payload_bytes,
                }
            )

    if not plan_rows:
        return _empty_mcap_read_plan()

    return from_pylist(plan_rows)


@PublicAPI
def read_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    batch_size: int = 1000,
    include_record_metadata: bool = False,
    topic_start_time_resolver: TopicStartTimeResolver | None = None,
) -> DataFrame:
    """Read mcap file.

    Args:
        path: mcap file path
        start_time: Start time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
        end_time: End time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
        topics: List of topics to filter messages.
        batch_size: Number of messages to read in each batch.
        include_record_metadata: Include MCAP schema/channel metadata columns for telemetry inspection:
            schema_name, schema_encoding, message_encoding, and message_size.
        topic_start_time_resolver: Optional callable to compute per-file, per-topic start times.
            The callable is invoked once per MCAP file with the resolved file path and must return
            a mapping where:
            - key: topic name (str)
            - value: start time (int, same unit as MCAP message.log_time)

            will create one scan task per (file, topic) and set the task's start_time to:
            max(start_time, topic_start_time_resolver(file)[topic]).

    Returns:
        DataFrame: DataFrame with the schema converted from the specified MCAP file.
    """
    return MCAPSource(
        file_path=path,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
        include_record_metadata=include_record_metadata,
        io_config=io_config,
        topic_start_time_resolver=topic_start_time_resolver,
    ).read()


class MCAPSource(DataSource):
    def __init__(
        self,
        file_path: str,
        start_time: int | None = None,
        end_time: int | None = None,
        topics: list[str] | None = None,
        batch_size: int = 1000,
        include_record_metadata: bool = False,
        io_config: IOConfig | None = None,
        topic_start_time_resolver: TopicStartTimeResolver | None = None,
    ):
        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
        self._batch_size = batch_size
        self._include_record_metadata = include_record_metadata
        self._topic_start_time_resolver = topic_start_time_resolver
        self._file_paths = [
            normalize_storage_path(file_path, io_config) for file_path in list_files(file_path, io_config)
        ]

        if not self._file_paths:
            raise FileNotFoundError(f"Path not found: {file_path}")

        self._schema = self._infer_schema()
        self._io_config = io_config

    @property
    def name(self) -> str:
        return "MCAPSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return (
            f"MCAPSource({self._file_paths}, start_time={self._start_time}, "
            f"end_time={self._end_time}, topics={self._topics}, "
            f"include_record_metadata={self._include_record_metadata})"
        )

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def _infer_schema(self) -> Schema:
        import pyarrow as pa

        fields = [
            pa.field("topic", pa.string()),
            pa.field("log_time", pa.int64()),
            pa.field("publish_time", pa.int64()),
            pa.field("sequence", pa.int32()),
            pa.field("data", pa.string()),
        ]
        if self._include_record_metadata:
            fields.extend(
                [
                    pa.field("schema_name", pa.string()),
                    pa.field("schema_encoding", pa.string()),
                    pa.field("message_encoding", pa.string()),
                    pa.field("message_size", pa.int64()),
                ]
            )
        schema = pa.schema(fields)
        return Schema.from_pyarrow_schema(schema)

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[MCAPSourceTask]:
        for file_path in self._file_paths:
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                keyframes = self._topic_start_time_resolver(file_path)

            if not keyframes:
                yield MCAPSourceTask(
                    _file_path=file_path,
                    _schema=self._schema,
                    _batch_size=self._batch_size,
                    _start_time=self._start_time,
                    _end_time=self._end_time,
                    _topics=self._topics,
                    _include_record_metadata=self._include_record_metadata,
                    _io_config=self._io_config,
                )
                continue

            if self._topics is None:
                topics = list(keyframes.keys())
            else:
                topics = self._topics

            for topic in topics:
                start_time = self._start_time
                keyframe_time = keyframes.get(topic)
                if keyframe_time is not None:
                    start_time = keyframe_time if start_time is None else max(start_time, keyframe_time)

                if self._end_time is not None and start_time is not None and start_time >= self._end_time:
                    continue

                yield MCAPSourceTask(
                    _file_path=file_path,
                    _schema=self._schema,
                    _batch_size=self._batch_size,
                    _start_time=start_time,
                    _end_time=self._end_time,
                    _topics=[topic],
                    _include_record_metadata=self._include_record_metadata,
                    _io_config=self._io_config,
                )


@dataclass
class MCAPSourceTask(DataSourceTask):
    _file_path: str
    _schema: Schema
    _batch_size: int = 1000
    _start_time: int | None = None
    _end_time: int | None = None
    _topics: list[str] | None = None
    _include_record_metadata: bool = False
    _io_config: IOConfig | None = None

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        import importlib

        make_reader = importlib.import_module("mcap.reader").make_reader

        with daft.open_file(self._file_path, "rb", io_config=self._io_config) as f:
            reader = make_reader(f, decoder_factories=[])

            buffer: list[dict[str, object]] = []
            for schema, channel, message in reader.iter_messages(
                topics=self._topics,
                start_time=self._start_time,
                end_time=self._end_time,
                log_time_order=True,
            ):
                row: dict[str, object] = {
                    "topic": channel.topic,
                    "log_time": message.log_time,
                    "publish_time": message.publish_time,
                    "sequence": message.sequence,
                    "data": str(message.data),
                }
                if self._include_record_metadata:
                    row.update(
                        {
                            "schema_name": getattr(schema, "name", None),
                            "schema_encoding": getattr(schema, "encoding", None),
                            "message_encoding": channel.message_encoding,
                            "message_size": len(message.data),
                        }
                    )
                buffer.append(row)

                if len(buffer) >= self._batch_size:
                    yield self._create_recordbatch(buffer)
                    buffer.clear()

            if buffer:
                yield self._create_recordbatch(buffer)

    def _create_recordbatch(self, data: list[dict[str, object]]) -> RecordBatch:
        import pyarrow as pa

        arrow_batch = pa.RecordBatch.from_pylist(data, schema=self._schema.to_pyarrow_schema())
        return RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema=self._schema.to_pyarrow_schema())
