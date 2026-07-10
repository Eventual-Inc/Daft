from __future__ import annotations

import pathlib
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.daft import PyMcapReader, io_glob
from daft.datatype import DataType, MediaType
from daft.dependencies import pafs
from daft.file.file import File
from daft.file.mcap import McapFile
from daft.filesystem import _resolve_paths_and_filesystem, get_protocol_from_path
from daft.io.mcap._pushdown import McapFilter, explicit_mcap_filter, extract_mcap_filter
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
        if "*" not in root_dir and root_dir.endswith(".mcap"):
            return [root_dir]
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


@PublicAPI
def read_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    batch_size: int = 1000,
    topic_start_time_resolver: TopicStartTimeResolver | None = None,
) -> DataFrame:
    """Read mcap file.

    Args:
        path: mcap file path
        start_time: Start time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
        end_time: End time to filter messages (same unit as MCAP message.log_time, typically nanoseconds).
        topics: List of topics to filter messages.
        batch_size: Number of messages to read in each batch.
        topic_start_time_resolver: Optional callable to compute per-file, per-topic start times.
            The callable is invoked once per MCAP file with the resolved file path and must return
            a mapping where:
            - key: topic name (str)
            - value: start time (int, same unit as MCAP message.log_time)

            will create one scan task per (file, topic) and set the task's start_time to:
            max(start_time, topic_start_time_resolver(file)[topic]).

    Returns:
        DataFrame: One row per message, including ``source_path`` provenance and
            a binary ``data`` payload.
    """
    return MCAPSource(
        file_path=path,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
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
        io_config: IOConfig | None = None,
        topic_start_time_resolver: TopicStartTimeResolver | None = None,
    ):
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        explicit_filter = explicit_mcap_filter(topics, start_time, end_time)
        self._start_time = explicit_filter.start_time
        self._end_time = explicit_filter.end_time
        self._topics = None if explicit_filter.topics is None else sorted(explicit_filter.topics)
        self._explicit_filter = explicit_filter
        self._batch_size = batch_size
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
        return f"MCAPSource({self._file_paths}, start_time={self._start_time}, end_time={self._end_time}, topics={self._topics})"

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def _infer_schema(self) -> Schema:
        return Schema.from_field_name_and_types(
            [
                ("source_path", DataType.string()),
                ("topic", DataType.string()),
                ("log_time", DataType.uint64()),
                ("publish_time", DataType.uint64()),
                ("sequence", DataType.uint32()),
                ("data", DataType.binary()),
            ]
        )

    def _task_schema(self, pushdowns: Pushdowns) -> Schema:
        if pushdowns.columns is None:
            return self._schema

        required = set(pushdowns.columns) | pushdowns.filter_required_column_names()
        fields = [(name, self._schema[name].dtype) for name in self._schema.column_names() if name in required]
        return Schema.from_field_name_and_types(fields)

    def _file_may_match(self, file_path: str, filters: McapFilter) -> bool:
        if filters.topics is None and filters.start_time is None and filters.end_time is None:
            return True

        metadata = McapFile(file_path, io_config=self._io_config)._read_metadata()
        if not metadata["has_summary"]:
            return True

        if filters.topics is not None and metadata["has_chunk_indexes"] and metadata["channels"]:
            file_topics = {channel["topic"] for channel in metadata["channels"]}
            if file_topics.isdisjoint(filters.topics):
                return False

        statistics = metadata["statistics"]
        if statistics is not None:
            if filters.start_time is not None and statistics["message_end_time"] < filters.start_time:
                return False
            if filters.end_time is not None and statistics["message_start_time"] >= filters.end_time:
                return False
        return True

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[MCAPSourceTask]:
        planner_filter = extract_mcap_filter(pushdowns.filters)
        filters = self._explicit_filter.intersect(planner_filter)
        if filters.empty:
            return

        task_schema = self._task_schema(pushdowns)
        columns = task_schema.column_names()
        effective_topics = None if filters.topics is None else sorted(filters.topics)
        native_limit = pushdowns.limit if pushdowns.filters is None or planner_filter.exact else None

        for file_path in self._file_paths:
            if len(self._file_paths) > 1 and not self._file_may_match(file_path, filters):
                continue

            topics = effective_topics
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                with suppress(Exception):
                    keyframes = self._topic_start_time_resolver(file_path)

            if not keyframes:
                yield MCAPSourceTask(
                    _file_path=file_path,
                    _schema=task_schema,
                    _columns=columns,
                    _batch_size=self._batch_size,
                    _start_time=filters.start_time,
                    _end_time=filters.end_time,
                    _topics=topics,
                    _limit=native_limit,
                    _io_config=self._io_config,
                )
                continue

            if topics is None:
                topics = list(keyframes.keys())

            for topic in topics:
                start_time = filters.start_time
                keyframe_time = keyframes.get(topic)
                if keyframe_time is not None:
                    start_time = keyframe_time if start_time is None else max(start_time, keyframe_time)

                if filters.end_time is not None and start_time is not None and start_time >= filters.end_time:
                    continue

                yield MCAPSourceTask(
                    _file_path=file_path,
                    _schema=task_schema,
                    _columns=columns,
                    _batch_size=self._batch_size,
                    _start_time=start_time,
                    _end_time=filters.end_time,
                    _topics=[topic],
                    _limit=native_limit,
                    _io_config=self._io_config,
                )


@dataclass
class MCAPSourceTask(DataSourceTask):
    _file_path: str
    _schema: Schema
    _columns: list[str]
    _batch_size: int = 1000
    _start_time: int | None = None
    _end_time: int | None = None
    _topics: list[str] | None = None
    _limit: int | None = None
    _io_config: IOConfig | None = None

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        file = File(self._file_path, self._io_config, MediaType.mcap())
        reader = PyMcapReader(
            file._inner,
            self._columns,
            batch_size=self._batch_size,
            start_time=self._start_time,
            end_time=self._end_time,
            topics=self._topics,
            limit=self._limit,
        )
        while (batch := reader.next_batch()) is not None:
            yield RecordBatch._from_pyrecordbatch(batch)
