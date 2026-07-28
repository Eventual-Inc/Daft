from __future__ import annotations

import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.daft import PyMcapReader, io_glob
from daft.datatype import DataType
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
        DataFrame: DataFrame with the schema converted from the specified MCAP file.
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
        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
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

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[MCAPSourceTask]:
        for file_path in self._file_paths:
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                try:
                    keyframes = self._topic_start_time_resolver(file_path)
                except Exception:  # noqa: BLE001
                    keyframes = None

            if not keyframes:
                yield MCAPSourceTask(
                    _file_path=file_path,
                    _schema=self._schema,
                    _batch_size=self._batch_size,
                    _start_time=self._start_time,
                    _end_time=self._end_time,
                    _topics=self._topics,
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
    _io_config: IOConfig | None = None

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        reader = PyMcapReader(
            self._file_path,
            io_config=self._io_config,
            batch_size=self._batch_size,
            start_time=self._start_time,
            end_time=self._end_time,
            topics=self._topics,
        )
        while (batch := reader.next_batch()) is not None:
            yield RecordBatch._from_pyrecordbatch(batch)
