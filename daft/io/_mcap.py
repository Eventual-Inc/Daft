from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import TYPE_CHECKING

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, McapSourceConfig, StorageConfig, io_glob
from daft.datatype import DataType
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft import DataFrame
    from daft.io.pushdowns import Pushdowns


TopicToStartTime = dict[str, int]
TopicStartTimeResolver = Callable[[str], TopicToStartTime]


class MCAPSource(DataSource):
    def __init__(
        self,
        file_path: str | pathlib.Path,
        start_time: int | None = None,
        end_time: int | None = None,
        topics: list[str] | None = None,
        batch_size: int = 1000,
        io_config: IOConfig | None = None,
        topic_start_time_resolver: TopicStartTimeResolver | None = None,
        storage_config: StorageConfig | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")

        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
        self._batch_size = batch_size
        self._topic_start_time_resolver = topic_start_time_resolver
        self._file_path = str(file_path)
        self._io_config = io_config
        self._storage_config = storage_config or StorageConfig(True, io_config)
        self._schema = self._infer_schema()

    @property
    def name(self) -> str:
        return "MCAPSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"MCAPSource({self._file_path}, start_time={self._start_time}, end_time={self._end_time}, topics={self._topics})"

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    @staticmethod
    def _infer_schema() -> Schema:
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

    def _task(
        self,
        file_path: str,
        file_size: int | None,
        pushdowns: Pushdowns,
        *,
        start_time: int | None,
        topics: list[str] | None,
    ) -> DataSourceTask:
        return DataSourceTask.mcap(
            path=file_path,
            schema=self._schema,
            mcap_config=McapSourceConfig(
                batch_size=self._batch_size,
                start_time=start_time,
                end_time=self._end_time,
                topics=topics,
            ),
            pushdowns=pushdowns,
            size_bytes=file_size,
            storage_config=self._storage_config,
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        file_infos = [
            file_info
            for file_info in io_glob(self._file_path, io_config=self._io_config)
            if file_info["type"] == "File"
        ]
        if not file_infos:
            raise FileNotFoundError(f"No files found at {self._file_path}")

        for file_info in file_infos:
            file_path = file_info["path"]
            file_size = file_info["size"]
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                try:
                    keyframes = self._topic_start_time_resolver(file_path)
                except Exception:
                    keyframes = None

            if not keyframes:
                yield self._task(
                    file_path,
                    file_size,
                    pushdowns,
                    start_time=self._start_time,
                    topics=self._topics,
                )
                continue

            topics = list(keyframes) if self._topics is None else self._topics
            for topic in topics:
                start_time = self._start_time
                if (keyframe_time := keyframes.get(topic)) is not None:
                    start_time = keyframe_time if start_time is None else max(start_time, keyframe_time)

                if self._end_time is not None and start_time is not None and start_time >= self._end_time:
                    continue

                yield self._task(
                    file_path,
                    file_size,
                    pushdowns,
                    start_time=start_time,
                    topics=[topic],
                )


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
    """Read raw messages from one or more MCAP files.

    Args:
        path: MCAP file or directory path.
        io_config: Configuration for storage credentials and native I/O.
        start_time: Inclusive lower bound for ``message.log_time``.
        end_time: Exclusive upper bound for ``message.log_time``.
        topics: Topic names to include.
        batch_size: Number of messages decoded per native record batch.
        topic_start_time_resolver: Optional per-file callback returning topic
            start times. Each result fans out into one native task per topic,
            using ``max(start_time, resolved_start_time)``.

    Returns:
        A DataFrame with ``source_path``, ``topic``, ``log_time``,
        ``publish_time``, ``sequence``, and raw binary ``data`` columns.
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    return MCAPSource(
        file_path=path,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
        io_config=io_config,
        topic_start_time_resolver=topic_start_time_resolver,
        storage_config=storage_config,
    ).read()
