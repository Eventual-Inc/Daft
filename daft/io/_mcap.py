from __future__ import annotations

import logging
import pathlib
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, McapSourceConfig, StorageConfig, io_glob
from daft.datatype import DataType
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from daft import DataFrame
    from daft.io.pushdowns import Pushdowns


logger = logging.getLogger(__name__)


def _topic_start_time_resolver_scan_specs(
    file_path: str,
    start_time: int | None,
    end_time: int | None,
    topics: list[str] | None,
    resolver: Callable[[str], dict[str, int]],
) -> Iterator[tuple[int | None, list[str] | None]]:
    """Yield scan specs for the legacy ``topic_start_time_resolver`` API.

    The callback was introduced for external video keyframe discovery in
    https://github.com/Eventual-Inc/Daft/pull/5886. Because each topic can
    resolve to a different start time, its result is expanded into one
    ``(start_time, topics)`` scan spec per topic.
    """
    # TODO: Remove this helper and the public callback after replacing the
    # keyframe workflow with dedicated custom video decoding support.
    try:
        topic_start_times = resolver(file_path)
    except Exception:
        logger.warning(
            "Failed to resolve MCAP topic start times for %s; using source-wide constraints",
            file_path,
            exc_info=True,
        )
        topic_start_times = None

    if not topic_start_times:
        yield start_time, topics
        return

    resolved_topics = list(topic_start_times) if topics is None else topics
    for topic in resolved_topics:
        topic_start_time = start_time
        if (resolved_start_time := topic_start_times.get(topic)) is not None:
            topic_start_time = (
                resolved_start_time if topic_start_time is None else max(topic_start_time, resolved_start_time)
            )

        if end_time is not None and topic_start_time is not None and topic_start_time >= end_time:
            continue

        yield topic_start_time, [topic]


class MCAPSource(DataSource):
    def __init__(
        self,
        file_path: str | pathlib.Path,
        start_time: int | None = None,
        end_time: int | None = None,
        topics: list[str] | None = None,
        batch_size: int = 1000,
        io_config: IOConfig | None = None,
        topic_start_time_resolver: Callable[[str], dict[str, int]] | None = None,
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

    def _make_task(
        self,
        file_path: str,
        file_size: int | None,
        pushdowns: Pushdowns,
        *,
        start_time: int | None,
        topics: list[str] | None,
    ) -> DataSourceTask:
        """Create a native MCAP scan task."""
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
            if self._topic_start_time_resolver is None:
                yield self._make_task(
                    file_info["path"],
                    file_info["size"],
                    pushdowns,
                    start_time=self._start_time,
                    topics=self._topics,
                )
                continue

            # Deprecated compatibility path; see the helper for provenance.
            for start_time, topics in _topic_start_time_resolver_scan_specs(
                file_info["path"],
                self._start_time,
                self._end_time,
                self._topics,
                self._topic_start_time_resolver,
            ):
                yield self._make_task(
                    file_info["path"],
                    file_info["size"],
                    pushdowns,
                    start_time=start_time,
                    topics=topics,
                )


@PublicAPI
def read_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    batch_size: int = 1000,
    topic_start_time_resolver: Callable[[str], dict[str, int]] | None = None,
    _multithreaded_io: bool | None = None,
) -> DataFrame:
    """Read raw messages from one or more MCAP files.

    Args:
        path: MCAP file or directory path.
        io_config: Configuration for storage credentials and native I/O.
        start_time: Inclusive non-negative lower bound for ``message.log_time``.
        end_time: Exclusive non-negative upper bound for ``message.log_time``.
        topics: Topic names to include.
        batch_size: Number of messages decoded per native record batch.
        topic_start_time_resolver: DEPRECATED. Optional per-file callback
            returning non-negative topic start times. Each result fans out into
            one native task per topic, using ``max(start_time, resolved_start_time)``.

    Warning:
        MCAP timestamps use the format's native unsigned 64-bit representation. ``start_time``,
        ``end_time``, and times returned by ``topic_start_time_resolver`` must be between 0 and
        ``2**64 - 1``. Negative time values accepted by earlier Daft versions now raise
        ``OverflowError`` because the output schema uses ``uint64`` timestamps instead of ``int64``.

    Returns:
        A DataFrame with ``source_path``, ``topic``, ``log_time``,
        ``publish_time``, ``sequence``, and raw binary ``data`` columns.
    """
    if topic_start_time_resolver is not None:
        warnings.warn(
            "`topic_start_time_resolver` is deprecated and will be removed in a future release. "
            "Use explicit `topics` and `start_time` values for MCAP scans. "
            "Dedicated support for custom video decoding will be added separately.",
            DeprecationWarning,
            stacklevel=2,
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    multithreaded_io = (
        (runners.get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )
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
