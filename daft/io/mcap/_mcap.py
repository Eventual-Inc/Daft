from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Literal

from daft.api_annotations import PublicAPI
from daft.dependencies import pafs
from daft.filesystem import _resolve_paths_and_filesystem, get_protocol_from_path
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Iterator
    from types import TracebackType

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
        start_time: Start time in milliseconds to filter messages.
        end_time: End time in milliseconds to filter messages.
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
        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
        self._batch_size = batch_size
        self._topic_start_time_resolver = topic_start_time_resolver
        self._file_paths = [
            normalize_storage_path(file_path, io_config) for file_path in list_files(file_path, io_config)
        ]

        if self._file_paths is None or len(self._file_paths) == 0:
            raise FileNotFoundError(f"Path not found: {file_path}")

        self._schema = self._infer_schema(self._file_paths[0])
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

    def _infer_schema(self, sample_path: str) -> Schema:
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("topic", pa.string()),
                pa.field("log_time", pa.int64()),
                pa.field("publish_time", pa.int64()),
                pa.field("sequence", pa.int32()),
                pa.field("data", pa.string()),
            ]
        )
        return Schema.from_pyarrow_schema(schema)

    def get_tasks(self, pushdowns: Pushdowns | None = None) -> Iterator[MCAPSourceTask]:
        for file_path in self._file_paths:
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                try:
                    keyframes = self._topic_start_time_resolver(file_path)
                except Exception:
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

    def execute(self) -> Iterator[MicroPartition]:
        import importlib

        make_reader = importlib.import_module("mcap.reader").make_reader

        from daft.filesystem import _infer_filesystem

        class _NonSeekableStreamWrapper:
            def __init__(self, file_obj: BytesIO):
                self._file_obj = file_obj

            def read(self, size: int = -1) -> bytes:
                return self._file_obj.read(size)

            def readable(self) -> bool:
                return True

            def seekable(self) -> bool:
                return False

            def tell(self) -> int:
                return self._file_obj.tell()

            def close(self) -> None:
                self._file_obj.close()

            def __enter__(self) -> _NonSeekableStreamWrapper:
                return self

            def __exit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> Literal[False]:
                self.close()
                return False

        resolved_path, fs, _ = _infer_filesystem(self._file_path, self._io_config)

        with fs.open_input_file(resolved_path) as file_obj:
            file_content = file_obj.read()

        in_memory_file = BytesIO(file_content)
        reader = make_reader(_NonSeekableStreamWrapper(in_memory_file), decoder_factories=[])

        buffer = []
        try:
            for _, channel, message in reader.iter_messages(
                topics=self._topics,
                start_time=self._start_time,
                end_time=self._end_time,
                log_time_order=True,
            ):
                buffer.append(
                    {
                        "topic": channel.topic,
                        "log_time": message.log_time,
                        "publish_time": message.publish_time,
                        "sequence": message.sequence,
                        "data": str(message.data),
                    }
                )

                if len(buffer) >= self._batch_size:
                    yield self._create_micropartition(buffer)
                    buffer.clear()

            if buffer:
                yield self._create_micropartition(buffer)
        finally:
            pass

    def _create_micropartition(self, data: list[dict[str, object]]) -> MicroPartition:
        import pyarrow as pa

        arrow_batch = pa.RecordBatch.from_pylist(data, schema=self._schema.to_pyarrow_schema())
        return MicroPartition.from_arrow_record_batches([arrow_batch], arrow_schema=self._schema.to_pyarrow_schema())

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        yield from self.execute()

    @property
    def schema(self) -> Schema:
        return self._schema
