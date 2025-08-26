from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.dependencies import pafs
from daft.filesystem import _resolve_paths_and_filesystem, get_protocol_from_path
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Iterator

    from daft import DataFrame
    from daft.io import IOConfig
    from daft.io.pushdowns import Pushdowns


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
) -> DataFrame:
    """Read mcap file.

    Args:
        path: mcap file path
        start_time: Start time in milliseconds to filter messages.
        end_time: End time in milliseconds to filter messages.
        topics: List of topics to filter messages.
        batch_size: Number of messages to read in each batch.

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
    ):
        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
        self._batch_size = batch_size
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
            yield MCAPSourceTask(
                _file_path=file_path,
                _schema=self._schema,
                _batch_size=self._batch_size,
                _start_time=self._start_time,
                _end_time=self._end_time,
                _topics=self._topics,
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
        from mcap.reader import make_reader
        from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
        from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

        from daft.filesystem import _infer_filesystem

        from .mcap_json_decoder import JsonDecoderFactory

        resolved_path, fs, _ = _infer_filesystem(self._file_path, self._io_config)

        with fs.open_input_file(resolved_path) as file_obj:
            reader = make_reader(
                file_obj, decoder_factories=[Ros2DecoderFactory(), ProtobufDecoderFactory(), JsonDecoderFactory()]
            )

            buffer = []
            for _, channel, message, ros_msg in reader.iter_decoded_messages(
                topics=self._topics, start_time=self._start_time, end_time=self._end_time, log_time_order=True
            ):
                buffer.append(
                    {
                        "topic": channel.topic,
                        "log_time": message.log_time,
                        "publish_time": message.publish_time,
                        "sequence": message.sequence,
                        "data": str(ros_msg),
                    }
                )

                if len(buffer) >= self._batch_size:
                    yield self._create_micropartition(buffer)
                    buffer.clear()

            if buffer:
                yield self._create_micropartition(buffer)

    def _create_micropartition(self, data: list[dict[str, object]]) -> MicroPartition:
        import pyarrow as pa

        arrow_batch = pa.RecordBatch.from_pylist(data, schema=self._schema.to_pyarrow_schema())
        return MicroPartition.from_arrow_record_batches([arrow_batch], arrow_schema=self._schema.to_pyarrow_schema())

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        yield from self.execute()

    @property
    def schema(self) -> Schema:
        return self._schema
