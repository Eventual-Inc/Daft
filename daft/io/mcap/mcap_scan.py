# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional

from daft.daft import PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from daft.io import IOConfig


# mcap format details see: https://github.com/foxglove/mcap
# reader: https://github.com/foxglove/mcap/blob/17d9324367ab7486ce4a3cd300e40a0b09cfb799/python/mcap-ros2-support/mcap_ros2/reader.py


def _mcap_factory_function(
    file_path: str,
    required_columns: Optional[list[str]] = None,
    limit: Optional[int] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    topics: Optional[list[str]] = None,
    batch_size: int = 1000,
    io_config: Optional["IOConfig"] = None,
) -> Iterator[PyRecordBatch]:
    import pyarrow as pa
    from mcap.reader import make_reader
    from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
    from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

    from daft.filesystem import _infer_filesystem

    from .json_reader import JsonDecoderFactory

    file_path, fs, _ = _infer_filesystem(file_path, io_config)

    with fs.open_input_file(file_path) as file_obj:
        reader = make_reader(
            file_obj, decoder_factories=[Ros2DecoderFactory(), ProtobufDecoderFactory(), JsonDecoderFactory()]
        )
        buffer: list[dict[str, Any]] = []

        # TODO ros_msg is not decoded here, we should decode it to a more structured format
        for schema, channel, message, ros_msg in reader.iter_decoded_messages(
            topics=topics, start_time=start_time, end_time=end_time, log_time_order=True
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

            if len(buffer) >= batch_size:
                arrow_batch = pa.RecordBatch.from_pylist(buffer)
                yield RecordBatch.from_arrow_record_batches([arrow_batch], arrow_batch.schema)._recordbatch
                buffer.clear()

        if buffer:
            arrow_batch = pa.RecordBatch.from_pylist(buffer)
            yield RecordBatch.from_arrow_record_batches([arrow_batch], arrow_batch.schema)._recordbatch


class MCAPScanOperator(ScanOperator):
    def __init__(
        self,
        file_path: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        topics: Optional[list[str]] = None,
        batch_size: int = 1000,
        io_config: Optional["IOConfig"] = None,
    ):
        from daft.filesystem import list_files, normalize_storage_path

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

    def name(self) -> str:
        return "MCAPScanOperator"

    def display_name(self) -> str:
        return f"MCAPScanOperator({self._file_paths}, start_time={self._start_time}, end_time={self._end_time}, topics={self._topics})"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
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

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        required_columns = pushdowns.columns if pushdowns.columns else None

        for file_path in self._file_paths:
            yield ScanTask.python_factory_func_scan_task(
                module=_mcap_factory_function.__module__,
                func_name=_mcap_factory_function.__name__,
                func_args=(
                    file_path,
                    required_columns,
                    pushdowns.limit,
                    self._start_time,
                    self._end_time,
                    self._topics,
                    self._batch_size,
                    self._io_config,
                ),
                schema=self.schema()._schema,
                num_rows=None,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
            )
