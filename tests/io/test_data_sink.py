from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest

import daft
from daft.io import AsyncDataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

from ..integration.io.conftest import minio_create_bucket


class MyWriteSink(AsyncDataSink[MicroPartition]):
    """Simple AsyncDataSink that writes micropartitions to parquet files."""

    def __init__(self, path: str, write_mode: str = "overwrite", io_config=None):
        self.path = path
        self.write_mode = write_mode
        self.io_config = io_config
        self.file_counter = 0

    def schema(self) -> Schema:
        return Schema.from_pydict({"path": daft.DataType.string()})

    async def write(self, micropartitions: Iterator[MicroPartition]) -> AsyncIterator[WriteResult[MicroPartition]]:
        for mp in micropartitions:
            result = await mp.write_parquet(self.path, write_mode=self.write_mode, io_config=self.io_config)

            yield WriteResult(result=result, bytes_written=0, rows_written=0)

    def finalize(self, results: list[WriteResult[MicroPartition]]) -> MicroPartition:
        tbl = MicroPartition.concat([res.result for res in results])
        return tbl


@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
def test_write_sink_local_basic(tmp_path, write_mode):
    sink = MyWriteSink(str(tmp_path), write_mode=write_mode)
    data = {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "David"],
    }
    df = daft.from_pydict(data)
    df.write_sink(sink)
    written_df = daft.read_parquet(str(tmp_path))
    written_data = written_df.sort("id").to_pydict()

    assert written_data["id"] == data["id"]
    assert written_data["name"] == data["name"]


@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
def test_write_sink_multiple_partitions(tmp_path, write_mode):
    """Test write sink with multiple partitions."""
    data = {
        "id": list(range(100)),
        "value": [i * 2.5 for i in range(100)],
    }
    df = daft.from_pydict(data)
    df = df.into_partitions(4)

    sink = MyWriteSink(str(tmp_path), write_mode=write_mode)
    df.write_sink(sink)

    written_df = daft.read_parquet(str(tmp_path))
    written_data = written_df.sort("id").to_pydict()

    assert written_data["id"] == data["id"]
    assert written_data["value"] == data["value"]


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
def test_write_sink_remote_basic(write_mode, minio_io_config):
    bucket_name = "my-bucket"
    s3_path = f"s3://{bucket_name}/my-folder"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        sink = MyWriteSink(s3_path, write_mode=write_mode, io_config=minio_io_config)
        data = {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "David"],
        }
        df = daft.from_pydict(data)
        df.write_sink(sink)
        written_df = daft.read_parquet(s3_path)
        written_data = written_df.sort("id").to_pydict()

        assert written_data["id"] == data["id"]
        assert written_data["name"] == data["name"]


@pytest.mark.integration()
@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
def test_write_sink_remote_multiple_partitions(write_mode, minio_io_config):
    """Test write sink with multiple partitions."""
    bucket_name = "my-bucket"
    s3_path = f"s3://{bucket_name}/my-folder"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        data = {
            "id": list(range(100)),
            "value": [i * 2.5 for i in range(100)],
        }
        df = daft.from_pydict(data)
        df = df.into_partitions(4)

        sink = MyWriteSink(s3_path, write_mode=write_mode, io_config=minio_io_config)
        df.write_sink(sink)

        written_df = daft.read_parquet(s3_path)
        written_data = written_df.sort("id").to_pydict()

        assert written_data["id"] == data["id"]
        assert written_data["value"] == data["value"]
