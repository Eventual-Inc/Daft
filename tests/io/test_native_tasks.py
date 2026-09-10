"""Tests for DataSourceTask.parquet() and _PyDataSourceTask."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft import DataType
from daft.daft import ParquetSourceConfig
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema


@pytest.fixture
def parquet_file(tmp_path: Path) -> str:
    """Create a simple parquet file and return its path."""
    table = pa.table({"x": [1, 2, 3, 4, 5], "y": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path / "test.parquet")
    pq.write_table(table, path)
    return path


@pytest.fixture
def partitioned_parquet_files(tmp_path: Path) -> list[tuple[str, int]]:
    """Create two parquet files simulating partitioned data. Returns (path, partition_value) pairs."""
    files = []
    for part_val in [10, 20]:
        table = pa.table({"x": list(range(part_val, part_val + 3))})
        path = str(tmp_path / f"part={part_val}" / "data.parquet")
        Path(path).parent.mkdir(parents=True)
        pq.write_table(table, path)
        files.append((path, part_val))
    return files


class SimpleParquetSource(DataSource):
    """A DataSource that reads parquet files via DataSourceTask.parquet()."""

    def __init__(self, paths: list[str], schema: Schema) -> None:
        self._paths = paths
        self._schema = schema

    @property
    def name(self) -> str:
        return "SimpleParquetSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        for path in self._paths:
            yield DataSourceTask.parquet(path=path, schema=self._schema, pushdowns=pushdowns)


def test_parquet_single_file(parquet_file: str):
    """DataSourceTask.parquet() reads a single parquet file through the native reader."""
    schema = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})
    source = SimpleParquetSource([parquet_file], schema)
    df = source.read()
    result = df.sort("x").to_pydict()
    assert result == {"x": [1, 2, 3, 4, 5], "y": ["a", "b", "c", "d", "e"]}


def test_parquet_multiple_files(tmp_path: Path):
    """DataSourceTask.parquet() handles multiple files as separate tasks."""
    schema = Schema.from_pydict({"x": DataType.int64()})
    paths = []
    for i in range(3):
        table = pa.table({"x": [i * 10 + j for j in range(3)]})
        path = str(tmp_path / f"file_{i}.parquet")
        pq.write_table(table, path)
        paths.append(path)

    source = SimpleParquetSource(paths, schema)
    df = source.read()
    result = df.sort("x").to_pydict()
    assert result == {"x": [0, 1, 2, 10, 11, 12, 20, 21, 22]}


def test_parquet_with_column_projection(parquet_file: str):
    """Column pushdowns work with DataSourceTask.parquet()."""
    schema = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})
    source = SimpleParquetSource([parquet_file], schema)
    df = source.read().select("x")
    result = df.sort("x").to_pydict()
    assert result == {"x": [1, 2, 3, 4, 5]}


def test_parquet_with_filter(parquet_file: str):
    """Filter pushdowns work with DataSourceTask.parquet()."""
    schema = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})
    source = SimpleParquetSource([parquet_file], schema)
    df = source.read().where(daft.col("x") > 3)
    result = df.sort("x").to_pydict()
    assert result == {"x": [4, 5], "y": ["d", "e"]}


def test_parquet_with_limit(parquet_file: str):
    """Limit pushdowns work with DataSourceTask.parquet()."""
    schema = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})
    source = SimpleParquetSource([parquet_file], schema)
    df = source.read().limit(2)
    result = df.to_pydict()
    assert len(result["x"]) == 2


def test_parquet_with_partition_values(partitioned_parquet_files: list[tuple[str, int]]):
    """DataSourceTask.parquet() with partition_values injects partition columns."""
    schema = Schema.from_pydict({"x": DataType.int64(), "part": DataType.int64()})

    class PartitionedParquetSource(DataSource):
        def __init__(self, files):
            self._files = files

        @property
        def name(self) -> str:
            return "PartitionedParquetSource"

        @property
        def schema(self) -> Schema:
            return schema

        async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
            for path, part_val in self._files:
                pv = RecordBatch.from_pydict({"part": [part_val]})
                yield DataSourceTask.parquet(
                    path=path,
                    schema=schema,
                    pushdowns=pushdowns,
                    partition_values=pv,
                    num_rows=3,
                )

    source = PartitionedParquetSource(partitioned_parquet_files)
    df = source.read()
    result = df.sort("x").to_pydict()
    assert result == {
        "x": [10, 11, 12, 20, 21, 22],
        "part": [10, 10, 10, 20, 20, 20],
    }


def test_parquet_collect_multiple_times(parquet_file: str):
    """DataSourceTask.parquet() sources can be collected multiple times."""
    schema = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})
    source = SimpleParquetSource([parquet_file], schema)
    df = source.read()
    assert df.count_rows() == 5
    assert df.count_rows() == 5
    assert len(df.collect()) == 5


def test_parquet_factory_returns_datasource_task():
    """DataSourceTask.parquet() returns a DataSourceTask instance."""
    schema = Schema.from_pydict({"x": DataType.int64()})
    task = DataSourceTask.parquet(path="/tmp/fake.parquet", schema=schema)
    assert isinstance(task, DataSourceTask)
    assert task.schema == schema


def test_parquet_factory_always_returns_task():
    """DataSourceTask.parquet() always returns a task (partition pruning is the DataSource's job)."""
    schema = Schema.from_pydict({"x": DataType.int64(), "part": DataType.int64()})
    pv = RecordBatch.from_pydict({"part": [10]})
    task = DataSourceTask.parquet(
        path="/tmp/fake.parquet",
        schema=schema,
        partition_values=pv,
    )
    assert isinstance(task, DataSourceTask)


def _write_parquet_with_field_ids(path: str, field_ids: dict[str, int]) -> None:
    """Write a parquet file whose columns carry ``PARQUET:field_id`` metadata."""
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    schema = pa.schema(
        [
            table.schema.field(name).with_metadata({b"PARQUET:field_id": str(field_ids[name]).encode()})
            for name in table.schema.names
        ]
    )
    pq.write_table(pa.Table.from_arrays(table.columns, schema=schema), path)


class FieldIdMappingSource(DataSource):
    """Reads one parquet file with a field ID mapping applied."""

    def __init__(self, path: str, schema: Schema) -> None:
        self._path = path
        self._schema = schema

    @property
    def name(self) -> str:
        return "FieldIdMappingSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        mapping = {1: self._schema["a"]._field, 2: self._schema["b"]._field}
        yield DataSourceTask.parquet(
            path=self._path,
            schema=self._schema,
            pushdowns=pushdowns,
            parquet_config=ParquetSourceConfig(field_id_mapping=mapping),
        )


def test_field_id_mapping_without_field_ids_raises(tmp_path: Path):
    """A field ID mapping against a file with no field IDs is an error, not all-null columns."""
    path = str(tmp_path / "no_field_ids.parquet")
    pq.write_table(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}), path)
    schema = Schema.from_pydict({"a": DataType.int64(), "b": DataType.string()})

    with pytest.raises(Exception, match="carries no parquet field IDs"):
        FieldIdMappingSource(path, schema).read().to_pydict()


def test_field_id_mapping_with_field_ids_reads_values(tmp_path: Path):
    """A file that does carry field IDs still reads through the mapping."""
    path = str(tmp_path / "with_field_ids.parquet")
    _write_parquet_with_field_ids(path, {"a": 1, "b": 2})
    schema = Schema.from_pydict({"a": DataType.int64(), "b": DataType.string()})

    result = FieldIdMappingSource(path, schema).read().sort("a").to_pydict()

    assert result == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
