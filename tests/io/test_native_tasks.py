"""Tests for DataSourceTask.parquet() and _PyDataSourceTask."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft import DataType
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


@pytest.fixture
def deleted_rows_parquet(tmp_path: Path) -> tuple[str, str]:
    """200 rows across 4 row groups, with positional deletes for rows 0..39.

    Row group 0 keeps only 10 of its 50 rows, so a limit budget computed from
    pre-delete row counts is exhausted before later row groups are considered.
    """
    data_path = str(tmp_path / "data.parquet")
    pq.write_table(pa.table({"x": list(range(200))}), data_path, row_group_size=50)

    delete_path = str(tmp_path / "delete.parquet")
    pq.write_table(
        pa.table({"file_path": [data_path] * 40, "pos": list(range(40))}),
        delete_path,
    )
    return data_path, delete_path


class DeletedRowsParquetSource(DataSource):
    """Reads one parquet file with Iceberg positional delete files attached."""

    def __init__(self, data_path: str, delete_path: str, schema: Schema) -> None:
        self._data_path = data_path
        self._delete_path = delete_path
        self._schema = schema

    @property
    def name(self) -> str:
        return "DeletedRowsParquetSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        yield DataSourceTask.parquet(
            path=self._data_path,
            schema=self._schema,
            pushdowns=pushdowns,
            iceberg_delete_files=[self._delete_path],
        )


@pytest.mark.parametrize("limit", [5, 10, 20, 45, 60, 100, 160, 200])
def test_parquet_limit_accounts_for_deleted_rows(deleted_rows_parquet: tuple[str, str], limit: int):
    """Deleted rows must not consume the limit budget.

    Row groups are pruned against `num_rows` before decoding. Counting deleted
    rows towards that budget drops later row groups, so the read returns fewer
    rows than the limit even though the file still has matching rows.
    """
    data_path, delete_path = deleted_rows_parquet
    schema = Schema.from_pydict({"x": DataType.int64()})
    source = DeletedRowsParquetSource(data_path, delete_path, schema)

    result = source.read().limit(limit).to_pydict()

    assert len(result["x"]) == min(limit, 160)
    # None of the deleted rows (0..39) may be returned.
    assert all(x >= 40 for x in result["x"])


def test_parquet_reads_all_undeleted_rows(deleted_rows_parquet: tuple[str, str]):
    """Without a limit the read returns exactly the undeleted rows."""
    data_path, delete_path = deleted_rows_parquet
    schema = Schema.from_pydict({"x": DataType.int64()})
    source = DeletedRowsParquetSource(data_path, delete_path, schema)

    result = source.read().sort("x").to_pydict()

    assert result == {"x": list(range(40, 200))}
