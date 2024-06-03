from __future__ import annotations

import contextlib
import sys

import pyarrow as pa
import pytest

import daft
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.schema import Schema


@contextlib.contextmanager
def split_small_pq_files():
    old_config = daft.context.get_context().daft_execution_config
    daft.set_execution_config(
        # Splits any parquet files >100 bytes in size
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=100,
    )
    yield
    daft.set_execution_config(config=old_config)


PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
PYTHON_LT_3_8 = sys.version_info[:2] < (3, 8)
pytestmark = pytest.mark.skipif(
    PYARROW_LE_8_0_0 or PYTHON_LT_3_8, reason="deltalake only supported if pyarrow >= 8.0.0 and python >= 3.8"
)


def test_deltalake_write_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    result = df.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def test_deltalake_multi_write_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    df.write_deltalake(str(path))

    result = df.write_deltalake(str(path))
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert read_delta.version() == 1
    assert read_delta.to_pyarrow_table() == pa.concat_tables([base_table, base_table])


def test_deltalake_write_cloud(base_table, cloud_paths):
    deltalake = pytest.importorskip("deltalake")
    path, io_config, catalog_table = cloud_paths
    df = daft.from_arrow(base_table)
    result = df.write_deltalake(str(path), io_config=io_config)
    result = result.to_pydict()
    assert result["operation"] == ["ADD"]
    assert result["rows"] == [base_table.num_rows]
    storage_options = io_config_to_storage_options(io_config, path) if io_config is not None else None
    read_delta = deltalake.DeltaTable(str(path), storage_options=storage_options)
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def test_deltalake_write_overwrite_basic(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]
    assert result["rows"] == [2, 2]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_cloud(cloud_paths):
    deltalake = pytest.importorskip("deltalake")
    path, io_config, catalog_table = cloud_paths
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), io_config=io_config)

    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite", io_config=io_config)
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]
    assert result["rows"] == [2, 2]

    storage_options = io_config_to_storage_options(io_config, path) if io_config is not None else None
    read_delta = deltalake.DeltaTable(str(path), storage_options=storage_options)
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_multi_partition(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2, 3, 4]})
    df1 = df1.repartition(2)
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"a": [5, 6, 7, 8]})
    df2 = df2.repartition(2)
    result = df2.write_deltalake(str(path), mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "ADD", "DELETE", "DELETE"]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_schema(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path))

    df2 = daft.from_pydict({"b": [3, 4]})
    result = df2.write_deltalake(str(path), mode="overwrite", schema_mode="overwrite")
    result = result.to_pydict()
    assert result["operation"] == ["ADD", "DELETE"]

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df2.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df2.to_arrow()


def test_deltalake_write_overwrite_error_schema(tmp_path):
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), mode="overwrite")
    df2 = daft.from_pydict({"b": [3, 4]})
    with pytest.raises(ValueError):
        df2.write_deltalake(str(path), mode="overwrite")


def test_deltalake_write_error(tmp_path, base_table):
    path = tmp_path / "some_table"
    df = daft.from_arrow(base_table)
    df.write_deltalake(str(path), mode="error")
    with pytest.raises(AssertionError):
        df.write_deltalake(str(path), mode="error")


def test_deltalake_write_ignore(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    df1 = daft.from_pydict({"a": [1, 2]})
    df1.write_deltalake(str(path), mode="ignore")
    df2 = daft.from_pydict({"a": [3, 4]})
    result = df2.write_deltalake(str(path), mode="ignore")
    result = result.to_arrow()
    assert result.num_rows == 0

    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df1.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == df1.to_arrow()
