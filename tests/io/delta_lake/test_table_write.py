from __future__ import annotations

import contextlib
import sys

import pyarrow as pa
import pytest

import daft
from daft.delta_lake.delta_lake_storage_function import _io_config_to_storage_options
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
    df.write_delta(str(path))
    read_delta = deltalake.DeltaTable(str(path))
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table


def test_deltalake_write_cloud(base_table, cloud_paths):
    deltalake = pytest.importorskip("deltalake")
    path, io_config, catalog_table = cloud_paths
    df = daft.from_arrow(base_table)
    df.write_delta(str(path), io_config=io_config)
    storage_options = _io_config_to_storage_options(io_config, path) if io_config is not None else None
    read_delta = deltalake.DeltaTable(str(path), storage_options=storage_options)
    expected_schema = Schema.from_pyarrow_schema(read_delta.schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert read_delta.to_pyarrow_table() == base_table
