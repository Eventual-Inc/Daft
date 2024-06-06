from __future__ import annotations

import contextlib
import sys

import pyarrow as pa
import pytest

import daft
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal


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


def test_deltalake_read_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, base_table)
    df = daft.read_deltalake(str(path))
    expected_schema = Schema.from_pyarrow_schema(deltalake.DeltaTable(path).schema().to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow(), base_table)


def test_deltalake_read_full(deltalake_table):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, parts = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow().sort_by("part_idx"), pa.concat_tables(parts).sort_by("part_idx"))


def test_deltalake_read_show(deltalake_table):
    path, catalog_table, io_config, _ = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    df.show()


def test_deltalake_read_row_group_splits(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"

    # Force 2 rowgroups
    deltalake.write_deltalake(path, base_table, min_rows_per_group=1, max_rows_per_group=2)

    # Force file splitting
    with split_small_pq_files():
        df = daft.read_deltalake(str(path))
        df.collect()
        assert len(df) == 3, "Length of non-materialized data when read through deltalake should be correct"


def test_deltalake_read_row_group_splits_with_filter(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"

    # Force 2 rowgroups
    deltalake.write_deltalake(path, base_table, min_rows_per_group=1, max_rows_per_group=2)

    # Force file splitting
    with split_small_pq_files():
        df = daft.read_deltalake(str(path))
        df = df.where(df["a"] > 1)
        df.collect()
        assert len(df) == 2, "Length of non-materialized data when read through deltalake should be correct"


def test_deltalake_read_row_group_splits_with_limit(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"

    # Force 2 rowgroups
    deltalake.write_deltalake(path, base_table, min_rows_per_group=1, max_rows_per_group=2)

    # Force file splitting
    with split_small_pq_files():
        df = daft.read_deltalake(str(path))
        df = df.limit(2)
        df.collect()
        assert len(df) == 2, "Length of non-materialized data when read through deltalake should be correct"
