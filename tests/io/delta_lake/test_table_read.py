from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(
    PYARROW_LOWER_BOUND_SKIP,
    reason="deltalake not supported on older versions of pyarrow",
)


def test_deltalake_read_basic(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, base_table)
    df = daft.read_deltalake(str(path))
    expected_schema = Schema.from_pyarrow_schema(pa.schema(deltalake.DeltaTable(path).schema().to_arrow()))
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow(), base_table)


def test_deltalake_read_full(deltalake_table):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, parts = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(pa.schema(delta_schema.to_arrow()))
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
    writer_properties = deltalake.WriterProperties(max_row_group_size=2)
    deltalake.write_deltalake(path, base_table, writer_properties=writer_properties)

    # Force file splitting
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=100,
    ):
        df = daft.read_deltalake(str(path))
        df.collect()
        assert len(df) == 3, "Length of non-materialized data when read through deltalake should be correct"


def test_deltalake_read_row_group_splits_with_filter(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"

    # Force 2 rowgroups
    writer_properties = deltalake.WriterProperties(max_row_group_size=2)
    deltalake.write_deltalake(path, base_table, writer_properties=writer_properties)

    # Force file splitting
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=100,
    ):
        df = daft.read_deltalake(str(path))
        df = df.where(df["a"] > 1)
        df.collect()
        assert len(df) == 2, "Length of non-materialized data when read through deltalake should be correct"


def test_deltalake_read_row_group_splits_with_limit(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"

    # Force 2 rowgroups
    writer_properties = deltalake.WriterProperties(max_row_group_size=2)
    deltalake.write_deltalake(path, base_table, writer_properties=writer_properties)

    # Force file splitting
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=100,
    ):
        df = daft.read_deltalake(str(path))
        df = df.limit(2)
        df.collect()
        assert len(df) == 2, "Length of non-materialized data when read through deltalake should be correct"


def test_deltalake_read_versioned(tmp_path, base_table):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "some_table"
    deltalake.write_deltalake(path, base_table)

    updated_columns = base_table.columns + [pa.array(["x", "y", "z"])]
    updated_column_names = base_table.column_names + ["new_column"]
    updated_table = pa.Table.from_arrays(updated_columns, names=updated_column_names)
    deltalake.write_deltalake(path, updated_table, mode="overwrite", schema_mode="overwrite")

    for version in [None, 1]:
        df = daft.read_deltalake(str(path), version=version)
        expected_schema = Schema.from_pyarrow_schema(pa.schema(deltalake.DeltaTable(path).schema().to_arrow()))
        assert df.schema() == expected_schema
        assert_pyarrow_tables_equal(df.to_arrow(), updated_table)

    df = daft.read_deltalake(str(path), version=0)
    expected_schema = Schema.from_pyarrow_schema(pa.schema(deltalake.DeltaTable(path, version=0).schema().to_arrow()))
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(df.to_arrow(), base_table)
