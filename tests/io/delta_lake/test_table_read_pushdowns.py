from __future__ import annotations

import datetime

import pytest

from daft.io.object_store_options import io_config_to_storage_options

deltalake = pytest.importorskip("deltalake")
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import daft
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
PYTHON_LT_3_8 = sys.version_info[:2] < (3, 8)
pytestmark = pytest.mark.skipif(
    PYARROW_LE_8_0_0 or PYTHON_LT_3_8, reason="deltalake only supported if pyarrow >= 8.0.0 and python >= 3.8"
)


def test_read_predicate_pushdown_on_data(deltalake_table):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    df = df.where(df["a"] == 2)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("a") == 2) for table in tables]).sort_by("part_idx"),
    )


def test_read_predicate_pushdown_on_part(deltalake_table, partition_generator):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    part_idx = 2
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where(df["part_idx"] == part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") == part_value) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_non_eq(deltalake_table, partition_generator):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    part_idx = 3
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where(df["part_idx"] < part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") < part_value) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_and_data(deltalake_table, partition_generator):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    part_idx = 2
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where((df["part_idx"] == part_value) & (df["f"] == datetime.datetime(2024, 2, 11)))
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables(
            [
                table.filter((pc.field("part_idx") == part_value) & (pc.field("f") == datetime.datetime(2024, 2, 11)))
                for table in tables
            ]
        ),
    )


def test_read_predicate_pushdown_on_part_and_data_same_clause(deltalake_table, partition_generator):
    deltalake = pytest.importorskip("deltalake")
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    partition_generator, col = partition_generator
    df = df.where(df["part_idx"] < df[col])
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") < pc.field(col)) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_empty(deltalake_table, partition_generator, num_partitions):
    deltalake = pytest.importorskip("deltalake")
    partition_generator, _ = partition_generator
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)
    # There should only be num_partitions partitions; see local_deltalake_table fixture.
    part_value = partition_generator(num_partitions)
    if part_value is None:
        part_value = num_partitions
    df = df.where(df["part_idx"] == part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") == part_value) for table in tables]),
    )


def test_read_select_partition_key(deltalake_table):
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)

    df = df.select("part_idx", "a")

    assert df.schema().column_names() == ["part_idx", "a"]

    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by([("part_idx", "ascending"), ("a", "ascending")]),
        pa.concat_tables([table.select(["part_idx", "a"]) for table in tables]).sort_by(
            [("part_idx", "ascending"), ("a", "ascending")]
        ),
    )


def test_read_select_partition_key_with_filter(deltalake_table):
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)

    df = df.select("part_idx", "a")
    df = df.where(df["a"] < 5)

    assert df.schema().column_names() == ["part_idx", "a"]

    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by([("part_idx", "ascending"), ("a", "ascending")]),
        pa.concat_tables([table.select(["part_idx", "a"]) for table in tables]).sort_by(
            [("part_idx", "ascending"), ("a", "ascending")]
        ),
    )


@pytest.mark.skip(
    reason="Selecting just the partition key in a deltalake table is not yet supported. "
    "Issue: https://github.com/Eventual-Inc/Daft/issues/2129"
)
def test_read_select_only_partition_key(deltalake_table):
    path, catalog_table, io_config, tables = deltalake_table
    df = daft.read_deltalake(str(path) if catalog_table is None else catalog_table, io_config=io_config)

    df = df.select("part_idx")

    assert df.schema().column_names() == ["part_idx"]

    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.select(["part_idx"]) for table in tables]).sort_by("part_idx"),
    )
