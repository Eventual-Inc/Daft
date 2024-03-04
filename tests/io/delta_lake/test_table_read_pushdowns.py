from __future__ import annotations

import datetime

import pytest

from daft.delta_lake.delta_lake_scan import _io_config_to_storage_options

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa
import pyarrow.compute as pc

import daft
from daft.logical.schema import Schema
from tests.utils import assert_pyarrow_tables_equal

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="deltalake only supported if pyarrow >= 8.0.0")


def test_read_predicate_pushdown_on_data(deltalake_table):
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    df = df.where(df["a"] == 2)
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"), pa.concat_tables([table.filter(pc.field("a") == 2) for table in tables])
    )


def test_read_predicate_pushdown_on_part(deltalake_table, partition_generator):
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    part_idx = 2
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where(df["part_idx"] == part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") == part_value) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_non_eq(deltalake_table, partition_generator):
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    part_idx = 3
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where(df["part_idx"] < part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") < part_value) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_and_data(deltalake_table, partition_generator):
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    part_idx = 2
    partition_generator, _ = partition_generator
    part_value = partition_generator(part_idx)
    if part_value is None:
        part_value = part_idx
    df = df.where((df["part_idx"] == part_value) & (df["f"] == datetime.datetime(2024, 2, 11)))
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
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
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    partition_generator, col = partition_generator
    df = df.where(df["part_idx"] < df[col])
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") < pc.field(col)) for table in tables]),
    )


def test_read_predicate_pushdown_on_part_empty(deltalake_table, partition_generator, num_partitions):
    partition_generator, _ = partition_generator
    path, io_config, tables = deltalake_table
    df = daft.read_delta_lake(str(path), io_config=io_config)
    # There should only be num_partitions partitions; see local_deltalake_table fixture.
    part_value = partition_generator(num_partitions)
    if part_value is None:
        part_value = num_partitions
    df = df.where(df["part_idx"] == part_value)
    delta_schema = deltalake.DeltaTable(path, storage_options=_io_config_to_storage_options(io_config, path)).schema()
    expected_schema = Schema.from_pyarrow_schema(delta_schema.to_pyarrow())
    assert df.schema() == expected_schema
    assert_pyarrow_tables_equal(
        df.to_arrow().sort_by("part_idx"),
        pa.concat_tables([table.filter(pc.field("part_idx") == part_value) for table in tables]),
    )
