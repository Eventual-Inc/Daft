from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import contextlib

import pyarrow as pa

import daft
from tests.conftest import assert_df_equals


@contextlib.contextmanager
def table_written_by_pyiceberg(local_iceberg_catalog):
    schema = pa.schema([("col", pa.int64()), ("mapCol", pa.map_(pa.int32(), pa.string()))])

    data = {"col": [1, 2, 3], "mapCol": [[(1, "foo"), (2, "bar")], [(3, "baz")], [(4, "foobar")]]}
    arrow_table = pa.Table.from_pydict(data, schema=schema)
    try:
        table = local_iceberg_catalog.create_table("pyiceberg.map_table", schema=schema)
        table.append(arrow_table)
        yield table
    except Exception as e:
        raise e
    finally:
        local_iceberg_catalog.drop_table("pyiceberg.map_table")


@contextlib.contextmanager
def table_written_by_daft(local_iceberg_catalog):
    schema = pa.schema([("col", pa.int64()), ("mapCol", pa.map_(pa.int32(), pa.string()))])

    data = {"col": [1, 2, 3], "mapCol": [[(1, "foo"), (2, "bar")], [(3, "baz")], [(4, "foobar")]]}
    arrow_table = pa.Table.from_pydict(data, schema=schema)
    try:
        table = local_iceberg_catalog.create_table("pyiceberg.map_table", schema=schema)
        df = daft.from_arrow(arrow_table)
        df.write_iceberg(table, mode="overwrite")
        table.refresh()
        yield table
    except Exception as e:
        raise e
    finally:
        local_iceberg_catalog.drop_table("pyiceberg.map_table")


@pytest.mark.integration()
def test_pyiceberg_written_catalog(local_iceberg_catalog):
    with table_written_by_pyiceberg(local_iceberg_catalog) as catalog_table:
        df = daft.read_iceberg(catalog_table)
        daft_pandas = df.to_pandas()
        iceberg_pandas = catalog_table.scan().to_arrow().to_pandas()
        assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.skip
def test_daft_written_catalog(local_iceberg_catalog):
    with table_written_by_daft(local_iceberg_catalog) as catalog_table:
        df = daft.read_iceberg(catalog_table)
        daft_pandas = df.to_pandas()
        iceberg_pandas = catalog_table.scan().to_arrow().to_pandas()
        assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])
