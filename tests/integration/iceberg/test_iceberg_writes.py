from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import contextlib

import pyarrow as pa

import daft
from tests.conftest import assert_df_equals


@contextlib.contextmanager
def table_written_by_pyiceberg(local_pyiceberg_catalog):
    schema = pa.schema([("col", pa.int64()), ("mapCol", pa.map_(pa.int32(), pa.string()))])

    data = {"col": [1, 2, 3], "mapCol": [[(1, "foo"), (2, "bar")], [(3, "baz")], [(4, "foobar")]]}
    arrow_table = pa.Table.from_pydict(data, schema=schema)
    table_name = "pyiceberg.map_table"
    try:
        table = local_pyiceberg_catalog.create_table(table_name, schema=schema)
        table.append(arrow_table)
        yield table_name
    except Exception as e:
        raise e
    finally:
        local_pyiceberg_catalog.drop_table(table_name)


@contextlib.contextmanager
def table_written_by_daft(local_pyiceberg_catalog):
    schema = pa.schema([("col", pa.int64()), ("mapCol", pa.map_(pa.int32(), pa.string()))])

    data = {"col": [1, 2, 3], "mapCol": [[(1, "foo"), (2, "bar")], [(3, "baz")], [(4, "foobar")]]}
    arrow_table = pa.Table.from_pydict(data, schema=schema)
    table_name = "pyiceberg.map_table"
    try:
        table = local_pyiceberg_catalog.create_table(table_name, schema=schema)
        df = daft.from_arrow(arrow_table)
        df.write_iceberg(table, mode="overwrite")
        table.refresh()
        yield table_name
    except Exception as e:
        raise e
    finally:
        local_pyiceberg_catalog.drop_table(table_name)


@pytest.mark.integration()
def test_pyiceberg_written_catalog(local_iceberg_catalog):
    catalog_name, local_pyiceberg_catalog = local_iceberg_catalog
    with table_written_by_pyiceberg(local_pyiceberg_catalog) as catalog_table_name:
        df = daft.read_table(f"{catalog_name}.{catalog_table_name}")
        daft_pandas = df.to_pandas()
        iceberg_pandas = local_pyiceberg_catalog.load_table(catalog_table_name).scan().to_arrow().to_pandas()
        assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])


@pytest.mark.integration()
@pytest.mark.skip
def test_daft_written_catalog(local_iceberg_catalog):
    catalog_name, local_pyiceberg_catalog = local_iceberg_catalog
    with table_written_by_daft(local_pyiceberg_catalog) as catalog_table_name:
        df = daft.read_table(f"{catalog_name}.{catalog_table_name}")
        daft_pandas = df.to_pandas()
        iceberg_pandas = local_pyiceberg_catalog.load_table(catalog_table_name).scan().to_arrow().to_pandas()
        assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])
