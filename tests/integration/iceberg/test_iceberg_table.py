"""Integration tests for the Iceberg Table actions."""

from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import pyarrow as pa
from pyiceberg.catalog import Catalog as PyIcebergCatalog

import daft
from daft.catalog import Table


def assert_eq(df1, df2):
    assert df1.to_pydict() == df2.to_pydict()


@pytest.fixture()
def table(local_iceberg_catalog):
    """A fresh table for each test case."""
    catalog: PyIcebergCatalog = local_iceberg_catalog[1]
    namespace = "test_iceberg_table"
    catalog.create_namespace_if_not_exists(namespace)
    try:
        t = catalog.create_table(
            f"{namespace}.T",
            schema=pa.schema(
                [
                    ("a", pa.bool_()),
                    ("b", pa.int64()),
                    ("c", pa.string()),
                ]
            ),
        )
        yield Table.from_iceberg(t)
    except Exception as e:
        raise e
    finally:
        catalog.drop_table(f"{namespace}.T")
        catalog.drop_namespace(namespace)


part1 = daft.from_pydict({"a": [True, True, False], "b": [1, 2, 3], "c": ["abc", "def", "ghi"]})


part2 = daft.from_pydict({"a": [False, False, True], "b": [4, 5, 6], "c": ["jkl", "mno", "pqr"]})


@pytest.mark.integration()
def test_append(table):
    table.append(part1)
    assert_eq(table.read(), part1)
    #
    table.append(part2)
    assert_eq(table.read(), part2.concat(part1))


@pytest.mark.integration()
def test_ovewrite(table):
    table.append(part1)
    assert_eq(table.read(), part1)
    #
    table.overwrite(part2)
    assert_eq(table.read(), part2)
