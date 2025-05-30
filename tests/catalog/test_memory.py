from __future__ import annotations

import pytest

import daft
from daft import col
from daft.catalog import Catalog, Identifier, Table


def test_table_from_pydict():
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}

    table = Table.from_pydict("foo", data)

    assert table.name == "foo"
    assert table.read().to_pydict() == data


def test_table_from_df():
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}

    df = daft.from_pydict(data)

    table = Table.from_df("foo", df)

    assert table.name == "foo"
    assert table.read().to_pydict() == data


def test_catalog_from_pydict():
    data1 = {"a": [1, 2, 3], "b": ["a", "b", "c"]}

    data2 = {
        "c": [1, 3, 5],
    }

    data3 = {
        "d": [0.5, 1.5],
        "e": [False, True],
    }

    catalog = Catalog.from_pydict(
        {"foo": data1, "bar": daft.from_pydict(data2), "baz": Table.from_pydict("foo", data3)}, "cat"
    )

    assert catalog.name == "cat"
    assert sorted(str(ident) for ident in catalog.list_tables()) == ["bar", "baz", "foo"]
    assert catalog.has_table("foo")
    assert catalog.has_table("bar")
    assert catalog.has_table("baz")
    assert not catalog.has_table("cat")

    assert catalog.get_table("foo").read().to_pydict() == data1
    assert catalog.get_table("bar").read().to_pydict() == data2
    assert catalog.get_table("baz").read().to_pydict() == data3


def test_catalog_create_table_from_schema():
    catalog = Catalog.from_pydict({}, "cat")

    schema = daft.Schema.from_pydict(
        {
            "a": daft.DataType.int64(),
            "b": daft.DataType.string(),
        }
    )
    tbl = catalog.create_table("tab", schema)
    assert tbl.name == "tab"
    assert tbl.schema() == schema
    assert tbl.read().to_pydict() == {"a": [], "b": []}

    assert catalog.has_table("tab")
    tbl = catalog.get_table("tab")
    assert tbl.name == "tab"
    assert tbl.schema() == schema
    assert tbl.read().to_pydict() == {"a": [], "b": []}


def test_catalog_create_table_from_dataframe():
    catalog = Catalog.from_pydict({}, "cat")

    data = {"a": [1, 2, 3], "b": ["a", "b", "c"]}
    df = daft.from_pydict(data)
    tbl = catalog.create_table("tab", df)
    assert tbl.name == "tab"
    assert tbl.schema() == df.schema()
    assert tbl.read().to_pydict() == data

    assert catalog.has_table("tab")
    tbl = catalog.get_table("tab")
    assert tbl.name == "tab"
    assert tbl.schema() == df.schema()
    assert tbl.read().to_pydict() == data


def test_catalog_drop_table():
    catalog = Catalog.from_pydict({"foo": {"a": [1, 2, 3], "b": ["a", "b", "c"]}}, "cat")

    assert catalog.has_table("foo")
    catalog.drop_table("foo")
    assert not catalog.has_table("foo")

    data = {"a": [1], "b": ["x"]}

    catalog.create_table("foo", daft.from_pydict(data))
    assert catalog.has_table("foo")


def test_table_append():
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    table = Table.from_df("foo", df)

    df2 = daft.from_pydict({"a": [4, 5, 6], "b": ["a", "b", "c"]})

    table.append(df2)

    assert table.read().sort(col("a")).to_pydict() == {"a": [1, 2, 3, 4, 5, 6], "b": ["x", "y", "z", "a", "b", "c"]}


def test_table_overwrite():
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    table = Table.from_df("foo", df)

    df2 = daft.from_pydict({"a": [4, 5, 6], "b": ["a", "b", "c"]})

    table.overwrite(df2)

    assert table.read().sort(col("a")).to_pydict() == {"a": [4, 5, 6], "b": ["a", "b", "c"]}


def test_namespace_operations():
    catalog = Catalog.from_pydict({}, "cat")

    # Initially no namespaces
    assert catalog.list_namespaces() == []

    # Create a namespace
    catalog.create_namespace("ns1")
    assert catalog.has_namespace("ns1")
    assert catalog.list_namespaces() == [Identifier("ns1")]

    # Create a table in namespace
    data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    catalog.create_table("ns1.table1", daft.from_pydict(data))
    assert catalog.has_table("ns1.table1")
    assert not catalog.has_table("table1")  # Table only exists in namespace

    # List tables should show namespaced table
    assert sorted(str(ident) for ident in catalog.list_tables()) == ["ns1.table1"]
    assert sorted(str(ident) for ident in catalog.list_tables("ns1")) == ["ns1.table1"]

    # Create another namespace
    catalog.create_namespace("ns2")
    assert sorted(str(ns) for ns in catalog.list_namespaces()) == ["ns1", "ns2"]

    # Drop empty namespace
    catalog.drop_namespace("ns2")
    assert not catalog.has_namespace("ns2")
    assert catalog.list_namespaces() == [Identifier("ns1")]

    # Drop namespace with content
    catalog.drop_namespace("ns1")
    assert not catalog.has_namespace("ns1")
    assert not catalog.has_table("ns1.table1")
    assert catalog.list_tables() == []


def test_namespace_errors():
    catalog = Catalog.from_pydict({}, "cat")

    # Test duplicate namespace creation
    catalog.create_namespace("ns1")
    with pytest.raises(Exception):
        catalog.create_namespace("ns1")

    # Test dropping non-existent namespace
    with pytest.raises(Exception):
        catalog.drop_namespace("nonexistent")

    # Test nested namespaces not supported
    with pytest.raises(Exception):
        catalog.create_namespace("ns1.ns2")

    # Test accessing table in non-existent namespace
    with pytest.raises(Exception):
        catalog.create_table("nonexistent.table", daft.from_pydict({"a": [1]}))


def test_namespace_table_operations():
    catalog = Catalog.from_pydict({}, "cat")

    # Create namespace and tables
    catalog.create_namespace("ns1")
    data1 = {"a": [1, 2, 3]}
    data2 = {"b": [4, 5, 6]}

    catalog.create_table("ns1.table1", daft.from_pydict(data1))
    catalog.create_table("ns1.table2", daft.from_pydict(data2))

    # Verify tables exist and have correct data
    table1 = catalog.get_table("ns1.table1")
    assert table1.read().to_pydict() == data1

    table2 = catalog.get_table("ns1.table2")
    assert table2.read().to_pydict() == data2

    # Drop table and verify namespace still exists
    catalog.drop_table("ns1.table1")
    assert not catalog.has_table("ns1.table1")
    assert catalog.has_table("ns1.table2")
    assert catalog.has_namespace("ns1")
