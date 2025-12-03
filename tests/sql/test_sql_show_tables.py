from __future__ import annotations

import pytest

from daft import Catalog, Session, from_pydict


@pytest.fixture
def sess():
    cat1 = Catalog.from_pydict(
        name="cat1",
        tables={
            "aa_table": {"x": [1]},
            "bb_table": {"y": [2]},
        },
    )
    cat2 = Catalog.from_pydict(
        name="cat2",
        tables={
            "cc_table": {"x": [1]},
            "dd_table": {"y": [2]},
        },
    )
    sess = Session()
    sess.attach_catalog(cat1)
    sess.attach_catalog(cat2)
    return sess


@pytest.fixture
def sess_with_namespaces():
    cat = Catalog.from_pydict(name="cat1", tables={})

    cat.create_namespace("ns1")
    cat.create_namespace("ns2")

    cat.create_table("ns1.table1", from_pydict({"a": [1]}))
    cat.create_table("ns1.table2", from_pydict({"b": [2]}))
    cat.create_table("ns2.table1", from_pydict({"c": [3]}))

    sess = Session()
    sess.attach_catalog(cat)
    return sess


@pytest.fixture
def sess_with_wildcard_tables():
    cat = Catalog.from_pydict(
        tables={
            "data_": {"z": [1]},
            "%table": {"w": [2]},
            "test\\data": {"v": [3]},
        },
    )
    sess = Session()
    sess.attach_catalog(cat)
    return sess


def test_show_tables(sess):
    res = sess.sql("SHOW TABLES").to_pydict()
    assert "cat1" in res["catalog"]
    assert len(res["table"]) == 2
    assert "aa_table" in res["table"]
    assert "bb_table" in res["table"]


def test_show_tables_all(sess):
    actual = sess.sql("SHOW TABLES").to_pydict()
    expect = {
        "catalog": [
            "cat1",
            "cat1",
        ],
        "namespace": [
            None,
            None,
        ],
        "table": [
            "aa_table",
            "bb_table",
        ],
    }
    assert actual == expect


def test_show_tables_with_pattern(sess):
    # test: show tables in the current catalog and current namespace matching the pattern
    res = sess.sql("SHOW TABLES LIKE 'aa%'").to_pydict()
    assert len(res["table"]) == 1
    assert "aa_table" in res["table"]

    res = sess.sql("SHOW TABLES LIKE '%table'").to_pydict()
    assert len(res["table"]) == 2
    assert "aa_table" in res["table"]
    assert "bb_table" in res["table"]

    res = sess.sql("SHOW TABLES LIKE '%a%'").to_pydict()
    assert len(res["table"]) == 2
    assert "aa_table" in res["table"]
    assert "bb_table" in res["table"]

    res = sess.sql("SHOW TABLES LIKE '___table'").to_pydict()
    assert len(res["table"]) == 2
    assert "aa_table" in res["table"]
    assert "bb_table" in res["table"]

    res = sess.sql("SHOW TABLES LIKE 'aa_table'").to_pydict()
    assert len(res["table"]) == 1
    assert "aa_table" in res["table"]

    res = sess.sql("SHOW TABLES LIKE '%_able'").to_pydict()
    assert len(res["table"]) == 2
    assert "aa_table" in res["table"]
    assert "bb_table" in res["table"]

    # test: nonexistent pattern
    res = sess.sql("SHOW TABLES LIKE 'nonexistent'").to_pydict()
    assert len(res["table"]) == 0
    res = sess.sql("SHOW TABLES LIKE '%xyz'").to_pydict()
    assert len(res["table"]) == 0
    res = sess.sql("SHOW TABLES LIKE ''").to_pydict()
    assert len(res["table"]) == 0


def test_show_tables_like_filters_table_names(sess_with_namespaces):
    res = sess_with_namespaces.sql("SHOW TABLES LIKE 'table1'").to_pydict()
    assert sorted(res["table"]) == ["table1", "table1"]
    assert sorted(res["namespace"]) == ["ns1", "ns2"]


def test_show_tables_like_with_qualified_pattern(sess_with_namespaces):
    # Test that qualified patterns (namespace.pattern) work correctly
    res = sess_with_namespaces.sql("SHOW TABLES LIKE 'ns1.table%'").to_pydict()
    assert len(res["table"]) == 2
    assert sorted(res["table"]) == ["table1", "table2"]
    assert all(ns == "ns1" for ns in res["namespace"])


def test_show_tables_with_special_chars(sess_with_wildcard_tables):
    # test: show tables with special characters in their names
    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE 'data_%'").to_pydict()
    assert len(res["table"]) == 1
    assert "data_" in res["table"]

    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE '%table'").to_pydict()
    assert len(res["table"]) == 1
    assert "%table" in res["table"]

    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE 'test\\\\%'").to_pydict()
    assert len(res["table"]) == 1
    assert "test\\data" in res["table"]

    # Escape sequences with wildcards
    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE 'data\\_'").to_pydict()
    assert len(res["table"]) == 1
    assert "data_" in res["table"]

    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE '\\%table'").to_pydict()
    assert len(res["table"]) == 1
    assert "%table" in res["table"]

    res = sess_with_wildcard_tables.sql("SHOW TABLES LIKE '%\\\\%'").to_pydict()
    assert len(res["table"]) == 1
    assert "test\\data" in res["table"]


def test_show_tables_in_catalog(sess):
    # test: show tables in catalog `cat2`
    res = sess.sql("SHOW TABLES FROM cat2").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 2
    assert "cc_table" in res["table"]
    assert "dd_table" in res["table"]


def test_show_tables_in_catalog_with_pattern(sess):
    # test: show tables in catalog `cat2` matching the pattern
    res = sess.sql("SHOW TABLES FROM cat2 LIKE 'cc%'").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 1
    assert "cc_table" in res["table"]

    res = sess.sql("SHOW TABLES FROM cat1 LIKE 'aa%'").to_pydict()
    assert "cat1" in res["catalog"]
    assert len(res["table"]) == 1
    assert "aa_table" in res["table"]


def test_show_tables_in_catalog_namespace(sess_with_namespaces):
    # test: show tables in catalog `cat1` and namespace `ns1`
    result = sess_with_namespaces.sql("SHOW TABLES FROM cat1.ns1")
    tables = result.to_pydict()["table"]
    assert len(tables) == 2  # Only tables in ns1
    assert "table1" in tables
    assert "table2" in tables


def test_show_tables_in_catalog_namespace_with_pattern(sess_with_namespaces):
    # test: show tables in catalog `cat1` and namespace `ns1` matching the pattern
    result = sess_with_namespaces.sql("SHOW TABLES FROM cat1.ns1 LIKE 'table1'")
    tables = result.to_pydict()["table"]
    assert len(tables) == 1
    assert "table1" in tables
    assert "table2" not in tables
