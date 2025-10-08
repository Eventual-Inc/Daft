from __future__ import annotations

import pytest

from daft import Catalog, Session


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


@pytest.mark.skip(
    "`SHOW TABLES LIKE` is not properly supported yet. See https://github.com/Eventual-Inc/Daft/issues/4461"
)
def test_show_tables_with_pattern(sess):
    # test: show tables in the current catalog and current namespace matching the pattern
    res = sess.sql("SHOW TABLES LIKE 'aa'").to_pydict()
    assert len(res["table"]) == 1
    assert "aa_table" in res["table"]


def test_show_tables_in_catalog(sess):
    # test: show tables in catalog `cat2`
    res = sess.sql("SHOW TABLES FROM cat2").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 2
    assert "cc_table" in res["table"]
    assert "dd_table" in res["table"]


@pytest.mark.skip(
    "`SHOW TABLES LIKE` is not properly supported yet. See https://github.com/Eventual-Inc/Daft/issues/4461"
)
def test_show_tables_in_catalog_with_pattern(sess):
    # test: show tables in catalog `cat2` matching the pattern
    res = sess.sql("SHOW TABLES FROM cat2 LIKE 'cc'").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 1
    assert "cc_table" in res["table"]


@pytest.mark.skip("FROM <catalog>.<namespace> not supported in sqlparser-rs")
def test_show_tables_in_catalog_namespace(sess):
    # test: show tables in catalog `cat2` and namespace `my_namespace`
    result = sess.sql("SHOW TABLES FROM cat2.my_namespace")
    tables = result.to_pydict()["table"]
    assert len(tables) == 2  # Only tables in my_namespace
    assert "foo_table" in tables
    assert "bar_table" in tables


@pytest.mark.skip("FROM <catalog>.<namespace> not supported in sqlparser-rs")
def test_show_tables_in_catalog_namespace_with_pattern(sess):
    # test: show tables in catalog `cat2` and namespace `my_namespace` matching the pattern
    result = sess.sql("SHOW TABLES FROM cat2.my_namespace LIKE 'foo'")
    tables = result.to_pydict()["table"]
    assert len(tables) == 1
    assert "foo_table" in tables
    assert "bar_table" not in tables
