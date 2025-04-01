import pytest

from daft import Catalog, Session


@pytest.fixture
def cat1():
    return Catalog.from_pydict(
        name="cat1",
        tables={
            "aa_table": {"x": [1]},
            "bb_table": {"y": [2]},
        },
    )


@pytest.fixture
def cat2():
    return Catalog.from_pydict(
        name="cat2",
        tables={
            "cc_table": {"x": [1]},
            "dd_table": {"y": [2]},
        },
    )


@pytest.fixture
def cat3():
    return Catalog.from_pydict(
        name="cat3",
        tables={
            "aa_table": {"x": [1]},
            "bb_table": {"y": [2]},
            "cc_table": {"x": [1]},
            "dd_table": {"y": [2]},
            "xx.ee_table": {"x": [1]},
            "xx.ff_table": {"y": [2]},
            "xx.gg_table": {"x": [1]},
            "xx.hh_table": {"y": [2]},
            "yy.ii_table": {"x": [1]},
            "yy.jj_table": {"y": [2]},
            "yy.kk_table": {"x": [1]},
            "yy.ll_table": {"y": [2]},
        },
    )


@pytest.fixture(scope="function")
def sess(cat1, cat2, cat3):
    sess = Session()
    sess.attach(cat1)
    sess.attach(cat2)
    sess.attach(cat3)
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


def test_show_tables_with_use_catalog(sess):
    # test: show tables in catalog `cat2` after doing `USE cat2` with no pattern.
    sess.use("cat2")
    res = sess.sql("SHOW TABLES").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 2
    assert "cc_table" in res["table"]
    assert "dd_table" in res["table"]


def test_show_tables_with_use_catalog_with_pattern(sess):
    # test: show tables in catalog `cat2` after doing `USE cat2` with pattern.
    sess.use("cat2")
    res = sess.sql("SHOW TABLES LIKE 'cc'").to_pydict()
    assert "cat2" in res["catalog"]
    assert len(res["table"]) == 1
    assert "cc_table" in res["table"]
    assert "dd_table" not in res["table"]


def test_show_tables_with_use_catalog_namespace(sess):
    # test: show tables in catalog `cat.ns` after doing `USE cat.xx` with no pattern.
    sess.use("cat3.xx")
    res = sess.sql("SHOW TABLES").to_pydict()
    assert "cat3" in res["catalog"]
    assert len(res["table"]) == 4
    assert "ee_table" in res["table"]
    assert "ff_table" in res["table"]
    assert "gg_table" in res["table"]
    assert "hh_table" in res["table"]


def test_show_tables_with_use_catalog_namespace_with_pattern(sess):
    # test: show tables in catalog `cat.ns` after doing `USE cat.xx` with pattern.
    sess.use("cat3.xx")
    res = sess.sql("SHOW TABLES LIKE 'e'").to_pydict()
    assert "cat3" in res["catalog"]
    assert len(res["table"]) == 1
    assert "ee_table" in res["table"]
    print(res)
