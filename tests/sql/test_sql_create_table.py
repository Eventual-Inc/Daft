from __future__ import annotations

import pytest

from daft import Catalog, Session


@pytest.fixture()
def sess() -> Session:
    """Create a session with a memory catalog attached."""
    sess = Session()
    cat = Catalog.from_pydict({}, name="mem")
    sess.attach_catalog(cat, alias="mem")
    # Pre-create the "default" namespace.
    sess.set_catalog("mem")
    sess.create_namespace("default")
    return sess


def test_create_table_catalog_qualified(sess: Session):
    """CREATE TABLE with catalog-qualified identifier."""
    assert sess.sql("CREATE TABLE mem.default.tbl (a int, b string)") is None
    res = sess.sql("SELECT * FROM mem.default.tbl").collect()
    assert res.column_names == ["a", "b"]


def test_create_table_schema_qualified(sess: Session):
    """CREATE TABLE with schema-qualified identifier using current catalog."""
    sess.set_catalog("mem")
    assert sess.sql("CREATE TABLE default.tbl (x int64, y float64)") is None
    res = sess.sql("SELECT * FROM mem.default.tbl").collect()
    assert res.column_names == ["x", "y"]


def test_create_table_unqualified(sess: Session):
    """CREATE TABLE with unqualified identifier using current catalog and namespace."""
    sess.set_catalog("mem")
    sess.set_namespace("default")
    assert sess.sql("CREATE TABLE tbl (id int)") is None
    res = sess.sql("SELECT * FROM mem.default.tbl").collect()
    assert res.column_names == ["id"]


def test_create_table_if_not_exists(sess: Session):
    """CREATE TABLE IF NOT EXISTS should be idempotent."""
    assert sess.sql("CREATE TABLE IF NOT EXISTS mem.default.dup (a int)") is None
    # Second call should not error.
    assert sess.sql("CREATE TABLE IF NOT EXISTS mem.default.dup (a int)") is None
    res = sess.sql("SELECT * FROM mem.default.dup").collect()
    assert res.column_names == ["a"]


def test_create_table_if_not_exists_preserves_original(sess: Session):
    """IF NOT EXISTS should not overwrite the existing table."""
    assert sess.sql("CREATE TABLE IF NOT EXISTS mem.default.tbl (a int)") is None
    # Attempt with different schema -- should be ignored.
    assert sess.sql("CREATE TABLE IF NOT EXISTS mem.default.tbl (x string, y string)") is None
    res = sess.sql("SELECT * FROM mem.default.tbl").collect()
    assert res.column_names == ["a"]


def test_create_table_no_catalog_errors():
    """CREATE TABLE without a catalog-qualified name and no current catalog should error."""
    sess = Session()
    with pytest.raises(Exception, match="without a current catalog|catalog"):
        sess.sql("CREATE TABLE tbl (a int)").collect()


def test_create_table_show_tables(sess: Session):
    """Table created via SQL should appear in SHOW TABLES."""
    sess.set_catalog("mem")
    assert sess.sql("CREATE TABLE default.show_test (a int)") is None
    res = sess.sql("SHOW TABLES IN mem.default").to_pydict()
    assert "show_test" in res["table"]


def test_create_table_multiple_columns(sess: Session):
    """CREATE TABLE with multiple columns of different types."""
    assert sess.sql("CREATE TABLE mem.default.multi (  id int64,   name string,   value float64,   flag bool)") is None
    res = sess.sql("SELECT * FROM mem.default.multi").collect()
    assert res.column_names == ["id", "name", "value", "flag"]


def test_create_table_empty_columns(sess: Session):
    """CREATE TABLE with no columns should succeed (empty schema)."""
    assert sess.sql("CREATE TABLE mem.default.empty ()") is None
    res = sess.sql("SELECT * FROM mem.default.empty").collect()
    assert res.column_names == []


def test_create_table_duplicate_columns(sess: Session):
    """CREATE TABLE with duplicate column names should be rejected."""
    with pytest.raises(Exception, match="Duplicate column name"):
        sess.sql("CREATE TABLE mem.default.bad (a int, a string)").collect()
