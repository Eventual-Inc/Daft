from __future__ import annotations

import pytest

import daft
from daft.catalog import Catalog, Identifier, NotFoundError, Table
from daft.session import Session

###
# SESSION SETUP
###


def test_current_session_exists():
    assert daft.current_session() is not None


###
# ATTACH & DETACH CATALOG
###


def test_attach_catalog():
    sess = Session()
    #
    # create some 'existing' catalogs
    cat1 = Catalog.from_pydict({})
    cat2 = Catalog.from_pydict({})
    #
    # attach them..
    sess.attach_catalog(cat1, alias="cat1")
    sess.attach_catalog(cat2, alias="cat2")
    #
    # list_catalogs
    assert 2 == len(sess.list_catalogs())
    #
    # get_catalog
    assert sess.get_catalog("cat1") == cat1
    assert sess.get_catalog("cat2") == cat2
    #
    # error!
    with pytest.raises(Exception, match="already exists"):
        sess.attach_catalog(cat1, alias="cat1")


def test_detach_catalog():
    sess = Session()
    #
    # setup.
    cat1 = Catalog.from_pydict({})
    cat2 = Catalog.from_pydict({})
    sess.attach_catalog(cat1, alias="cat1")
    sess.attach_catalog(cat2, alias="cat2")
    #
    #
    assert 2 == len(sess.list_catalogs())
    #
    # detach existing
    sess.detach_catalog("cat1")
    assert 1 == len(sess.list_catalogs())
    #
    # error!
    with pytest.raises(Exception, match="not found"):
        sess.detach_catalog("cat1")


###
# ATTACH & DETACH TABLE
###


def test_attach_table():
    sess = Session()
    #
    # create some tables
    view1 = Table.from_df("view1", daft.from_pydict({"x": [1.1, 2.2, 3.3]}))
    view2 = Table.from_df("view2", daft.from_pydict({"y": ["a", "b", "c"]}))
    #
    # attach them..
    sess.attach_table(view1, alias="tbl1")
    sess.attach_table(view2, alias="tbl2")
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    #
    # get_table exact object (proper unwrapping)
    assert sess.get_table("tbl1") == view1
    assert sess.get_table("tbl2") == view2
    # read_table
    assert sess.read_table("tbl1").to_pydict() == view1.read().to_pydict()
    assert sess.read_table("tbl2").to_pydict() == view2.read().to_pydict()
    #
    # error!
    with pytest.raises(Exception, match="already exists"):
        sess.attach_table(view1, alias="tbl1")


def test_detach_table():
    sess = Session()
    #
    # setup.
    view1 = Table.from_df("view1", daft.from_pydict({"x": [1.1, 2.2, 3.3]}))
    sess.attach_table(view1, alias="tbl1")
    #
    # check is attached
    assert 1 == len(sess.list_tables())
    #
    # detach existing
    sess.detach_table("tbl1")
    assert 0 == len(sess.list_tables())
    #
    # error!
    with pytest.raises(Exception, match="not found"):
        sess.detach_table("tbl1")


###
# CREATE TABLE
###


def test_create_temp_table():
    sess = Session()
    #
    # create some dataframes
    df1 = daft.from_pydict({"x": [1.1, 2.2, 3.3]})
    df2 = daft.from_pydict({"y": ["a", "b", "c"]})
    #
    # create temp tables from these views
    sess.create_temp_table("tbl1", df1)
    sess.create_temp_table("tbl2", df2)
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    #
    # read_table
    assert sess.read_table("tbl1").to_pydict() == df1.to_pydict()
    assert sess.read_table("tbl2").to_pydict() == df2.to_pydict()
    #
    # replace tbl1 with df2
    sess.create_temp_table("tbl1", df2)
    assert sess.read_table("tbl1").to_pydict() == df2.to_pydict()


###
# USE / SET [CATALOG|SCHEMA]
###


def test_use():
    sess = Session()
    #
    # create some catalogs to use
    cat1 = Catalog.from_pydict(name="cat1", tables={})
    cat2 = Catalog.from_pydict(name="cat2", tables={})
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # current catalog defaults to the first attached
    assert sess.current_catalog() == cat1
    #
    # set and assert
    sess.set_catalog("cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    # set a namespace
    sess.set_namespace("a.b")
    assert sess.current_namespace() == Identifier("a", "b")
    #
    sess.set_catalog(None)
    sess.set_namespace(None)
    assert sess.current_catalog() is None
    assert sess.current_namespace() is None
    #
    # test use <catalog>
    sess.use("cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    #  test use <catalog>.<namespace>
    sess.use("cat2.a.b")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() == Identifier("a", "b")


def test_sql():
    sess = Session()
    #
    # create some catalogs to use
    cat1 = Catalog.from_pydict(name="cat1", tables={})
    cat2 = Catalog.from_pydict(name="cat2", tables={})
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # test use <catalog>
    sess.sql("USE cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    #  test use <catalog>.<namespace>
    sess.sql("USE cat2.a.b")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() == Identifier("a", "b")


###
# exception testing
###


def test_exception_surfacing():
    class ThrowingCatalog(Catalog):
        @property
        def name(self):
            return "throwing"

        def _create_namespace(self, identifier):
            raise NotImplementedError

        def _create_table(self, identifier, source):
            raise NotImplementedError

        def _drop_namespace(self, identifier):
            raise NotImplementedError

        def _drop_table(self, identifier):
            raise NotImplementedError

        def _get_table(self, identifier):
            if str(identifier) == "boom":
                raise RuntimeError("something went wrong")
            raise NotFoundError(f"Table {identifier} not found")

        def _list_namespaces(self, pattern=None):
            raise NotImplementedError

        def _list_tables(self, pattern=None):
            raise NotImplementedError

        def _has_namespace(self, ident):
            raise NotImplementedError

        def _has_table(self, ident):
            if str(ident) == "boom":
                raise RuntimeError("something went wrong")
            raise NotFoundError(f"Table {ident} not found")

    sess = Session()
    sess.attach(ThrowingCatalog())

    # the session should
    with pytest.raises(Exception, match="not found"):
        sess.read_table("test")

    # some internal error should be surfaced in the runtime exception
    with pytest.raises(Exception, match="something went wrong"):
        sess.read_table("boom")
