import pytest

import daft
from daft.catalog import Catalog
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
    sess = Session.empty()
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
    sess = Session.empty()
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
    sess = Session.empty()
    #
    # create some tables
    df1 = daft.from_pydict({"x": [1.1, 2.2, 3.3]})
    df2 = daft.from_pydict({"y": ["a", "b", "c"]})
    #
    # attach them..
    sess.attach_table(df1, alias="tbl1")
    sess.attach_table(df2, alias="tbl2")
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    #
    # read_table
    assert sess.read_table("tbl1").to_pydict() == df1.to_pydict()
    assert sess.read_table("tbl2").to_pydict() == df2.to_pydict()
    #
    # error!
    with pytest.raises(Exception, match="already exists"):
        sess.attach_table(df1, alias="tbl1")


def test_detach_table():
    sess = Session.empty()
    #
    # setup.
    tbl1 = daft.from_pydict({})
    tbl2 = daft.from_pydict({})
    sess.attach_table(tbl1, alias="tbl1")
    sess.attach_table(tbl2, alias="tbl2")
    #
    #
    assert 2 == len(sess.list_tables())
    #
    # detach existing
    sess.detach_table("tbl1")
    assert 1 == len(sess.list_tables())
    #
    # error!
    with pytest.raises(Exception, match="not found"):
        sess.detach_table("tbl1")
