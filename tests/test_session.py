import daft

import pytest

import daft
from daft.logical.schema import Schema, Field
from daft import Catalog, Session

"""
SESSION SETUP
"""

def test_current_session_exists():
    assert daft.current_session() is not None

"""
ATTACH & DETACH
"""

def test_attach():
    sess = Session.empty()
    #
    # create some 'existing' catalogs
    cat1 = Catalog.empty()
    cat2 = Catalog.empty()
    #
    # attach them..
    sess.attach(cat1, alias="cat1")
    sess.attach(cat2, alias="cat2")
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
        sess.attach(cat1, alias="cat1")

def test_detach():
    sess = Session.empty()
    #
    # setup.
    cat1 = Catalog.empty()
    cat2 = Catalog.empty()
    sess.attach(cat1, alias="cat1")
    sess.attach(cat2, alias="cat2")
    #
    # 
    assert 2 == len(sess.list_catalogs())
    #
    # detach existing
    sess.detach("cat1")
    assert 1 == len(sess.list_catalogs())
    #
    # error!
    with pytest.raises(Exception, match="not found"):
        sess.detach("cat1")

"""
CATALOG ACTIONS
"""

@pytest.mark.skip
def test_catalog_actions():
    sess = Session.empty()
    #
    # setup.
    cat1 = Catalog.empty()
    cat2 = Catalog.empty()
    sess.attach(cat1, alias="cat1")
    sess.attach(cat2, alias="cat2")
    #
    # current_catalog should default to first in.
    assert cat1 == sess.current_catalog()
    #
    # set_catalog and current_catalog
    sess.set_catalog("cat2")
    assert cat2 == sess.current_catalog()

"""
TABLE ACTIONS
"""

def schema(**columns):
    fields = [Field.create(name, dtype) for name, dtype in columns.items()]
    return Schema._from_fields(fields)

def test_table_actions():
    sess = Session.empty()
    #
    # create_temp_table
    t1 = sess.create_temp_table("t1")
    t2 = sess.create_temp_table("t2")
    t3 = sess.create_temp_table("t3", daft.from_pydict({}))
    assert t1 is not None
    assert t2 is not None
    assert t3 is not None
    #
    # get_table (todo equality since these are diff objects)
    assert sess.get_table("t1") is not None
    assert sess.get_table("t2") is not None
    #
    # err on table not found
    with pytest.raises(Exception, match="not found"):
        sess.get_table("t4")
    #
    # err on qualified identifier
    with pytest.raises(Exception, match="not yet supported"):
        sess.get_table("default.t1")
