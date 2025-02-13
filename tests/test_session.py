from __future__ import annotations

import pytest

import daft
from daft import Session

"""
SESSION TESTS
"""

def test_current_session_exists():
    assert daft.current_session() is not None

"""
CATALOG TESTS
"""

@pytest.mark.skip
def test_catalog_actions():
    sess = Session.empty()
    #
    # create_catalog (ok)
    sess.create_catalog("cat1")
    sess.create_catalog("cat2")
    #
    # list_catalogs
    assert 2 == len(sess.list_catalogs())
    # assert 2 == len(sess.list_catalogs("cat"))
    # assert 0 == len(sess.list_catalogs("xxx"))
    #
    # set_catalog (ok)
    sess.set_catalog("cat1")
    assert "cat1" == sess.current_catalog().name()
    sess.set_catalog("cat2")
    assert "cat2" == sess.current_catalog().name()
    #
    # set_catalog (err)
    with pytest.raises(Exception, match="does not exist"):
        sess.set_schema("cat3")

"""
SCHEMA TESTS
"""

@pytest.mark.skip
def test_schema_actions():
    sess = Session.empty()
    #
    # create_schema (err)
    with pytest.raises(Exception, match="no current_catalog"):
        sess.create_schema("s1")
    #
    # create_schema (ok)
    sess.create_catalog("c1")
    assert sess.create_schema("s1") is not None
    assert sess.create_schema("s2") is not None
    #
    # list_schemas
    assert 2 == len(sess.list_schemas())
    # assert 2 == len(sess.list_schemas("s"))
    # assert 0 == len(sess.list_schemas("xxx"))
    #
    # use_schema (ok)
    sess.set_schema("s1")
    assert "s1" == sess.current_schema().name()
    sess.set_schema("s2")
    assert "s2" == sess.current_schema().name()
    #
    # use_schema (err)
    with pytest.raises(Exception, match="does not exist"):
        sess.set_schema("s3")

@pytest.mark.skip
def test_schema_actions_global():
    sess = denv
    #
    # default schema exists
    assert sess.current_schema() is not None
    #
    # create_schema (ok)
    sess.create_catalog("c1")
    assert sess.create_schema("s1") is not None
    assert sess.create_schema("s2") is not None
    #
    # list_schemas + default!
    assert 3 == len(sess.list_schemas())
    # assert 2 == len(sess.list_schemas("s"))
    # assert 0 == len(sess.list_schemas("xxx"))
    #
    # use_schema (ok)
    sess.set_schema("s1")
    assert "s1" == sess.current_schema().name()
    sess.set_schema("s2")
    assert "s2" == sess.current_schema().name()
    #
    # use_schema (err)
    with pytest.raises(Exception, match="does not exist"):
        sess.set_schema("s3")

"""
TABLE TESTS
"""

@pytest.mark.skip
def test_table_actions():
    sess = Session.default()
    #
    # create_table
    t1 = sess.create_table("t1")
    t2 = sess.create_table("t2")
    assert t1 is not None
    assert t2 is not None
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    # assert 2 == len(sess.list_tables("t"))
    # assert 1 == len(sess.list_tables("1"))
    # assert 0 == len(sess.list_tables("xxx"))
    #
    # get_table
    assert t1 == sess.get_table("t1")
    assert t2 == sess.get_table("t2")

"""
PATH TESTS
"""

@pytest.mark.skip
def test_sess_path():
    raise NotImplementedError

"""
ATTACH & DETACH TESTS
"""

def test_attach():
    sess = Session.empty()
    #
    # create some 'existing' catalogs
    cat1 = daft.load_catalog("cat1")
    cat2 = daft.load_catalog("cat2")
    #
    # attach them..
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # list_catalogs
    assert 2 == len(sess.list_catalogs())
    #
    # get_catalog
    assert sess.get_catalog("cat1") == cat1
    assert sess.get_catalog("cat2") == cat2


# test attach
# test attach existing
# test detach
# test detach non-existing
