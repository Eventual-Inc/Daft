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
# ATTACH & DETACH
###


def test_attach():
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
        sess.attach(cat1, alias="cat1")


def test_detach():
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
