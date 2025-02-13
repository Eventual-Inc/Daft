import daft
from daft.session import Session

import pytest

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
    #
    # error!
    with pytest.raises(Exception, match="already exists"):
        sess.attach(cat1)

def test_detach():
    sess = Session.empty()
    #
    # setup.
    cat1 = daft.load_catalog("cat1")
    cat2 = daft.load_catalog("cat2")
    sess.attach(cat1)
    sess.attach(cat2)
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
    cat1 = daft.load_catalog("cat1")
    cat2 = daft.load_catalog("cat2")
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # current_catalog should default to first in.
    assert cat1 == sess.current_catalog()
    #
    # set_catalog and current_catalog
    sess.set_catalog("cat2")
    assert cat2 == sess.current_catalog()
