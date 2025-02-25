from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import Session

CATALOG_ALIAS = "_test_catalog_iceberg"


@pytest.fixture()
def iceberg_catalog(tmpdir):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        CATALOG_ALIAS,
        **{
            "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    #
    # define a table via iceberg
    catalog.create_namespace("default")
    catalog.create_table("default.tbl", pa.schema([("x", pa.bool_()), ("y", pa.int64()), ("z", pa.string())]))
    return catalog


@pytest.fixture()
def global_sess(iceberg_catalog):
    daft.attach_catalog(iceberg_catalog, alias=CATALOG_ALIAS)
    yield daft.current_session()
    daft.detach_catalog(alias=CATALOG_ALIAS)


@pytest.fixture()
def sess(iceberg_catalog):
    sess = Session()
    sess.attach_catalog(iceberg_catalog, alias=CATALOG_ALIAS)
    yield sess
    sess.detach_catalog(alias=CATALOG_ALIAS)


def test_daft_read_table(global_sess: Session):
    assert global_sess.read_table(f"{CATALOG_ALIAS}.default.tbl") is not None
    assert daft.read_table(f"{CATALOG_ALIAS}.default.tbl") is not None


def test_sess_read_table(sess: Session):
    # catalog-qualified
    assert sess.read_table(f"{CATALOG_ALIAS}.default.tbl") is not None
    # schema-qualified
    assert sess.read_table("default.tbl") is not None
    # unqualified
    sess.set_namespace("default")
    assert sess.read_table("tbl") is not None
