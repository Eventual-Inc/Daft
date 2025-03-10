from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import Catalog, Session
from daft.logical.schema import DataType as dt
from daft.logical.schema import Field, Schema

CATALOG_ALIAS = "_test_catalog_iceberg"


@pytest.fixture(scope="session")
def iceberg_catalog(tmp_path_factory):
    from pyiceberg.catalog.sql import SqlCatalog

    tmpdir = tmp_path_factory.mktemp("test_iceberg")
    catalog = SqlCatalog(
        CATALOG_ALIAS,
        **{
            "uri": f"sqlite:///{tmpdir}/iceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    #
    # define a table via iceberg
    catalog.create_namespace("default")
    catalog.create_table(
        "default.tbl",
        pa.schema([("x", pa.bool_()), ("y", pa.int64()), ("z", pa.string())]),
    )
    return catalog


@pytest.fixture(scope="session")
def global_sess(iceberg_catalog):
    daft.attach_catalog(iceberg_catalog, alias=CATALOG_ALIAS)
    yield daft.current_session()
    daft.detach_catalog(alias=CATALOG_ALIAS)


@pytest.fixture(scope="session")
def sess(iceberg_catalog):
    sess = Session()
    sess.attach_catalog(iceberg_catalog, alias=CATALOG_ALIAS)
    yield sess
    sess.detach_catalog(alias=CATALOG_ALIAS)


@pytest.fixture(scope="session")
def catalog(iceberg_catalog):
    return Catalog.from_iceberg(iceberg_catalog)


def schema(fields: dict[str, dt]) -> Schema:
    return Schema._from_fields([Field.create(k, v) for k, v in fields.items()])


def assert_eq(df1, df2):
    assert df1.to_pydict() == df2.to_pydict()


def test_name(catalog):
    assert catalog.name == CATALOG_ALIAS
    assert catalog.get_table("default.tbl").name == "tbl"
    assert catalog.get_table("default.tbl").__repr__() == "Table('tbl')"


###
# ddl tests
###


def test_create_namespace(catalog: Catalog):
    c = catalog
    n = "test_create_namespace"
    #
    c.create_namespace(f"{n}")
    c.create_namespace(f"{n}.a")
    c.create_namespace(f"{n}.a.b")
    c.create_namespace(f"{n}.b")
    #
    # bug? iceberg sql catalog does not include child namespace
    # assert len(c.list_namespaces(f"{n}")) == 3
    assert len(c.list_namespaces(f"{n}.a")) == 1
    assert len(c.list_namespaces(f"{n}.b")) == 1
    #
    # err! should not exist
    with pytest.raises(Exception, match="does not exist"):
        c.list_namespaces("x")
    #
    # cleanup
    c.drop_namespace(n)


def test_create_table(catalog: Catalog):
    c = catalog
    n = "test_create_table"
    c.create_namespace(n)
    #
    c.create_table(
        f"{n}.tbl1",
        schema(
            {
                "a": dt.bool(),
                "b": dt.int64(),
                "c": dt.string(),
            }
        ),
    )
    # TODO create table as source (CTAS)
    # c.create_table(
    #     f"{n}.tbl2",
    #     daft.from_pydict({"a": [True, True, False], "b": [1, 2, 3], "c": ["x", "y", "z"]}),
    # )
    #
    assert c.get_table(f"{n}.tbl1")
    # assert c.get_table(f"{n}.tbl2")
    assert len(c.list_tables(n)) == 1
    #
    # cleanup
    c.drop_table(f"{n}.tbl1")
    # c.drop_table(f"{n}.tbl2")
    c.drop_namespace(n)


###
# read tests
###


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


def test_sess_read_options(sess: Session):
    # invalid options (version is a unity option, not iceberg)
    with pytest.raises(ValueError, match="Unsupported option"):
        sess.read_table("tbl", version="1")
