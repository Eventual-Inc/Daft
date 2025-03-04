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
    c.create_table(
        f"{n}.tbl2",
        daft.from_pydict({"a": [True, True, False], "b": [1, 2, 3], "c": ["x", "y", "z"]}),
    )
    #
    assert c.get_table(f"{n}.tbl1")
    assert c.get_table(f"{n}.tbl2")
    assert len(c.list_tables(n)) == 2
    #
    # cleanup
    c.drop_table(f"{n}.tbl1")
    c.drop_table(f"{n}.tbl2")
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


###
# write tests
###


def test_write_table(catalog: Catalog):
    c = catalog
    n = "test_write_table"
    t1 = f"{n}.tbl1"
    t2 = f"{n}.tbl2"
    c.create_namespace(n)
    c.create_table(t1, schema({"v": dt.int64()}))
    c.create_table(t2, schema({"v": dt.int64()}))
    #
    part1 = daft.from_pydict({"v": [1, 2, 3]})
    part2 = daft.from_pydict({"v": [4, 5, 6]})
    parts = daft.from_pydict({"v": [4, 5, 6, 1, 2, 3]})
    #
    # append parts to t1 using catalog.write_table helper
    c.write_table(t1, part1)
    assert_eq(c.read_table(t1), part1)
    c.write_table(t1, part2)
    assert_eq(c.read_table(t1), parts)
    #
    # append parts to t2 using table methods
    tbl2 = c.get_table(t2)
    tbl2.append(part1)
    assert_eq(tbl2.read(), part1)
    tbl2.append(part2)
    assert_eq(tbl2.read(), parts)
    #
    # assert overwrite
    #
    part3 = daft.from_pydict({"v": [0]})
    #
    # overwrite t1 using catalog.write with mode
    c.write_table(t1, part3, mode="overwrite")
    assert_eq(c.read_table(t1), part3)
    #
    # overwrite t2 using table methods
    tbl2.overwrite(part3)
    assert_eq(tbl2.read(), part3)
