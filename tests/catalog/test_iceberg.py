from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import Catalog, Session
from daft.catalog import NotFoundError
from daft.logical.schema import DataType as dt
from daft.logical.schema import Field, Schema

CATALOG_ALIAS = "_test_catalog_iceberg"

# skip if pyarrow < 9
pyiceberg = pytest.importorskip("pyiceberg")
PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="iceberg not supported on old versions of pyarrow")


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

    # existence checks
    assert c.has_namespace(n)
    assert not c.has_namespace("x")

    #
    # err! should not exist
    with pytest.raises(Exception, match="does not exist"):
        c.list_namespaces("x")
    #
    # cleanup
    c.drop_namespace(n)


def test_create_namespace_if_exists(catalog: Catalog):
    c = catalog
    n = "test_create_namespace_if_exists"

    # test when namespace doesn't exist
    assert not c.has_namespace(n)
    c.create_namespace_if_not_exists(n)
    assert c.has_namespace(n)

    # should not raise an exception
    c.create_namespace_if_not_exists(n)
    assert c.has_namespace(n)

    # cleanup
    c.drop_namespace(n)


def test_create_table(catalog: Catalog):
    c = catalog
    n = "test_create_table"
    c.create_namespace(n)

    # create table with daft schema
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

    # test get_table
    assert c.get_table(f"{n}.tbl1")
    assert len(c.list_tables(n)) == 1

    # test has_table
    assert c.has_table(f"{n}.tbl1")
    assert not c.has_table(f"{n}.does_not_exist")

    # test exception
    with pytest.raises(NotFoundError):
        c.get_table(f"{n}.does_not_exist")

    # cleanup
    c.drop_table(f"{n}.tbl1")
    c.drop_namespace(n)


def test_create_table_if_not_exists(catalog: Catalog):
    c = catalog
    n = "test_create_table_if_not_exists"
    c.create_namespace(n)

    assert not c.has_table(f"{n}.tbl1")
    c.create_table_if_not_exists(
        f"{n}.tbl1",
        schema(
            {
                "a": dt.bool(),
                "b": dt.int64(),
                "c": dt.string(),
            }
        ),
    )
    assert c.has_table(f"{n}.tbl1")

    # should not raise an exception
    c.create_table_if_not_exists(
        f"{n}.tbl1",
        schema(
            {
                "a": dt.bool(),
                "b": dt.int64(),
                "c": dt.string(),
            }
        ),
    )
    assert c.has_table(f"{n}.tbl1")

    # Cleanup
    c.drop_table(f"{n}.tbl1")
    c.drop_namespace(n)


def test_create_table_as_select(catalog: Catalog):
    cat = catalog
    ns = "test_create_table_as_select"
    table_name = f"{ns}.tbl"
    df = daft.from_pydict({"a": [True, True, False], "b": [1, 2, 3], "c": ["x", "y", "z"]})

    # create_table with dataframe
    cat.create_namespace(ns)
    cat.create_table(table_name, df)

    # test get_table
    tbl = cat.get_table(table_name)
    assert tbl is not None
    assert len(cat.list_tables(ns)) == 1

    # validate the data
    assert tbl.read().to_pydict() == df.to_pydict()

    # cleanup
    cat.drop_table(table_name)
    cat.drop_namespace(ns)


def test_list_tables(catalog: Catalog, sess: Session):
    c = catalog

    # create a few namespaces
    n1 = "test_list_tables_a"
    n2 = "test_list_tables_b"
    n3 = "test_list_tables_c"
    c.create_namespace(n1)
    c.create_namespace(n2)
    c.create_namespace(n3)

    # no tables in any namespace
    assert len(c.list_tables()) == 1  # default
    assert len(c.list_tables(n1)) == 0
    assert len(c.list_tables(n2)) == 0
    assert len(c.list_tables(n3)) == 0

    # create a few tables in each namespace
    c.create_table(f"{n1}.tbl1_1", schema({"a": dt.bool()}))

    c.create_table(f"{n2}.tbl2_1", schema({"a": dt.bool()}))
    c.create_table(f"{n2}.tbl2_2", schema({"a": dt.bool()}))

    c.create_table(f"{n3}.tbl3_1", schema({"a": dt.bool()}))
    c.create_table(f"{n3}.tbl3_2", schema({"a": dt.bool()}))
    c.create_table(f"{n3}.tbl3_3", schema({"a": dt.bool()}))

    # list all tables
    assert len(c.list_tables()) == 7  # default + 6
    assert len(c.list_tables(n1)) == 1
    assert len(c.list_tables(n2)) == 2
    assert len(c.list_tables(n3)) == 3

    # show tables
    res = sess.sql("SHOW TABLES").to_pydict()
    assert len(res["table"]) == 7
    assert "tbl1_1" in res["table"]
    assert "tbl2_1" in res["table"]
    assert "tbl2_2" in res["table"]
    assert "tbl3_1" in res["table"]
    assert "tbl3_2" in res["table"]
    assert "tbl3_3" in res["table"]

    # # cleanup
    c.drop_table(f"{n1}.tbl1_1")
    c.drop_table(f"{n2}.tbl2_1")
    c.drop_table(f"{n2}.tbl2_2")
    c.drop_table(f"{n3}.tbl3_1")
    c.drop_table(f"{n3}.tbl3_2")
    c.drop_table(f"{n3}.tbl3_3")

    c.drop_namespace(n3)
    c.drop_namespace(n2)
    c.drop_namespace(n1)


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
