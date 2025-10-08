from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft import Catalog, DataFrame, Session


def assert_eq(actual: DataFrame, expect: DataFrame):
    assert actual.to_pydict() == expect.to_pydict()


def _create_catalog(name: str, tmpdir: str):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        name,
        **{
            "uri": f"sqlite:///{tmpdir}/pytest_sql_{name}.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    # using naming convention "tbl_<catalog>_<namespace #><table #>"
    # which let's us know where the table is coming from with only its name
    catalog.create_namespace("ns_1")
    catalog.create_namespace("ns_2")
    catalog.create_table(f"ns_1.tbl_{name}_11", pa.schema([]))
    catalog.create_table(f"ns_1.tbl_{name}_12", pa.schema([]))
    catalog.create_table(f"ns_2.tbl_{name}_21", pa.schema([]))
    catalog.create_table(f"ns_2.tbl_{name}_22", pa.schema([]))
    return Catalog.from_iceberg(catalog)


@pytest.fixture()
def sess(tmpdir) -> Session:
    # create some tmp catalogs
    cat_1 = _create_catalog("cat_1", tmpdir)
    cat_2 = _create_catalog("cat_2", tmpdir)
    # attach to a new session
    sess = Session()
    sess.attach_catalog(cat_1, alias="cat_1")
    sess.attach_catalog(cat_2, alias="cat_2")
    return sess


# chore: consider reducing test verbosity
# def try_resolve(sess: Session, ident) -> DataFrame:
#     sess.sql(f"select * from {ident}")

# chore: consider reducing test verbosity
# def assert_resolve(sess: Session, ident: str):
#     assert try_resolve(ident) is not None


def test_catatalog_qualified_idents(sess: Session):
    # catalog-qualified
    assert sess.sql("select * from cat_1.ns_1.tbl_cat_1_11") is not None
    assert sess.sql("select * from cat_2.ns_1.tbl_cat_2_11") is not None


def test_schema_qualified_idents(sess: Session):
    #
    # schema-qualified should work for cat_1
    sess.set_catalog("cat_1")
    assert sess.sql("select * from ns_1.tbl_cat_1_11") is not None
    assert sess.sql("select * from ns_2.tbl_cat_1_21") is not None
    #
    # catalog-qualified should still work.
    assert sess.sql("select * from cat_1.ns_1.tbl_cat_1_11") is not None
    assert sess.sql("select * from cat_1.ns_2.tbl_cat_1_21") is not None
    #
    # err! should not find things from cat_2
    with pytest.raises(Exception, match="not found"):
        sess.sql("select * from ns_1.tbl_cat_2_11")
    with pytest.raises(Exception, match="not found"):
        sess.sql("select * from ns_2.tbl_cat_2_21")
    #
    # find in cat_2 only if catalog-qualified
    assert sess.sql("select * from cat_2.ns_1.tbl_cat_2_11") is not None
    assert sess.sql("select * from cat_2.ns_2.tbl_cat_2_21") is not None


def test_unqualified_idents(sess: Session):
    #
    # unqualified should only work in cat_1 and ns_1
    sess.set_catalog("cat_1")
    sess.set_namespace("ns_1")
    assert sess.sql("select * from tbl_cat_1_11") is not None
    assert sess.sql("select * from tbl_cat_1_12") is not None
    #
    # schema-qualified and  should still work.
    assert sess.sql("select * from ns_1.tbl_cat_1_11") is not None
    assert sess.sql("select * from ns_2.tbl_cat_1_21") is not None
    #
    # catalog-qualified should still work.
    assert sess.sql("select * from cat_1.ns_1.tbl_cat_1_11") is not None
    assert sess.sql("select * from cat_1.ns_2.tbl_cat_1_21") is not None
    #
    # err! should not find unqualified things from ns_2
    with pytest.raises(Exception, match="not found"):
        sess.sql("select * from tbl_cat_1_21")
    with pytest.raises(Exception, match="not found"):
        sess.sql("select * from tbl_cat_1_22")
    #
    # find in cat_1.ns_2 only if schema-qualified
    assert sess.sql("select * from ns_2.tbl_cat_1_21") is not None
    assert sess.sql("select * from ns_2.tbl_cat_1_22") is not None
    #
    # find in cat_2 only if catalog-qualified
    assert sess.sql("select * from cat_2.ns_1.tbl_cat_2_11") is not None
    assert sess.sql("select * from cat_2.ns_2.tbl_cat_2_21") is not None


# name resolution sanity check
def test_pydict_catalog():
    sess = Session()
    sess.attach(
        Catalog.from_pydict(
            {
                "T": {
                    "a": [1, 3, 5],
                    "b": [2, 4, 6],
                },
                "S": daft.from_pylist(
                    [
                        {
                            "a": 1,
                            "b": 2,
                        },
                        {
                            "a": 3,
                            "b": 4,
                        },
                        {
                            "a": 5,
                            "b": 6,
                        },
                    ]
                ),
            }
        )
    )
    table_t = sess.sql("SELECT * FROM T")
    table_s = sess.read_table("S")
    assert_eq(table_t, table_s)
