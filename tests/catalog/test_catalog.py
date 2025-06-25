from __future__ import annotations

from daft.catalog import Catalog, Identifier, Properties, Table
from daft.dataframe import DataFrame
from daft.logical.schema import DataType as dt
from daft.logical.schema import Schema


def assert_eq(df1, df2):
    assert df1.to_pydict() == df2.to_pydict()


def test_try_from_iceberg(tmpdir):
    from pyiceberg.catalog.sql import SqlCatalog

    pyiceberg_catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    # assert doesn't throw!
    assert Catalog._from_obj(pyiceberg_catalog) is not None
    assert Catalog.from_iceberg(pyiceberg_catalog) is not None


def test_try_from_unity():
    from daft.unity_catalog import UnityCatalog

    unity_catalog = UnityCatalog("-", token="-")
    # assert doesn't throw!
    assert Catalog._from_obj(unity_catalog) is not None
    assert Catalog.from_unity(unity_catalog) is not None


def test_from_pydict():
    import daft
    from daft.catalog import Catalog, Table
    from daft.session import Session

    dictionary = {"x": [1, 2, 3]}
    dataframe = daft.from_pydict(dictionary)
    table = Table.from_df("temp", dataframe)

    cat = Catalog.from_pydict(
        {
            "R": dictionary,
            "S": dataframe,
            "T": table,
        }
    )

    assert len(cat.list_tables()) == 3
    assert cat.get_table("T") is not None

    sess = Session()
    sess.attach_catalog(cat)
    sess.attach_table(table)

    assert_eq(sess.read_table("temp"), dataframe)
    assert_eq(sess.read_table("default.R"), dataframe)
    assert_eq(sess.read_table("default.S"), dataframe)
    assert_eq(sess.read_table("default.T"), dataframe)


def test_from_pydict_namespaced():
    from daft.session import Session

    # dummy data, we're testing namespaces here
    table = {"_": [0]}
    cat = Catalog.from_pydict(
        {
            "T0": table,
            "S0": table,
            "ns1.T1": table,
            "ns1.S1": table,
            "ns2.T2": table,
            "ns2.S2": table,
        }
    )

    assert len(cat.list_tables()) == 6
    assert len(cat.list_namespaces()) == 2
    assert cat.get_table("T0") is not None
    assert cat.get_table("ns1.T1") is not None
    assert cat.get_table("ns2.T2") is not None

    # session name resolution should still work
    sess = Session()
    sess.attach_catalog(cat)
    assert sess.get_table("default.T0") is not None
    assert sess.get_table("default.ns1.T1") is not None
    assert sess.get_table("default.ns2.T2") is not None


class MockCatalog(Catalog):
    """MockCatalog is an implementation designed to test implementing the ABC."""

    _tables: dict[str, MockTable]

    def __init__(self):
        self._tables = {}

    @property
    def name(self) -> str:
        return "test"

    def _create_namespace(self, identifier: Identifier):
        self.create_namespace_calls.append(identifier)
        self.namespaces.add(str(identifier))

    def _create_table(self, identifier: Identifier, schema: Schema, properties: Properties | None = None) -> Table:
        k = str(identifier)
        t = MockTable(k, properties)
        self._tables[k] = t
        return t

    def _drop_namespace(self, identifier: Identifier):
        raise NotImplementedError

    def _drop_table(self, identifier: Identifier):
        del self._tables[str(identifier)]

    def _get_table(self, identifier: Identifier) -> Table:
        return self._tables[str(identifier)]

    def _list_namespaces(self, prefix: Identifier | None = None) -> list[Identifier]:
        raise NotImplementedError

    def _list_tables(self, prefix: Identifier | None = None) -> list[Identifier]:
        raise NotImplementedError

    def _has_namespace(self, ident):
        raise NotImplementedError

    def _has_table(self, ident):
        return str(ident) in self._tables


class MockTable(Table):
    _name: str
    properties: Properties

    def __init__(self, name: str, properties: Properties | None = None) -> None:
        self._name = name
        self.properties = properties

    @property
    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        raise NotImplementedError

    def read(self, **options) -> DataFrame:
        raise NotImplementedError

    def append(self, df: DataFrame, **options) -> None:
        raise NotImplementedError

    def overwrite(self, df: DataFrame, **options) -> None:
        raise NotImplementedError


def test_session_create_table_with_properties():
    from daft.session import Session

    catalog = MockCatalog()
    sess = Session()
    sess.attach_catalog(catalog)

    # properties to pass through
    properties = {"format": "parquet", "partitioning": ["col1"], "description": "Test table with properties"}

    schema = Schema.from_pydict(
        {
            "a": dt.bool(),
            "b": dt.int64(),
            "c": dt.string(),
        }
    )

    # pass as kwargs
    _ = sess.create_table("t1", schema, **properties)
    t1 = sess.get_table("t1")
    assert t1
    assert t1.name == "t1"
    assert t1.properties == properties

    # pass as dict
    _ = catalog.create_table("t2", schema, properties=properties)
    t2 = catalog.get_table("t2")
    assert t2
    assert t2.name == "t2"
    assert t2.properties == properties
