from __future__ import annotations

from typing import TYPE_CHECKING

from daft.catalog import Catalog, Function, Identifier, Properties, Table
from daft.dataframe import DataFrame
from daft.logical.schema import DataType as dt
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.io.partitioning import PartitionField


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
    from daft.catalog.__unity._client import UnityCatalogClient

    unity_catalog = UnityCatalogClient("-", token="-")
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


def test_from_pydict_with_identifier_keys():
    data = {"x": [1, 2, 3]}

    # Test with Identifier objects as keys (using single-level and namespace.table format)
    cat = Catalog.from_pydict(
        {
            Identifier("table1"): data,
            Identifier("namespace", "table2"): data,
        }
    )

    assert len(cat.list_tables()) == 2
    assert cat.get_table("table1") is not None
    assert cat.get_table("namespace.table2") is not None

    # Test mixing string and Identifier keys
    cat2 = Catalog.from_pydict(
        {
            "string_key": data,
            Identifier("identifier_key"): data,
        }
    )

    assert len(cat2.list_tables()) == 2
    assert cat2.get_table("string_key") is not None
    assert cat2.get_table("identifier_key") is not None


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

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
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


###
# _get_function tests
###


class MockCatalogWithFunctions(Catalog):
    """A mock catalog that supports function lookup via _get_function."""

    _functions: dict[str, object]

    def __init__(self):
        self._functions = {}

    def register_function(
        self,
        name,
        module_name: str,
        binding_name: str,
    ):
        self._functions[name] = Function(module_name, binding_name)

    @property
    def name(self) -> str:
        return "test_with_functions"

    def _create_namespace(self, ident: Identifier):
        raise NotImplementedError

    def _create_table(self, ident, schema, properties=None, partition_fields=None) -> Table:
        raise NotImplementedError

    def _drop_namespace(self, ident):
        raise NotImplementedError

    def _drop_table(self, ident):
        raise NotImplementedError

    def _get_table(self, ident) -> Table:
        raise NotImplementedError

    def _list_namespaces(self, pattern=None):
        raise NotImplementedError

    def _list_tables(self, pattern=None):
        raise NotImplementedError

    def _has_namespace(self, ident):
        raise NotImplementedError

    def _has_table(self, ident):
        raise NotImplementedError

    def _get_function(self, ident: Identifier) -> Function | None:
        func_name = ident[-1]
        return self._functions.get(func_name)


def test_catalog_get_function_default_returns_none():
    """Test that the default _get_function returns None."""
    catalog = MockCatalog()
    assert catalog.get_function("any_function") is None


def test_catalog_get_function_with_override():
    """Test that a catalog with _get_function override returns the function."""
    catalog = MockCatalogWithFunctions()

    catalog.register_function("my_func", "tests.udf.my_funcs", "catalog_udf")

    # found
    assert catalog.get_function("my_func").to_py_func() is not None

    # not found
    assert catalog.get_function("nonexistent") is None


def test_catalog_get_function_from_pydict_returns_none():
    """Test that the built-in from_pydict catalog returns None for get_function (default behavior)."""
    catalog = Catalog.from_pydict({"t": {"x": [1, 2, 3]}})
    assert catalog.get_function("anything") is None


def test_catalog_get_function_module_not_found():
    """Test that to_py_func raises ImportError when the module does not exist."""
    catalog = MockCatalogWithFunctions()
    catalog.register_function(
        "bad_module_fn",
        module_name="tests.udf.nonexistent_module",
        binding_name="some_func",
    )

    func = catalog.get_function("bad_module_fn")
    assert func is not None

    import pytest

    with pytest.raises((ImportError, ModuleNotFoundError)):
        func.to_py_func()


def test_catalog_get_function_binding_not_found():
    """Test that to_py_func returns None when the binding name does not exist in the module."""
    catalog = MockCatalogWithFunctions()
    catalog.register_function(
        "bad_binding_fn",
        module_name="tests.udf.my_funcs",
        binding_name="nonexistent_function_name",
    )

    func = catalog.get_function("bad_binding_fn")
    assert func is not None
    assert func.to_py_func() is None
