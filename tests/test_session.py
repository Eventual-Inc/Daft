from __future__ import annotations

import pytest

import daft
from daft.catalog import Catalog, Function, Identifier, NotFoundError, Table
from daft.session import Session

###
# SESSION SETUP
###


def test_current_session_exists():
    assert daft.current_session() is not None


###
# ATTACH & DETACH CATALOG
###


def test_attach_catalog():
    sess = Session()
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
        sess.attach_catalog(cat1, alias="cat1")


def test_detach_catalog():
    sess = Session()
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


###
# ATTACH & DETACH TABLE
###


def test_attach_table():
    sess = Session()
    #
    # create some tables
    view1 = Table.from_df("view1", daft.from_pydict({"x": [1.1, 2.2, 3.3]}))
    view2 = Table.from_df("view2", daft.from_pydict({"y": ["a", "b", "c"]}))
    #
    # attach them..
    sess.attach_table(view1, alias="tbl1")
    sess.attach_table(view2, alias="tbl2")
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    #
    # get_table exact object (proper unwrapping)
    assert sess.get_table("tbl1") == view1
    assert sess.get_table("tbl2") == view2
    # read_table
    assert sess.read_table("tbl1").to_pydict() == view1.read().to_pydict()
    assert sess.read_table("tbl2").to_pydict() == view2.read().to_pydict()
    #
    # error!
    with pytest.raises(Exception, match="already exists"):
        sess.attach_table(view1, alias="tbl1")


def test_detach_table():
    sess = Session()
    #
    # setup.
    view1 = Table.from_df("view1", daft.from_pydict({"x": [1.1, 2.2, 3.3]}))
    sess.attach_table(view1, alias="tbl1")
    #
    # check is attached
    assert 1 == len(sess.list_tables())
    #
    # detach existing
    sess.detach_table("tbl1")
    assert 0 == len(sess.list_tables())
    #
    # error!
    with pytest.raises(Exception, match="not found"):
        sess.detach_table("tbl1")


###
# CREATE TABLE
###


def test_create_temp_table():
    sess = Session()
    #
    # create some dataframes
    df1 = daft.from_pydict({"x": [1.1, 2.2, 3.3]})
    df2 = daft.from_pydict({"y": ["a", "b", "c"]})
    #
    # create temp tables from these views
    sess.create_temp_table("tbl1", df1)
    sess.create_temp_table("tbl2", df2)
    #
    # list_tables
    assert 2 == len(sess.list_tables())
    #
    # read_table
    assert sess.read_table("tbl1").to_pydict() == df1.to_pydict()
    assert sess.read_table("tbl2").to_pydict() == df2.to_pydict()
    #
    # replace tbl1 with df2
    sess.create_temp_table("tbl1", df2)
    assert sess.read_table("tbl1").to_pydict() == df2.to_pydict()


###
# USE / SET [CATALOG|SCHEMA]
###


def test_use():
    sess = Session()
    #
    # create some catalogs to use
    cat1 = Catalog.from_pydict(name="cat1", tables={})
    cat2 = Catalog.from_pydict(name="cat2", tables={})
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # current catalog defaults to the first attached
    assert sess.current_catalog() == cat1
    #
    # set and assert
    sess.set_catalog("cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    # set a namespace
    sess.set_namespace("a.b")
    assert sess.current_namespace() == Identifier("a", "b")
    #
    sess.set_catalog(None)
    sess.set_namespace(None)
    assert sess.current_catalog() is None
    assert sess.current_namespace() is None
    #
    # test use <catalog>
    sess.use("cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    #  test use <catalog>.<namespace>
    sess.use("cat2.a.b")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() == Identifier("a", "b")


def test_sql():
    sess = Session()
    #
    # create some catalogs to use
    cat1 = Catalog.from_pydict(name="cat1", tables={})
    cat2 = Catalog.from_pydict(name="cat2", tables={})
    sess.attach(cat1)
    sess.attach(cat2)
    #
    # test use <catalog>
    sess.sql("USE cat2")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() is None
    #
    #  test use <catalog>.<namespace>
    sess.sql("USE cat2.a.b")
    assert sess.current_catalog() == cat2
    assert sess.current_namespace() == Identifier("a", "b")


###
# exception testing
###


def test_exception_surfacing():
    class ThrowingCatalog(Catalog):
        @property
        def name(self):
            return "throwing"

        def _create_namespace(self, identifier):
            raise NotImplementedError

        def _create_table(self, identifier, source):
            raise NotImplementedError

        def _drop_namespace(self, identifier):
            raise NotImplementedError

        def _drop_table(self, identifier):
            raise NotImplementedError

        def _get_table(self, identifier):
            if str(identifier) == "boom":
                raise RuntimeError("something went wrong")
            raise NotFoundError(f"Table {identifier} not found")

        def _list_namespaces(self, pattern=None):
            raise NotImplementedError

        def _list_tables(self, pattern=None):
            raise NotImplementedError

        def _has_namespace(self, ident):
            raise NotImplementedError

        def _has_table(self, ident):
            if str(ident) == "boom":
                raise RuntimeError("something went wrong")
            raise NotFoundError(f"Table {ident} not found")

    sess = Session()
    sess.attach(ThrowingCatalog())

    # the session should
    with pytest.raises(Exception, match="not found"):
        sess.read_table("test")

    # some internal error should be surfaced in the runtime exception
    with pytest.raises(Exception, match="something went wrong"):
        sess.read_table("boom")


#
# PROVIDERS
#


class MockProvider:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name


def test_attach_provider():
    sess = Session()

    provider = MockProvider("test_provider")
    expect = sess.attach_provider(provider)
    actual = sess.get_provider("test_provider")

    assert expect is actual


def test_attach_provider_with_alias():
    sess = Session()

    provider = MockProvider("test_provider")
    expect = sess.attach_provider(provider, alias="alias")
    actual = sess.get_provider("alias")

    assert expect is actual
    with pytest.raises(Exception):
        sess.get_provider("test_provider")


def test_detach_provider():
    sess = Session()

    provider = MockProvider("test_provider")
    sess.attach_provider(provider)
    sess.detach_provider("test_provider")

    with pytest.raises(Exception):
        sess.get_provider("test_provider")


def test_set_provider_and_current_provider(monkeypatch):
    mock_provider = MockProvider("mock_provider")
    sess = Session()
    sess.attach_provider(mock_provider)
    sess.set_provider("mock_provider")
    assert sess.current_provider() is mock_provider


def test_current_session_drop_table():
    sess = Session()
    sess.attach(
        Catalog.from_pydict(
            name="my_catalog",
            tables={
                "ns.t1": {"x": [1, 2, 3]},
                "ns.t2": {"x": [4, 5, 6]},
            },
        )
    )
    daft.set_session(sess)

    cata = daft.current_catalog()
    assert cata.list_namespaces() == [Identifier.from_str("ns")]
    assert cata.list_tables("ns.%") == [Identifier.from_str("ns.t1"), Identifier.from_str("ns.t2")]

    daft.drop_table("ns.t1")

    assert cata.has_namespace("ns")
    assert not cata.has_table("ns.t1")
    assert cata.has_table("ns.t2")

    daft.drop_namespace("ns")
    assert not cata.has_namespace("ns")


###
# CATALOG FUNCTION FALLBACK
###


class _FunctionCatalog(Catalog):
    """A minimal catalog that supports _get_function for testing."""

    _functions: dict

    def __init__(self):
        self._functions = {}

    def register_function(
        self,
        ident: Identifier,
        module_name: str,
        binding_name: str,
    ):
        self._functions[str(ident)] = Function(ident, module_name, binding_name)

    @property
    def name(self):
        return "func_catalog"

    def _create_namespace(self, ident):
        raise NotImplementedError

    def _create_table(self, ident, schema, properties=None, partition_fields=None):
        raise NotImplementedError

    def _drop_namespace(self, ident):
        raise NotImplementedError

    def _drop_table(self, ident):
        raise NotImplementedError

    def _get_table(self, ident):
        raise NotFoundError(f"Table {ident} not found")

    def _list_namespaces(self, pattern=None):
        return []

    def _list_tables(self, pattern=None):
        return []

    def _has_namespace(self, ident):
        return False

    def _has_table(self, ident):
        return False

    def _get_function(self, ident):
        # ident is an Identifier; the last part is the function name
        return self._functions.get(str(ident))


def test_session_catalog_function_fallback():
    """Test that catalog _get_function is used during SQL plan resolution."""
    catalog = _FunctionCatalog()
    catalog.register_function(Identifier("my_catalog_udf"), "tests.udf.my_funcs", "catalog_udf")

    sess = Session()
    sess.attach_catalog(catalog)

    # Verify the catalog itself returns the function
    assert catalog.get_function(Identifier("my_catalog_udf"))(1) is not None

    # Verify the function is accessible via SQL plan resolution.
    # The UDF registered in the catalog should be found during SQL planning.
    df = daft.from_pydict({"x": [1, 2, 3]})
    sess.attach_table(Table.from_df("t", df), alias="t")
    daft.set_session(sess)
    result = sess.sql("SELECT my_catalog_udf(x) FROM t")
    assert result is not None


def test_session_catalog_function_fallback_returns_none():
    """Test that catalog get_function returns None when function is not found."""
    catalog = _FunctionCatalog()

    # Catalog itself should return None for nonexistent functions
    assert catalog.get_function("nonexistent_function") is None


def test_session_function_priority_over_catalog():
    """Test that session-scoped functions take priority over catalog functions."""
    catalog = _FunctionCatalog()
    catalog.register_function(Identifier("shared_fn"), "tests.udf.my_funcs", "my_catalog_udf")

    @daft.udf(return_dtype=daft.DataType.int64())
    def session_udf(x):
        return x + 1

    sess = Session()
    sess.attach_catalog(catalog)
    sess.attach_function(session_udf, alias="shared_fn")

    # Both catalog and session have "shared_fn", but session should take priority.
    # Verify via SQL that the function resolves (session-scoped wins).
    df = daft.from_pydict({"x": [1, 2, 3]})
    sess.attach_table(Table.from_df("t", df), alias="t")
    daft.set_session(sess)
    result = sess.sql("SELECT shared_fn(x) FROM t")
    assert result is not None


def test_dataframe_select_with_catalog_get_function():
    """Test that dataframe.select can use a UDF retrieved via catalog.get_function."""
    catalog = _FunctionCatalog()
    catalog.register_function(Identifier("double_value"), "tests.udf.my_funcs", "double_value")

    sess = Session()
    sess.attach_catalog(catalog)
    daft.set_session(sess)

    # Retrieve the UDF from the catalog and use it directly in dataframe.select
    udf_fn = catalog.get_function(Identifier("double_value"))

    df = daft.from_pydict({"x": [1, 2, 3]})
    result = df.select(udf_fn(df["x"]))

    assert result.to_pydict() == {"x": [2, 4, 6]}


def test_catalog_register_cls_udf_from_external_module():
    """Test that a @daft.cls UDF can be loaded from an external module via register_function."""
    catalog = _FunctionCatalog()
    catalog.register_function(
        Identifier("mock_predictor"),
        module_name="tests.udf.my_funcs",
        binding_name="MockModelPredictor",
    )

    predictor_cls = catalog.get_function(Identifier("mock_predictor"))
    assert predictor_cls is not None

    predictor = predictor_cls("bert-base")
    df = daft.from_pydict({"text": ["hello", "world", "daft"]})
    result = df.select(predictor(df["text"])).to_pydict()

    expected = {
        "text": [
            "model bert-base predict hello",
            "model bert-base predict world",
            "model bert-base predict daft",
        ]
    }
    assert result == expected
