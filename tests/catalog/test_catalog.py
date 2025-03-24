from daft.catalog import Catalog


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


def test_register_python_catalog():
    import daft.unity_catalog

    # sanity check for backwards compatibility
    cat1 = daft.unity_catalog.UnityCatalog("", "")
    daft.catalog.register_python_catalog(cat1, "test")
    daft.catalog.unregister_catalog("test")


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
    from daft.catalog import Identifier
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

    assert cat.list_namespaces("XXX") == []
    assert cat.list_namespaces("ns1") == [Identifier("ns1")]
    assert cat.list_namespaces("ns2") == [Identifier("ns2")]
    namespaces = cat.list_namespaces("ns")
    assert Identifier("ns1") in namespaces
    assert Identifier("ns2") in namespaces

    # session name resolution should still work
    sess = Session()
    sess.attach_catalog(cat)
    assert sess.get_table("default.T0") is not None
    assert sess.get_table("default.ns1.T1") is not None
    assert sess.get_table("default.ns2.T2") is not None
