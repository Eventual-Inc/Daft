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
