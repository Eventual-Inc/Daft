from daft.catalog import Catalog


def test_try_from_iceberg(tmpdir):
    from pyiceberg.catalog.sql import SqlCatalog

    pyiceberg_catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    assert Catalog._try_from(pyiceberg_catalog) is not None
    assert Catalog._try_from_iceberg(pyiceberg_catalog) is not None


def test_try_from_unity():
    from daft.unity_catalog import UnityCatalog

    unity_catalog = UnityCatalog("-", token="-")
    assert Catalog._try_from(unity_catalog) is not None
    assert Catalog._try_from_unity(unity_catalog) is not None
