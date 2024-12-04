from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.unity_catalog import UnityCatalog, UnityCatalogTable

from daft.catalog.python_catalog import PythonCatalog, PythonCatalogTable


class UnityCatalogAdaptor(PythonCatalog):
    def __init__(self, unity_catalog: UnityCatalog):
        self._catalog = unity_catalog

    def list_tables(self, prefix: str) -> list[str]:
        num_namespaces = prefix.count(".")
        if prefix == "":
            return [
                tbl
                for cat in self._catalog.list_catalogs()
                for schema in self._catalog.list_schemas(cat)
                for tbl in self._catalog.list_tables(schema)
            ]
        elif num_namespaces == 0:
            catalog_name = prefix
            return [
                tbl for schema in self._catalog.list_schemas(catalog_name) for tbl in self._catalog.list_tables(schema)
            ]
        elif num_namespaces == 1:
            schema_name = prefix
            return [tbl for tbl in self._catalog.list_tables(schema_name)]
        else:
            raise ValueError(
                f"Unrecognized catalog name or schema name, expected a '.'-separated namespace but received: {prefix}"
            )

    def load_table(self, name: str) -> UnityTableAdaptor:
        return UnityTableAdaptor(self._catalog.load_table(name))


class UnityTableAdaptor(PythonCatalogTable):
    def __init__(self, unity_table: UnityCatalogTable):
        self._table = unity_table

    def to_dataframe(self) -> DataFrame:
        import daft

        return daft.read_deltalake(self._table)
