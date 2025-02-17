"""WARNING! These APIs are internal; please use Catalog.from_unity() and Table.from_unity()."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.catalog import Catalog, Table

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.unity_catalog import UnityCatalog as InnerCatalog
    from daft.unity_catalog import UnityCatalogTable as InnerTable


class UnityCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self, unity_catalog: InnerCatalog):
        """DEPRECATED: Please use `Catalog.from_unity`."""
        self._inner = unity_catalog

    ###
    # get_*
    ###

    def get_table(self, name: str) -> UnityTable:
        return UnityTable(self._inner.load_table(name))

    ###
    # list_.*
    ###

    def list_tables(self, pattern: str | None = None) -> list[str]:
        if pattern is None or pattern == "":
            return [
                tbl
                for cat in self._inner.list_catalogs()
                for schema in self._inner.list_schemas(cat)
                for tbl in self._inner.list_tables(schema)
            ]
        num_namespaces = pattern.count(".")
        if num_namespaces == 0:
            catalog_name = pattern
            return [tbl for schema in self._inner.list_schemas(catalog_name) for tbl in self._inner.list_tables(schema)]
        elif num_namespaces == 1:
            schema_name = pattern
            return [tbl for tbl in self._inner.list_tables(schema_name)]
        else:
            raise ValueError(
                f"Unrecognized catalog name or schema name, expected a '.'-separated namespace but received: {pattern}"
            )


class UnityTable(Table):
    _inner: InnerTable

    def __init__(self, unity_table: InnerTable):
        self._inner = unity_table

    def read(self) -> DataFrame:
        import daft

        return daft.read_deltalake(self._inner)
