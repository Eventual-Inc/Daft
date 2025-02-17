"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.catalog import Catalog, Table

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as InnerCatalog
    from pyiceberg.table import Table as InnerTable

    from daft.dataframe import DataFrame


class IcebergCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self, pyiceberg_catalog: InnerCatalog):
        """DEPRECATED: Please use `Catalog.from_iceberg`."""
        self._inner = pyiceberg_catalog

    ###
    # get_*
    ###

    def get_table(self, name: str) -> IcebergTable:
        return IcebergTable(self._inner.load_table(name))

    ###
    # list_*
    ###

    def list_tables(self, pattern: str | None = None) -> list[str]:
        namespace = pattern if pattern else ""  # pyiceberg lists on namespaces
        return [".".join(tup) for tup in self._inner.list_tables(namespace)]


class IcebergTable(Table):
    _inner: InnerTable

    def __init__(self, inner: InnerTable):
        self._inner = inner

    def read(self) -> DataFrame:
        import daft

        return daft.read_iceberg(self._inner)
