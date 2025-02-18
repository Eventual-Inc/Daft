"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import Catalog as InnerCatalog
from pyiceberg.table import Table as InnerTable

from daft.catalog import Catalog, Table

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


class IcebergCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self, pyiceberg_catalog: InnerCatalog):
        """DEPRECATED: Please use `Catalog.from_iceberg`."""
        self._inner = pyiceberg_catalog

    @staticmethod
    def _try_from(obj: object) -> IcebergCatalog | None:
        """Returns an IcebergCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            return IcebergCatalog(obj)
        return None

    @property
    def inner(self) -> InnerCatalog:
        """Returns the inner iceberg catalog."""
        return self._inner

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

    @staticmethod
    def _try_from(obj: object) -> IcebergTable | None:
        """Returns an IcebergTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            return IcebergTable(obj)
        return None

    @property
    def inner(self) -> InnerTable:
        """Returns the inner iceberg table."""
        return self._inner

    def read(self) -> DataFrame:
        import daft

        return daft.read_iceberg(self._inner)
