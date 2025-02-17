"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from pyiceberg.catalog import Catalog as InnerCatalog
from pyiceberg.table import Table as InnerTable

from daft.catalog import Catalog, Table

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


class IcebergCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self, pyiceberg_catalog: InnerCatalog):
        """DEPRECATED: Please use `Catalog.from_iceberg`; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please use `Catalog.from_iceberg` instead.",
            category=DeprecationWarning,
        )
        self._inner = pyiceberg_catalog

    @staticmethod
    def _from_obj(obj: object) -> IcebergCatalog:
        """Returns an IcebergCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = IcebergCatalog.__new__(IcebergCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported iceberg catalog type: {type(obj)}")

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
        """List tables under the given namespace (pattern) in the catalog, or all tables if no namespace is provided."""
        return [".".join(tup) for tup in self._inner.list_tables(pattern)]


class IcebergTable(Table):
    _inner: InnerTable

    def __init__(self):
        raise ValueError("IcebergTable.__init__ is not supported, please use `Table.from_iceberg`.")

    @staticmethod
    def _from_obj(obj: object) -> IcebergTable | None:
        """Returns an IcebergTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = IcebergTable.__new__(IcebergTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported iceberg table type: {type(obj)}")

    @property
    def inner(self) -> InnerTable:
        """Returns the inner iceberg table."""
        return self._inner

    def read(self) -> DataFrame:
        import daft

        return daft.read_iceberg(self._inner)
