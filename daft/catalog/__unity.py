"""WARNING! These APIs are internal; please use Catalog.from_unity() and Table.from_unity()."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from daft.catalog import Catalog, Identifier, Table
from daft.unity_catalog import UnityCatalog as InnerCatalog  # noqa: TID253
from daft.unity_catalog import UnityCatalogTable as InnerTable  # noqa: TID253

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


class UnityCatalog(Catalog):
    _inner: InnerCatalog

    def __init__(self, unity_catalog: InnerCatalog):
        """DEPRECATED: Please use `Catalog.from_unity`; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please prefer using `Catalog.from_unity` instead; version 0.5.0!",
            category=DeprecationWarning,
        )
        self._inner = unity_catalog

    @staticmethod
    def _from_obj(obj: object) -> UnityCatalog:
        """Returns an UnityCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = UnityCatalog.__new__(UnityCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported unity catalog type: {type(obj)}")

    ###
    # get_*
    ###

    def get_table(self, ident: Identifier | str) -> UnityTable:
        if isinstance(ident, Identifier):
            ident = ".".join(ident)  # TODO unity qualified identifiers
        return UnityTable(self._inner.load_table(ident))

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
        """DEPRECATED: Please use `Table.from_unity`; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please prefer using `Table.from_unity` instead; version 0.5.0!",
            category=DeprecationWarning,
        )
        self._inner = unity_table

    @staticmethod
    def _from_obj(obj: object) -> UnityTable | None:
        """Returns a UnityTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = UnityTable.__new__(UnityTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported unity table type: {type(obj)}")

    @staticmethod
    def _try_from(obj: object) -> UnityTable | None:
        """Returns an UnityTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            return UnityTable(obj)
        return None

    @property
    def inner(self) -> InnerTable:
        """Returns the inner unity table."""
        return self._inner

    def read(self) -> DataFrame:
        import daft

        return daft.read_deltalake(self._inner)
