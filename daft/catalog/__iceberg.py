"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from pyiceberg.catalog import Catalog as InnerCatalog
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.table import Table as InnerTable

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.io.iceberg._iceberg import read_iceberg

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

    @staticmethod
    def _load_catalog(name: str, **options: str | None) -> IcebergCatalog:
        c = IcebergCatalog.__new__(IcebergCatalog)
        c._inner = load_catalog(name, **options)
        return c

    @property
    def name(self) -> str:
        return self._inner.name

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.create_namespace(ident)

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
    ) -> Table:
        i = _to_pyiceberg_ident(identifier)
        t = IcebergTable.__new__(IcebergTable)
        t._inner = self._inner.create_table(i, schema=schema.to_pyarrow_schema())
        return t

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.drop_namespace(ident)

    def _drop_table(self, identifier: Identifier) -> None:
        ident = _to_pyiceberg_ident(identifier)
        self._inner.drop_table(ident)

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        ident = _to_pyiceberg_ident(identifier)
        try:
            _ = self._inner.list_namespaces(ident)
            return True
        except NoSuchNamespaceError:
            return False

    def _has_table(self, identifier: Identifier) -> bool:
        ident = _to_pyiceberg_ident(identifier)
        try:
            # using load_table instead of table_exists because table_exists does not work with an instance of the `tabulario/iceberg-rest` Docker image
            self._inner.load_table(ident)
            return True
        except NoSuchTableError:
            return False

    ###
    # get_*
    ###

    def _get_table(self, identifier: Identifier) -> IcebergTable:
        ident = _to_pyiceberg_ident(identifier)
        try:
            return IcebergTable._from_obj(self._inner.load_table(ident))
        except NoSuchTableError as ex:
            # convert to not found because we want to (sometimes) ignore it internally
            raise NotFoundError() from ex
        except Exception as ex:
            # wrap original exceptions
            raise Exception("pyiceberg raised an exception while calling get_table") from ex

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        prefix = () if pattern is None else _to_pyiceberg_ident(pattern)
        return [Identifier(*tup) for tup in self._inner.list_namespaces(prefix)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is None:
            tables = []
            for ns in self.list_namespaces():
                tables.extend(self._inner.list_tables(str(ns)))
        else:
            tables = self._inner.list_tables(pattern)
        return [Identifier(*tup) for tup in tables]


class IcebergTable(Table):
    _inner: InnerTable

    _read_options = {"snapshot_id"}
    _write_options: set[str] = set()

    def __init__(self, inner: InnerTable):
        """DEPRECATED: Please use `Table.from_iceberg`; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please prefer using `Table.from_iceberg` instead; version 0.5.0!",
            category=DeprecationWarning,
        )
        self._inner = inner

    @property
    def name(self) -> str:
        return self._inner.name()[-1]

    def schema(self) -> Schema:
        return self.read().schema()

    @staticmethod
    def _from_obj(obj: object) -> IcebergTable:
        """Returns an IcebergTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = IcebergTable.__new__(IcebergTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported iceberg table type: {type(obj)}")

    def read(self, **options: Any | None) -> DataFrame:
        Table._validate_options("Iceberg read", options, IcebergTable._read_options)
        return read_iceberg(self._inner, snapshot_id=options.get("snapshot_id"))

    def append(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Iceberg write", options, IcebergTable._write_options)

        df.write_iceberg(self._inner, mode="append")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self._validate_options("Iceberg write", options, IcebergTable._write_options)

        df.write_iceberg(self._inner, mode="overwrite")


def _to_pyiceberg_ident(ident: Identifier | str) -> tuple[str, ...] | str:
    return tuple(ident) if isinstance(ident, Identifier) else ident
