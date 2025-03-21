"""WARNING! These APIs are internal; please use Catalog.from_iceberg() and Table.from_iceberg()."""

from __future__ import annotations

import warnings

from pyiceberg.catalog import Catalog as InnerCatalog
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table as InnerTable

from daft.catalog import Catalog, Identifier, Table, TableSource
from daft.dataframe import DataFrame
from daft.io._iceberg import read_iceberg
from daft.logical.schema import Schema


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
    def _load_catalog(name: str, **options) -> IcebergCatalog:
        c = IcebergCatalog.__new__(IcebergCatalog)
        c._inner = load_catalog(name, **options)
        return c

    @property
    def name(self) -> str:
        return self._inner.name

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        if isinstance(identifier, Identifier):
            identifier = tuple(identifier)  # type: ignore
        self._inner.create_namespace(identifier)

    def create_table(self, identifier: Identifier | str, source: TableSource | object) -> Table:
        if isinstance(source, DataFrame):
            return self._create_table_from_df(identifier, source)
        elif isinstance(source, str):
            return self._create_table_from_path(identifier, source)
        elif isinstance(source, Schema):
            return self._create_table_from_schema(identifier, source)
        else:
            raise Exception(f"Unknown table source: {source}")

    def _create_table_from_df(self, ident: Identifier | str, source: DataFrame) -> Table:
        t = self._create_table_from_schema(ident, source.schema())
        t.append(source)
        return t

    def _create_table_from_path(self, ident: Identifier | str, source: str) -> Table:
        raise ValueError("table from path not yet supported")

    def _create_table_from_schema(self, ident: Identifier | str, source: Schema) -> Table:
        if isinstance(ident, Identifier):
            ident = tuple(ident)  # type: ignore
        obj = self._inner.create_table(ident, schema=source.to_pyarrow_schema())
        return IcebergTable._from_obj(obj)

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        if isinstance(identifier, Identifier):
            identifier = tuple(identifier)  # type: ignore
        self._inner.drop_namespace(identifier)

    def drop_table(self, identifier: Identifier | str):
        if isinstance(identifier, Identifier):
            identifier = tuple(identifier)  # type: ignore
        self._inner.drop_table(identifier)

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str) -> IcebergTable:
        if isinstance(identifier, Identifier):
            identifier = tuple(identifier)  # type: ignore
        return IcebergTable._from_obj(self._inner.load_table(identifier))

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List namespaces under the given namespace (pattern) in the catalog, or all namespaces if no namespace is provided."""
        return [Identifier(*tup) for tup in self._inner.list_namespaces(pattern)]

    def list_tables(self, pattern: str | None = None) -> list[str]:
        """List tables under the given namespace (pattern) in the catalog, or all tables if no namespace is provided."""
        return [".".join(tup) for tup in self._inner.list_tables(pattern or ())]


class IcebergTable(Table):
    _inner: InnerTable

    _read_options = {"snapshot_id"}
    _write_options = set()

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

    @staticmethod
    def _from_obj(obj: object) -> IcebergTable | None:
        """Returns an IcebergTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = IcebergTable.__new__(IcebergTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported iceberg table type: {type(obj)}")

    def read(self, **options) -> DataFrame:
        Table._validate_options("Iceberg read", options, IcebergTable._read_options)

        return read_iceberg(self._inner, snapshot_id=options.get("snapshot_id"))

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        self._validate_options("Iceberg write", options, IcebergTable._write_options)

        df.write_iceberg(self._inner, mode=mode)
