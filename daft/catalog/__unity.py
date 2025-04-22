"""WARNING! These APIs are internal; please use Catalog.from_unity() and Table.from_unity()."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Literal

from unitycatalog import NotFoundError as UnityNotFoundError

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.io._deltalake import read_deltalake
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

    @property
    def name(self) -> str:
        # TODO feat: add names to unity catalogs
        return "unity"

    @staticmethod
    def _from_obj(obj: object) -> UnityCatalog:
        """Returns an UnityCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = UnityCatalog.__new__(UnityCatalog)
            c._inner = obj
            return c
        raise ValueError(f"Unsupported unity catalog type: {type(obj)}")

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        raise NotImplementedError("Unity create_namespace not yet supported.")

    def create_table(
        self,
        identifier: Identifier | str,
        source: Schema | DataFrame,
        properties: Properties | None = None,
    ) -> Table:
        raise NotImplementedError("Unity create_table not yet supported.")

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        raise NotImplementedError("Unity drop_namespace not yet supported.")

    def drop_table(self, identifier: Identifier | str):
        raise NotImplementedError("Unity drop_table not yet supported.")

    ###
    # get_*
    ###

    def get_table(self, ident: Identifier | str) -> UnityTable:
        if isinstance(ident, Identifier):
            ident = ".".join(ident)  # TODO unity qualified identifiers
        try:
            return UnityTable(self._inner.load_table(ident))
        except UnityNotFoundError:
            raise NotFoundError(f"Table {ident} not found!")

    ###
    # list_.*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        raise NotImplementedError("Unity list_namespaces not yet supported.")

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

    _read_options = {"version"}
    _write_options = {
        "schema_mode",
        "partition_col",
        "description",
        "configuration",
        "custom_metadata",
        "dynamo_table_name",
        "allow_unsafe_rename",
    }

    def __init__(self, unity_table: InnerTable):
        """DEPRECATED: Please use `Table.from_unity`; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please prefer using `Table.from_unity` instead; version 0.5.0!",
            category=DeprecationWarning,
        )
        self._inner = unity_table

    @property
    def name(self) -> str:
        return self._inner.table_info.name

    @staticmethod
    def _from_obj(obj: object) -> UnityTable:
        """Returns a UnityTable if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = UnityTable.__new__(UnityTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported unity table type: {type(obj)}")

    ###
    # read methods
    ###

    def read(self, **options) -> DataFrame:
        Table._validate_options("Unity read", options, UnityTable._read_options)

        return read_deltalake(self._inner, version=options.get("version"))

    ###
    # write methods
    ###

    def write(self, df: DataFrame, mode: Literal["append", "overwrite", "error", "ignore"] = "append", **options):
        self._validate_options("Unity write", options, UnityTable._write_options)

        return df.write_deltalake(
            self._inner,
            mode=mode,
            schema_mode=options.get("schema_mode"),
            partition_cols=options.get("partition_cols"),
            description=options.get("description"),
            configuration=options.get("configuration"),
            custom_metadata=options.get("custom_metadata"),
            dynamo_table_name=options.get("dynamo_table_name"),
            allow_unsafe_rename=options.get("allow_unsafe_rename", False),
        )
