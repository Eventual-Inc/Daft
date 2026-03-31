"""WARNING! These APIs are internal; please use Catalog.from_paimon() and Table.from_paimon()."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from pypaimon.catalog.catalog import Catalog as InnerCatalog
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)
from pypaimon.table.table import Table as InnerTable

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField


class PaimonCatalog(Catalog):
    _inner: InnerCatalog
    _name: str

    def __init__(self) -> None:
        raise RuntimeError("PaimonCatalog.__init__ is not supported, please use `Catalog.from_paimon` instead.")

    @staticmethod
    def _from_obj(obj: object, name: str = "paimon") -> PaimonCatalog:
        """Returns a PaimonCatalog instance if the given object can be adapted so."""
        if isinstance(obj, InnerCatalog):
            c = PaimonCatalog.__new__(PaimonCatalog)
            c._inner = obj
            c._name = name
            return c
        raise ValueError(f"Unsupported paimon catalog type: {type(obj)}")

    @property
    def name(self) -> str:
        return self._name

    # ------------------------------------------------------------------
    # Internal capability helpers
    # ------------------------------------------------------------------

    def _supports_listing(self) -> bool:
        """True when the inner catalog exposes list_databases / list_tables (e.g. RESTCatalog)."""
        return hasattr(self._inner, "list_databases") and hasattr(self._inner, "list_tables")

    def _supports_drop(self) -> bool:
        """True when the inner catalog exposes drop_database / drop_table (e.g. RESTCatalog)."""
        return hasattr(self._inner, "drop_database") and hasattr(self._inner, "drop_table")

    ###
    # create_*
    ###

    def _create_namespace(self, ident: Identifier) -> None:
        db_name = _to_paimon_db_name(ident)
        self._inner.create_database(db_name, ignore_if_exists=False)

    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        import pypaimon

        pa_schema = _cast_large_types(schema.to_pyarrow_schema())
        partition_keys = [pf.field.name for pf in (partition_fields or [])]
        primary_keys = list((properties or {}).get("primary_keys", []))

        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=partition_keys,
            primary_keys=primary_keys,
        )

        paimon_ident = _to_paimon_ident_str(ident)
        self._inner.create_table(paimon_ident, paimon_schema, ignore_if_exists=False)

        inner_table = self._inner.get_table(paimon_ident)
        return PaimonTable._from_obj(inner_table)

    ###
    # drop_*
    ###

    def _drop_namespace(self, ident: Identifier) -> None:
        if not self._supports_drop():
            raise NotImplementedError(
                "This Paimon catalog does not support dropping namespaces. "
                "drop_database is only available on RESTCatalog."
            )
        db_name = _to_paimon_db_name(ident)
        try:
            self._inner.drop_database(db_name, ignore_if_exists=False)
        except DatabaseNotExistException as ex:
            raise NotFoundError(f"Namespace '{db_name}' not found.") from ex

    def _drop_table(self, ident: Identifier) -> None:
        if not self._supports_drop():
            raise NotImplementedError(
                "This Paimon catalog does not support dropping tables. drop_table is only available on RESTCatalog."
            )
        paimon_ident = _to_paimon_ident_str(ident)
        try:
            self._inner.drop_table(paimon_ident, ignore_if_exists=False)
        except TableNotExistException as ex:
            raise NotFoundError(f"Table '{paimon_ident}' not found.") from ex

    ###
    # has_*
    ###

    def _has_namespace(self, ident: Identifier) -> bool:
        db_name = _to_paimon_db_name(ident)
        try:
            self._inner.get_database(db_name)
            return True
        except DatabaseNotExistException:
            return False

    def _has_table(self, ident: Identifier) -> bool:
        paimon_ident = _to_paimon_ident_str(ident)
        try:
            self._inner.get_table(paimon_ident)
            return True
        except (TableNotExistException, DatabaseNotExistException):
            return False

    ###
    # get_*
    ###

    def _get_table(self, ident: Identifier) -> PaimonTable:
        paimon_ident = _to_paimon_ident_str(ident)
        try:
            inner = self._inner.get_table(paimon_ident)
            return PaimonTable._from_obj(inner)
        except TableNotExistException as ex:
            raise NotFoundError() from ex

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        # REST catalog: use native list_databases() (auto-paginated).
        if self._supports_listing():
            databases: list[str] = self._inner.list_databases()
            return [Identifier(db) for db in databases if pattern is None or db.startswith(pattern)]

        # FileSystemCatalog: use file_io — works for local, OSS, S3, and HDFS.
        file_io = getattr(self._inner, "file_io", None)
        warehouse = getattr(self._inner, "warehouse", None)
        if file_io is not None and warehouse is not None:
            return _list_namespaces_via_file_io(file_io, warehouse, pattern)

        raise NotImplementedError("Listing namespaces is not supported for this Paimon catalog type.")

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        # REST catalog: iterate databases then call list_tables(db) for each.
        if self._supports_listing():
            result = []
            for db in self._inner.list_databases():
                for table_name in self._inner.list_tables(db):
                    ident = Identifier(db, table_name)
                    if pattern is None or str(ident).startswith(pattern):
                        result.append(ident)
            return result

        # FileSystemCatalog: use file_io — works for local, OSS, S3, and HDFS.
        file_io = getattr(self._inner, "file_io", None)
        warehouse = getattr(self._inner, "warehouse", None)
        if file_io is not None and warehouse is not None:
            return _list_tables_via_file_io(file_io, warehouse, pattern)

        raise NotImplementedError("Listing tables is not supported for this Paimon catalog type.")


# ------------------------------------------------------------------
# FileSystemCatalog listing via FileIO (local + remote object stores)
# ------------------------------------------------------------------


def _list_namespaces_via_file_io(file_io: Any, warehouse: str, pattern: str | None) -> list[Identifier]:
    from pypaimon.catalog.catalog import Catalog as PaimonBaseCatalog
    from pypaimon.catalog.filesystem_catalog import FileSystemCatalog as PaimonFSCatalog

    db_suffix = PaimonBaseCatalog.DB_SUFFIX  # ".db"
    warehouse_path = PaimonFSCatalog._trim_schema(warehouse)

    namespaces = []
    for info in file_io.list_directories(warehouse_path):
        if not info.base_name.endswith(db_suffix):
            continue
        db_name = info.base_name[: -len(db_suffix)]
        if pattern is None or db_name.startswith(pattern):
            namespaces.append(Identifier(db_name))
    return namespaces


def _list_tables_via_file_io(file_io: Any, warehouse: str, pattern: str | None) -> list[Identifier]:
    from pypaimon.catalog.catalog import Catalog as PaimonBaseCatalog
    from pypaimon.catalog.filesystem_catalog import FileSystemCatalog as PaimonFSCatalog

    db_suffix = PaimonBaseCatalog.DB_SUFFIX  # ".db"
    warehouse_path = PaimonFSCatalog._trim_schema(warehouse)

    tables: list[Identifier] = []
    for db_info in file_io.list_directories(warehouse_path):
        if not db_info.base_name.endswith(db_suffix):
            continue
        db_name = db_info.base_name[: -len(db_suffix)]
        db_path = Path(db_info.path)
        for table_info in file_io.list_directories(db_path):
            # A valid Paimon table directory contains a 'schema' subdirectory.
            schema_path = Path(table_info.path) / "schema"
            if not file_io.exists(schema_path):
                continue
            ident = Identifier(db_name, table_info.base_name)
            if pattern is None or str(ident).startswith(pattern):
                tables.append(ident)
    return tables


# ------------------------------------------------------------------
# PaimonTable
# ------------------------------------------------------------------


class PaimonTable(Table):
    _inner: InnerTable

    def __init__(self) -> None:
        raise RuntimeError("PaimonTable.__init__ is not supported, please use `Table.from_paimon` instead.")

    @staticmethod
    def _from_obj(obj: object) -> PaimonTable:
        """Returns a PaimonTable instance if the given object can be adapted so."""
        if isinstance(obj, InnerTable):
            t = PaimonTable.__new__(PaimonTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported paimon table type: {type(obj)}")

    @property
    def name(self) -> str:
        identifier = self._inner.identifier
        return identifier.object

    @property
    def primary_keys(self) -> list[str]:
        """Returns the primary key columns of this table."""
        return list(self._inner.primary_keys)

    @property
    def partition_keys(self) -> list[str]:
        """Returns the partition key columns of this table."""
        return list(self._inner.partition_keys)

    @property
    def is_primary_key_table(self) -> bool:
        """Returns True if this is a primary key table (merge-on-read)."""
        return self._inner.is_primary_key_table

    @property
    def bucket_count(self) -> int:
        """Returns the number of buckets for this table."""
        return self._inner.total_buckets

    @property
    def table_options(self) -> dict[str, str]:
        """Returns the table options/configuration."""
        return dict(self._inner.options)

    def schema(self) -> Schema:
        return self.read().schema()

    def read(self, **options: Any) -> DataFrame:
        Table._validate_options("Paimon read", options, set())
        from daft.io.paimon._paimon import read_paimon

        return read_paimon(self._inner)

    def append(self, df: DataFrame, **options: Any) -> None:
        Table._validate_options("Paimon write", options, set())
        df.write_paimon(self._inner, mode="append")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        Table._validate_options("Paimon write", options, set())
        df.write_paimon(self._inner, mode="overwrite")


# ------------------------------------------------------------------
# Identifier conversion helpers
# ------------------------------------------------------------------


def _to_paimon_db_name(ident: Identifier) -> str:
    """Extract the Paimon database name from a Daft Identifier.

    Always uses the last part, stripping any leading catalog prefix.
    """
    return tuple(ident)[-1]


def _to_paimon_ident_str(ident: Identifier) -> str:
    """Convert a Daft Identifier to a pypaimon 'db.table' string.

    - 1 part  (table,)              → 'default.table'
    - 2 parts (db, table)           → 'db.table'
    - 3 parts (catalog, db, table)  → 'db.table'  (catalog prefix stripped)
    """
    parts = tuple(ident)
    if len(parts) == 1:
        from pypaimon.catalog.catalog import Catalog as PaimonBaseCatalog

        return f"{PaimonBaseCatalog.DEFAULT_DATABASE}.{parts[0]}"
    elif len(parts) == 2:
        return f"{parts[0]}.{parts[1]}"
    elif len(parts) == 3:
        return f"{parts[1]}.{parts[2]}"
    else:
        raise ValueError(f"Paimon identifier must have 1–3 parts, got {len(parts)}: {ident}")


def _cast_large_types(pa_schema: Any) -> Any:
    """Cast large_string → string and large_binary → binary in a PyArrow schema."""
    from daft.io.paimon._utils import convert_arrow_schema_for_paimon

    return convert_arrow_schema_for_paimon(pa_schema)
