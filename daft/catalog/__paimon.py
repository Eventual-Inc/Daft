"""WARNING! These APIs are internal; please use Catalog.from_paimon() and Table.from_paimon()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa  # noqa: TID253
from pypaimon.catalog.catalog import Catalog as InnerCatalog
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)
from pypaimon.table.table import Table as InnerTable

from daft.catalog import Catalog, Function, Identifier, NotFoundError, Properties, Schema, Table

if TYPE_CHECKING:
    from collections.abc import Callable

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

    ###
    # create_*
    ###

    def _create_function(self, ident: Identifier, function: Function | Callable[..., Any]) -> None:
        raise NotImplementedError("Paimon does not support function registration.")

    def _create_namespace(self, ident: Identifier) -> None:
        db_name = _to_paimon_ident(ident)
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
        # Exclude non-option keys so only string-valued table properties are forwarded
        options = {k: str(v) for k, v in (properties or {}).items() if k != "primary_keys"} if properties else {}

        paimon_schema = pypaimon.Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=partition_keys,
            primary_keys=primary_keys,
            options=options,
        )

        paimon_ident = _to_paimon_ident(ident)
        self._inner.create_table(paimon_ident, paimon_schema, ignore_if_exists=False)

        inner_table = self._inner.get_table(paimon_ident)
        return PaimonTable._from_obj(inner_table)

    ###
    # drop_*
    ###

    def _drop_namespace(self, ident: Identifier) -> None:
        db_name = _to_paimon_ident(ident)
        try:
            self._inner.drop_database(db_name, ignore_if_not_exists=False)
        except DatabaseNotExistException as ex:
            raise NotFoundError(f"Namespace '{db_name}' not found.") from ex

    def _drop_table(self, ident: Identifier) -> None:
        paimon_ident = _to_paimon_ident(ident)
        try:
            self._inner.drop_table(paimon_ident, ignore_if_not_exists=False)
        except TableNotExistException as ex:
            raise NotFoundError(f"Table '{paimon_ident}' not found.") from ex

    ###
    # has_*
    ###

    def _has_namespace(self, ident: Identifier) -> bool:
        db_name = _to_paimon_ident(ident)
        try:
            self._inner.get_database(db_name)
            return True
        except DatabaseNotExistException:
            return False

    def _has_table(self, ident: Identifier) -> bool:
        paimon_ident = _to_paimon_ident(ident)
        try:
            self._inner.get_table(paimon_ident)
            return True
        except (TableNotExistException, DatabaseNotExistException):
            return False

    ###
    # get_*
    ###

    def _get_function(self, ident: Identifier) -> Function:
        raise NotFoundError(f"Function '{ident}' not found in catalog '{self.name}'")

    def _get_table(self, ident: Identifier) -> PaimonTable:
        paimon_ident = _to_paimon_ident(ident)
        try:
            inner = self._inner.get_table(paimon_ident)
            return PaimonTable._from_obj(inner)
        except TableNotExistException as ex:
            raise NotFoundError() from ex

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        databases: list[str] = self._inner.list_databases()
        return [Identifier(db) for db in databases if pattern is None or db.startswith(pattern)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        result = []
        for db in self._inner.list_databases():
            for table_name in self._inner.list_tables(db):
                ident = Identifier(db, table_name)
                if pattern is None or str(ident).startswith(pattern):
                    result.append(ident)
        return result


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
        return dict(self._inner.options.options.to_map())

    def schema(self) -> Schema:
        return self.read().schema()

    def read(self, **options: Any) -> DataFrame:
        Table._validate_options("Paimon read", options, set())
        from daft.io.paimon import read_paimon

        return read_paimon(self._inner)

    def append(self, df: DataFrame, **options: Any) -> None:
        Table._validate_options("Paimon write", options, set())
        df.write_paimon(self._inner, mode="append")

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        Table._validate_options("Paimon write", options, set())
        df.write_paimon(self._inner, mode="overwrite")

    def truncate(self) -> None:
        """Remove all data from this table."""
        write_builder = self._inner.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        try:
            table_commit.truncate_table()
        finally:
            table_commit.close()

    def truncate_partitions(self, partitions: list[dict[str, str]]) -> None:
        """Remove data from specific partitions."""
        write_builder = self._inner.new_batch_write_builder()
        table_commit = write_builder.new_commit()
        try:
            table_commit.truncate_partitions(partitions)
        finally:
            table_commit.close()


# ------------------------------------------------------------------
# Identifier conversion helpers
# ------------------------------------------------------------------


def _to_paimon_ident(ident: Identifier) -> str:
    """Convert a Daft identifier to a pypaimon identifier string.

    Strips the leading catalog prefix for 3-part identifiers:
    - 1 part  (table,)              → 'table'
    - 2 parts (db, table)           → 'db.table'
    - 3 parts (catalog, db, table)  → 'db.table'  (catalog prefix stripped)
    """
    if isinstance(ident, Identifier):
        parts = tuple(ident)
        if len(parts) == 3:
            return f"{parts[1]}.{parts[2]}"
        if len(parts) == 2:
            return f"{parts[0]}.{parts[1]}"
        return str(parts[0])
    return ident


def _cast_large_types(arrow_schema: pa.Schema) -> pa.Schema:
    """Convert PyArrow schema to be compatible with pypaimon.

    pypaimon doesn't support large_string, so we convert it to regular string.
    large_binary is kept as-is because pypaimon 1.4+ maps it to the BLOB type.
    """
    new_fields = []
    need_conversion = False

    for field in arrow_schema:
        field_type = field.type
        if pa.types.is_large_string(field_type):
            field_type = pa.string()
            need_conversion = True
        new_fields.append(pa.field(field.name, field_type, nullable=field.nullable, metadata=field.metadata))

    if need_conversion:
        return pa.schema(new_fields, metadata=arrow_schema.metadata)
    return arrow_schema
