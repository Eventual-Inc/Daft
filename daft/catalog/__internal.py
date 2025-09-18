from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.catalog import Catalog, Identifier, Properties, Table
from daft.daft import PyCatalog as _PyCatalog
from daft.daft import PyTable as _PyTable
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.schema import Schema

if TYPE_CHECKING:
    from daft.io.partitioning import PartitionField


class _RustCatalog(Catalog):
    """Shim to wrap PyCatalog and subclass Catalog.

    This should not be used directly, but instead be subclassed for each Rust catalog.
    """

    def __init__(self, inner: _PyCatalog):
        self.inner = inner

    @property
    def name(self) -> str:
        return self.inner.name()

    def _create_namespace(self, ident: Identifier) -> None:
        self.inner.create_namespace(ident._ident)

    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        return self.inner.create_table(ident._ident, schema._schema)

    def _drop_namespace(self, ident: Identifier) -> None:
        self.inner.drop_namespace(ident._ident)

    def _drop_table(self, ident: Identifier) -> None:
        self.inner.drop_table(ident._ident)

    def _get_table(self, ident: Identifier) -> Table:
        return self.inner.get_table(ident._ident)

    def _has_namespace(self, ident: Identifier) -> bool:
        return self.inner.has_namespace(ident._ident)

    def _has_table(self, ident: Identifier) -> bool:
        return self.inner.has_table(ident._ident)

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        return [Identifier._from_pyidentifier(ident) for ident in self.inner.list_namespaces(pattern)]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        return [Identifier._from_pyidentifier(ident) for ident in self.inner.list_tables(pattern)]


class _RustTable(Table):
    """Shim to wrap PyTable and subclass Table.

    This should not be used directly, but instead be subclassed for each Rust table.
    """

    def __init__(self, inner: _PyTable):
        self.inner = inner

    @property
    def name(self) -> str:
        return self.inner.name()

    def schema(self) -> Schema:
        return Schema._from_pyschema(self.inner.schema())

    def read(self, **options: Any) -> DataFrame:
        return DataFrame(LogicalPlanBuilder(self.inner.to_logical_plan()))

    def append(self, df: DataFrame, **options: Any) -> None:
        self.inner.append(df._builder._builder, **options)

    def overwrite(self, df: DataFrame, **options: Any) -> None:
        self.inner.overwrite(df._builder._builder, **options)


class View(_RustTable):
    pass


class MemoryCatalog(_RustCatalog):
    @staticmethod
    def _new(name: str) -> Catalog:
        return _PyCatalog.new_memory_catalog(name)


class MemoryTable(_RustTable):
    @staticmethod
    def _new(name: str, schema: Schema) -> Table:
        return _PyTable.new_memory_table(name, schema._schema)
