"""An in-memory implementation for the daft catalog abstractions."""

from __future__ import annotations

from typing import Any

from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table
from daft.dataframe.dataframe import DataFrame


class MemoryCatalog(Catalog):
    """An in-memory catalog is backed by a dictionary."""

    _name: str
    _tables: dict[str, Table]

    def __init__(self, name: str, tables: list[Table] = []):
        self._name = name
        self._tables = {t.name: t for t in tables}

    @property
    def name(self) -> str:
        return self._name

    @staticmethod
    def _from_pydict(name: str, tables: dict[str, object]) -> MemoryCatalog:
        mem = MemoryCatalog(name)
        for path, source in tables.items():
            ident = Identifier.from_str(path)
            table = MemoryTable._from_pyobject(str(ident[-1]), source)
            mem._tables[path] = table
        return mem

    ###
    # create_*
    ###

    def _create_namespace(self, identifier: Identifier) -> None:
        raise NotImplementedError("Memory create_namespace not yet supported.")

    def _create_table(
        self,
        identifier: Identifier,
        schema: Schema,
        properties: Properties | None = None,
    ) -> Table:
        raise NotImplementedError("Memory create_table not yet supported.")

    ###
    # drop_*
    ###

    def _drop_namespace(self, identifier: Identifier) -> None:
        raise NotImplementedError("Memory drop_namespace not yet supported.")

    def _drop_table(self, identifier: Identifier) -> None:
        raise NotImplementedError("Memory drop_table not yet supported.")

    ###
    # has_*
    ###

    def _has_namespace(self, identifier: Identifier) -> bool:
        prefix = str(identifier)
        for ident in self._tables.keys():
            if ident.startswith(prefix):
                return True
        return False

    def _has_table(self, identifier: Identifier) -> bool:
        return str(identifier) in self._tables

    ###
    # list_*
    ###

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        namespaces = set()
        for path in self._tables.keys():
            if pattern is not None and not path.startswith(pattern):
                continue  # did not match pattern
            split = path.rfind(".")
            if split == -1:
                continue  # does not have a namespace
            namespaces.add(path[:split])
        return [Identifier.from_str(ns) for ns in namespaces]

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is None:
            return [Identifier.from_str(path) for path in self._tables.keys()]
        return [Identifier.from_str(path) for path in self._tables.keys() if path.startswith(pattern)]

    ###
    # get_*
    ###

    def _get_table(self, identifier: Identifier) -> Table:
        path = str(identifier)
        if path not in self._tables:
            raise NotFoundError(f"Table {path} does not exist.")
        return self._tables[path]


class MemoryTable(Table):
    """An in-memory table holds a reference to an existing dataframe."""

    _name: str
    _inner: DataFrame

    def __init__(self, name: str, inner: DataFrame):
        self._name = name
        self._inner = inner

    @property
    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        return self._inner.schema()

    @staticmethod
    def _from_pyobject(name: str, source: object) -> Table:
        """Returns a Daft Table from a supported object type or raises an error."""
        if isinstance(source, Table):
            # we want to rename and create an immutable view from this external table
            return Table.from_df(name, source.read())
        elif isinstance(source, DataFrame):
            return Table.from_df(name, source)
        elif isinstance(source, dict):
            return Table.from_df(name, DataFrame._from_pydict(source))
        else:
            raise ValueError(f"Unsupported table source {type(source)}")

    ###
    # read methods
    ###

    def read(self, **options: Any) -> DataFrame:
        return self._inner

    ###
    # write methods
    ###

    def write(self, df: DataFrame, mode: str = "append", **options: Any) -> None:
        raise NotImplementedError("Writes to in-memory tables are not yet supported.")
