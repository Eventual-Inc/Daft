"""An in-memory implementation for the daft catalog abstractions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.catalog import Catalog, Identifier, Table, TableSource

if TYPE_CHECKING:
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
            table = Table._from_obj(ident[-1], source)
            mem._tables[path] = table
        return mem

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str):
        raise ValueError("Memory create_namespace not yet supported.")

    def create_table(self, identifier: Identifier | str, source: TableSource) -> Table:
        raise ValueError("Memory create_table not yet supported.")

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        raise ValueError("Memory drop_namespace not yet supported.")

    def drop_table(self, identifier: Identifier | str):
        raise ValueError("Memory drop_table not yet supported.")

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        namespaces = set()
        for path in self._tables.keys():
            if pattern is not None and not path.startswith(pattern):
                continue  # did not match pattern
            split = path.rfind(".")
            if split == -1:
                continue  # does not have a namespace
            namespaces.add(path[:split])
        return [Identifier.from_str(ns) for ns in namespaces]

    def list_tables(self, pattern: str | None = None) -> list[str]:
        if pattern is None:
            return list(self._tables.keys())
        return [path for path in self._tables.keys() if path.startswith(pattern)]

    ###
    # get_*
    ###

    def get_table(self, identifier: str | Identifier) -> Table:
        path = str(identifier)
        if path not in self._tables:
            raise ValueError(f"Table {path} does not exist.")
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

    ###
    # read methods
    ###

    def read(self, **options) -> DataFrame:
        return self._inner

    ###
    # write methods
    ###

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        raise ValueError("Writes to in-memory tables are not yet supported.")
