"""The daft-catalog moduel documentation..."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Sequence
from collections.abc import Sequence
from daft.daft import PyIdentifier, PyTableSource
from daft.dataframe import DataFrame
from daft.logical.schema import Schema


class Catalog(ABC):
    """Catalog documentation..."""

    @staticmethod
    def empty() -> Catalog:
        """Returns a new in-memory catalog implementation."""
        from daft.catalog.__memory import MemoryCatalog

        return MemoryCatalog({})

    @staticmethod
    def from_pydict(tables: dict[str, Table]) -> Catalog:
        """Returns a new in-memory catalog implementation with temporary tables."""
        from daft.catalog.__memory import MemoryCatalog

        return MemoryCatalog(tables)

    # TODO UPDATE
    # def from_opts(name: str, options: object | None = None) -> Catalog:
    #     """Loads a new catalog from the configuration options or creates an in-memory catalog if none given."""
    #     if options is None:
    #         return Catalog._from_none(name)
    #     else:
    #         return Catalog._from_some(name, options)

    # @property
    # @abstractmethod
    # def inner(self) -> Catalog | None:
    #     """Returns the inner catalog object for direct access if neccessary."""

    ###
    # create_*
    ###

    # @abstractmethod
    # def create_namespace(self, name: str) -> Namespace:
    #     """Creates a namespace scoped to this catalog."""

    @abstractmethod
    def create_table(self, name: str, source: TableSource | None = None) -> Table:
        """Creates a table scoped to this catalog."""

    ###
    # has_*
    ###

    # @abstractmethod
    # def has_namespace(self, name: str) -> bool:
    #     """Returns true iff this catalog has a namespace with the given name."""

    ###
    # get_*
    ###

    # @abstractmethod
    # def get_namespace(self, name: str | None = None) -> Namespace:
    #     """Returns the given namespace if it exists, otherwise raises an exception."""

    # @abstractmethod
    # def get_table(self, name: str) -> Table:
    #     """Returns the given table if it exists, otherwise raises an exception."""

    ###
    # list_*
    ###

    # @abstractmethod
    # def list_namespaces(self, pattern: str | None = None) -> list[Namespace]:
    #     """Lists all namespaces matching the optional pattern."""

    # @abstractmethod
    # def list_tables(self, pattern: str | None = None) -> list[Table]:
    #     """Lists all tables matching the optional pattern."""

    ###
    # read_*
    ###

    # def read_table(self, name: Identifier) -> DataFrame:
    #     raise NotImplementedError("read_table not implemented")


class Identifier(Sequence):
    """A reference (path) to a catalog object.

    Example:
    >>> id = Identifier("a", "b")
    >>> assert len(id) == 2
    """

    _identifier: PyIdentifier

    def __init__(self, *parts: str):
        """Creates an Identifier from its parts.

        Example:
        >>> Identifier("namespace", "table")

        Returns:
            Identifier: A new identifier.
        """
        if len(parts) < 1:
            raise ValueError("Identifier requires at least one part.")
        self._identifier = PyIdentifier(parts[:-1], parts[-1])

    @staticmethod
    def from_sql(input: str, normalize: bool = False) -> Identifier:
        """Parses an Identifier from an SQL string, normalizing to lowercase if specified.

        Example:
        >>> Identifier.from_sql("namespace.table") == Identifier("namespace", "table")
        >>> Identifier.from_sql('"a.b"') == Identifier('"a.b."')
        >>> Identifier.from_sql('ABC."xYz"', normalize=True) == Identifier("abc", "xYz")

        Returns:
            Identifier: A new identifier.
        """
        i = Identifier.__new__(Identifier)
        i._identifier = PyIdentifier.from_sql(input, normalize)
        return i

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Identifier):
            return False
        return self._identifier.eq(other._identifier)

    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        if isinstance(index, slice):
            raise IndexError("slicing not supported")
        if isinstance(index, int):
            return self._identifier.getitem(index)

    def __len__(self) -> int:
        return self._identifier.__len__()

    def __repr__(self) -> str:
        return f"Identifier('{self._identifier.__repr__()}')"


# TODO make a sequence
Namespace = tuple[str]


class TableSource:
    _source: PyTableSource

    def __init__(self) -> None:
        raise ValueError("We do not support creating a TableSource via __init__")

    @staticmethod
    def _from_object(source: object = None) -> TableSource:
        # TODO for future sources, consider https://github.com/Eventual-Inc/Daft/pull/2864
        if source is None:
            return TableSource._from_none()
        elif isinstance(source, DataFrame):
            return TableSource._from_df(source)
        elif isinstance(source, str):
            return TableSource._from_path(source)
        elif isinstance(source, Schema):
            return TableSource._from_schema(source)
        else:
            raise Exception(f"Unknown table source: {source}")

    @staticmethod
    def _from_none() -> TableSource:
        # for creating temp mutable tables, but we don't have those yet
        # s = TableSource.__new__(TableSource)
        # s._source = PyTableSource.empty()
        # return s
        # todo temp workaround just use an empty schema
        return TableSource._from_schema(Schema._from_fields([]))

    @staticmethod
    def _from_schema(schema: Schema) -> TableSource:
        # we don't have mutable temp tables, so just make an empty view
        # s = TableSource.__new__(TableSource)
        # s._source = PyTableSource.from_schema(schema._schema)
        # return s
        # todo temp workaround until create_table is wired
        return TableSource._from_df(DataFrame._from_pylist([]))

    @staticmethod
    def _from_df(df: DataFrame) -> TableSource:
        s = TableSource.__new__(TableSource)
        s._source = PyTableSource.from_view(df._builder._builder)
        return s

    @staticmethod
    def _from_path(path: str) -> TableSource:
        # for supporting daft.create_table("t", "/path/to/data") <-> CREATE TABLE t AS '/path/to/my.data'
        raise NotImplementedError("creating a table source from a path is not yet supported.")


class Table(ABC):
    """Table documentation..."""

    def __repr__(self) -> str:
        return f"Table('{self.name()}')"

    @abstractmethod
    def name(self) -> str:
        """Returns the table name."""

    @abstractmethod
    def schema(self) -> Schema:
        """Returns the table schema."""

    ###
    # Creation Methods
    ###

    @staticmethod
    def _from_source(name: str, source: TableSource | None = None) -> Table:
        from daft.catalog.__memory import MemoryTable

        return MemoryTable._from_source(name, source)

    ###
    # DataFrame Methods
    ###

    @abstractmethod
    def read(self) -> DataFrame:
        """Returns a DataFrame from this table."""

    @abstractmethod
    def show(self, n: int = 8) -> None:
        """Shows the first n rows from this table."""


__all__ = [
    "Catalog",
    "Identifier",
    "Namespace",
    "Table",
    "TableSource",
]
