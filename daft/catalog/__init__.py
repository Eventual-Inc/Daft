"""The daft-catalog moduel documentation..."""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Sequence
from collections.abc import Sequence
from daft.daft import PyCatalog, PyIdentifier
from daft.dataframe import DataFrame
from daft.expressions import Expression
from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition


def load_catalog(name: str, options: object | None = None) -> Catalog:
    """Loads a new catalog from the configuration options or creates an in-memory catalog if none given."""
    if options is None:
        return Catalog._from_none(name)
    else:
        return Catalog._from_some(name, options)


class Catalog(ABC):
    """Catalog documentation..."""

    def __repr__(self) -> str:
        return f"Catalog('{self.name()}')"

    @staticmethod
    def _from_none(name: str):
        from daft.catalog.__temp import TempCatalog

        return TempCatalog(name)

    @staticmethod
    def _from_some(name: str, options: object) -> Catalog:
        return NotImplementedError("catalog from options")

    @abstractmethod
    def name(self) -> str:
        """Returns the catalog name."""

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

    # @abstractmethod
    # def create_table(self, name: str, source: Source = None) -> Table:
    #     """Creates a table scoped to this catalog."""

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
        >>> #

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


# TODO for future sources, consider https://github.com/Eventual-Inc/Daft/pull/2864
# pandas/arrow/arrow_record_batches/pydict
TableSource = Schema | DataFrame | str | None


class Table(ABC):
    """Table documentation..."""

    def __repr__(self) -> str:
        return f"Table('{self._name}')"

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
    def _from_source(name: str, source: TableSource = None) -> Table:
        from daft.catalog.__temp import TempTable
        return TempTable._from_source(name, source)

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
    "load_catalog",
]
