"""The `daft.catalog` module contains functionality for Catalogs.

A Catalog can be understood as a system/service for users to discover, access and query their data.
Most commonly, users' data is represented as a "table". Some more modern Catalogs such as Unity Catalog
also expose other types of data including files, ML models, registered functions and more.

Examples of Catalogs include AWS Glue, Hive Metastore, Apache Iceberg REST and Unity Catalog.

**Catalog**

Daft recognizes a default catalog which it will attempt to use when no specific catalog name is provided.

```python
# This will hit the default catalog
daft.read_table("my_db.my_namespace.my_table")
```

**Named Tables**

Daft allows also the registration of temporary tables, which have no catalog associated with them.

Note that temporary tables take precedence over catalog tables when resolving unqualified names.

```python
df = daft.from_pydict({"foo": [1, 2, 3]})

# TODO deprecated catalog APIs #3819
daft.catalog.register_table(
    "my_table",
    df,
)

# Your table is now accessible from Daft-SQL, or Daft's `read_table`
df1 = daft.read_table("my_table")
df2 = daft.sql("SELECT * FROM my_table")
```
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from daft.daft import catalog as native_catalog
from daft.daft import PyIdentifier
from daft.logical.builder import LogicalPlanBuilder

from daft.dataframe import DataFrame

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.dataframe.dataframe import ColumnInputType


__all__ = [
    "Catalog",
    "Identifier",
    "Table",
    # TODO deprecated catalog APIs #3819
    "read_table",
    "register_python_catalog",
    "register_table",
    "unregister_catalog",
]

# TODO deprecated catalog APIs #3819
unregister_catalog = native_catalog.unregister_catalog


# TODO deprecated catalog APIs #3819
def read_table(name: str) -> DataFrame:
    """Finds a table with the specified name and reads it as a DataFrame.

    The provided name can be any of the following, and Daft will return them with the following order of priority:

    1. Name of a registered dataframe/SQL view (manually registered using `daft.register_table`): `"my_registered_table"`
    2. Name of a table within the default catalog (without inputting the catalog name) for example: `"my.table.name"`
    3. Name of a fully-qualified table path with the catalog name for example: `"my_catalog.my.table.name"`

    Args:
        name: The identifier for the table to read

    Returns:
        A DataFrame containing the data from the specified table.
    """
    native_logical_plan_builder = native_catalog.read_table(name)
    return DataFrame(LogicalPlanBuilder(native_logical_plan_builder))


# TODO deprecated catalog APIs #3819
def register_table(name: str, dataframe: DataFrame) -> str:
    """Register a DataFrame as a named table.

    This function registers a DataFrame as a named table, making it accessible
    via Daft-SQL or Daft's `read_table` function.

    Args:
        name (str): The name to register the table under.
        dataframe (daft.DataFrame): The DataFrame to register as a table.

    Returns:
        str: The name of the registered table.

    Example:
        >>> df = daft.from_pydict({"foo": [1, 2, 3]})
        >>> daft.catalog.register_table("my_table", df)
        >>> daft.read_table("my_table")
    """
    return native_catalog.register_table(name, dataframe._builder._builder)


# TODO deprecated catalog APIs #3819
def register_python_catalog(catalog: object, name: str | None = None) -> str:
    """Registers a Python catalog with Daft.

    Currently supports:

    * [PyIceberg Catalogs](https://py.iceberg.apache.org/api/)
    * [Unity Catalog](https://www.getdaft.io/projects/docs/en/latest/user_guide/integrations/unity-catalog.html)

    Args:
        catalog (PyIcebergCatalog | UnityCatalog): The Python catalog to register.
        name (str | None, optional): The name to register the catalog under. If None, this catalog is registered as the default catalog.

    Returns:
        str: The name of the registered catalog.

    Raises:
        ValueError: If an unsupported catalog type is provided.

    Example:
        >>> from pyiceberg.catalog import load_catalog
        >>> catalog = load_catalog("my_catalog")
        >>> daft.catalog.register_python_catalog(catalog, "my_daft_catalog")

    """
    if (c := Catalog._try_from(catalog)) is not None:
        return native_catalog.register_python_catalog(c, name)
    raise ValueError(f"Unsupported catalog type: {type(catalog)}")


class Catalog(ABC):
    """Interface for python catalog implementations."""

    @property
    def inner(self) -> object | None:
        """Returns the inner catalog object if this is an adapter."""

    @staticmethod
    def _try_from(obj: object) -> Catalog | None:
        for factory in (Catalog._try_from_iceberg, Catalog._try_from_unity):
            if (c := factory(obj)) is not None:
                return c
        return None

    @staticmethod
    def _try_from_iceberg(obj: object) -> Catalog | None:
        """Returns a Daft Catalog instance from an Iceberg catalog."""
        try:
            from daft.catalog.__iceberg import IcebergCatalog

            return IcebergCatalog._try_from(obj)
        except ImportError:
            return None

    @staticmethod
    def _try_from_unity(obj: object) -> Catalog | None:
        """Returns a Daft Catalog instance from a Unity catalog."""
        try:
            from daft.catalog.__unity import UnityCatalog

            return UnityCatalog._try_from(obj)
        except ImportError:
            return None

    ###
    # list_*
    ###

    @abstractmethod
    def list_tables(self, pattern: str | None = None) -> list[str]: ...

    ###
    # get_*
    ###

    @abstractmethod
    def get_table(self, name: str) -> Table: ...

    # TODO deprecated catalog APIs #3819
    def load_table(self, name: str) -> Table:
        """DEPRECATED: Please use get_table(name: str)."""
        return self.get_table(name)


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


class Table(ABC):
    """Interface for python table implementations."""

    @property
    def inner(self) -> object | None:
        """Returns the inner table object if this is an adapter."""

    # TODO deprecated catalog APIs #3819
    def to_dataframe(self) -> DataFrame:
        """DEPRECATED: Please use `read()`."""
        return self.read()

    @abstractmethod
    def read(self) -> DataFrame:
        """Returns a DataFrame from this table."""

    def select(self, *columns: ColumnInputType) -> DataFrame:
        """Returns a DataFrame from this table with the selected columns."""
        return self.read().select(*columns)

    def show(self, n: int = 8) -> None:
        """Shows the first n rows from this table."""
        self.read().show(n)
