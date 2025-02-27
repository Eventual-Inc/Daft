"""The `daft.catalog` module contains functionality for Catalogs.

A Catalog can be understood as a system/service for users to discover, access and query their data.
Most commonly, users' data is represented as a "table". Some more modern Catalogs such as Unity Catalog
also expose other types of data including files, ML models, registered functions and more.

Examples of Catalogs include AWS Glue, Hive Metastore, Apache Iceberg REST and Unity Catalog.

**Catalog**

```python
# without any qualifiers, the default catalog and namespace are used.
daft.read_table("my_table")

# with a qualified identifier (uses default catalog)
daft.read_table("my_namespace.my_table")

# with a fully qualified identifier
daft.read_table("my_catalog.my_namespace.my_table")
```

**Named Tables**

Daft allows also the registration of temporary tables, which have no catalog associated with them.

Note that temporary tables take precedence over catalog tables when resolving unqualified names.

```python
df = daft.from_pydict({"foo": [1, 2, 3]})

# Attach the DataFrame for use across other APIs.
daft.attach_table(df, "my_table")

# Your table is now accessible from Daft-SQL, or Daft's `read_table`
df1 = daft.read_table("my_table")
df2 = daft.sql("SELECT * FROM my_table")
```
"""

from __future__ import annotations

import warnings

from abc import ABC, abstractmethod
from collections.abc import Sequence
from daft.daft import PyIdentifier, PyTableSource

from daft.dataframe import DataFrame

from typing import TYPE_CHECKING

from daft.logical.schema import Schema

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
def unregister_catalog(catalog_name: str | None) -> bool:
    """Unregisters a catalog from the Daft catalog system.

    DEPRECATED: This is deprecated and will be removed in daft >= 0.5.0; please use `daft.detach_catalog`.

    This function removes a previously registered catalog from the Daft catalog system.

    Args:
        catalog_name (Optional[str]): The name of the catalog to unregister. If None, the default catalog will be unregistered.

    Returns:
        bool: True if a catalog was successfully unregistered, False otherwise.

    Example:
        >>> import daft
        >>> daft.unregister_catalog("my_catalog")
        True
    """
    from daft.session import detach_catalog

    warnings.warn(
        "This is deprecated and will be removed in daft >= 0.5.0; please use `daft.detach_catalog`.",
        category=DeprecationWarning,
    )
    try:
        alias = catalog_name if catalog_name else "default"
        detach_catalog(alias)
        return True
    except Exception:
        return False


# TODO deprecated catalog APIs #3819
def read_table(name: str) -> DataFrame:
    """Finds a table with the specified name and reads it as a DataFrame.

    DEPRECATED: This is deprecated and will be removed in daft >= 0.5.0; please use `daft.read_table`.

    The provided name can be any of the following, and Daft will return them with the following order of priority:

    1. Name of a registered dataframe/SQL view (manually registered using `daft.register_table`): `"my_registered_table"`
    2. Name of a table within the default catalog (without inputting the catalog name) for example: `"my.table.name"`
    3. Name of a fully-qualified table path with the catalog name for example: `"my_catalog.my.table.name"`

    Args:
        name: The identifier for the table to read

    Returns:
        A DataFrame containing the data from the specified table.
    """
    from daft.session import read_table

    warnings.warn(
        "This is deprecated and will be removed in daft >= 0.5.0; please use `daft.read_table`.",
        category=DeprecationWarning,
    )
    return read_table(name)


# TODO deprecated catalog APIs #3819
def register_table(name: str, dataframe: DataFrame) -> str:
    """Register a DataFrame as a named table.

    DEPRECATED: This is deprecated and will be removed in daft >= 0.5.0; please use `daft.attach_table`.

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
    from daft.session import create_temp_table

    warnings.warn(
        "This is deprecated and will be removed in daft >= 0.5.0; please use `daft.create_temp_table`.",
        category=DeprecationWarning,
    )
    _ = create_temp_table(name, dataframe)
    return name


# TODO deprecated catalog APIs #3819
def register_python_catalog(catalog: object, name: str | None = None) -> str:
    """Registers a Python catalog with Daft.

    DEPRECATED: This is deprecated and will be removed in daft >= 0.5.0; please use `daft.attach_catalog`.

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
    from daft.session import attach_catalog

    warnings.warn(
        "This is deprecated and will be removed in daft >= 0.5.0; please use `daft.attach_catalog`.",
        category=DeprecationWarning,
    )
    if name is None:
        name = "default"
    _ = attach_catalog(catalog, name)
    return name


class Catalog(ABC):
    """Interface for python catalog implementations."""

    @staticmethod
    def from_pydict(tables: dict[str, Table]) -> Catalog:
        """Returns an in-memory catalog from the dictionary."""
        from daft.catalog.__memory import MemoryCatalog

        return MemoryCatalog(tables)

    @staticmethod
    def from_iceberg(obj: object) -> Catalog:
        """Returns a Daft Catalog instance from an Iceberg catalog."""
        try:
            from daft.catalog.__iceberg import IcebergCatalog

            return IcebergCatalog._from_obj(obj)
        except ImportError:
            raise ImportError("Iceberg support not installed: pip install -U 'getdaft[iceberg]'")

    @staticmethod
    def from_unity(obj: object) -> Catalog:
        """Returns a Daft Catalog instance from a Unity catalog."""
        try:
            from daft.catalog.__unity import UnityCatalog

            return UnityCatalog._from_obj(obj)
        except ImportError:
            raise ImportError("Unity support not installed: pip install -U 'getdaft[unity]'")

    @staticmethod
    def _from_obj(obj: object) -> Catalog:
        """Returns a Daft Catalog from a supported object type or raises a ValueError."""
        for factory in (Catalog.from_iceberg, Catalog.from_unity):
            try:
                return factory(obj)
            except ValueError:
                pass
            except ImportError:
                pass
        raise ValueError(
            f"Unsupported catalog type: {type(obj)}; please ensure all required extra dependencies are installed."
        )

    ###
    # list_*
    ###

    @abstractmethod
    def list_tables(self, pattern: str | None = None) -> list[str]: ...

    ###
    # get_*
    ###

    @abstractmethod
    def get_table(self, identifier: Identifier | str) -> Table: ...

    # TODO deprecated catalog APIs #3819
    def load_table(self, name: str) -> Table:
        """DEPRECATED: Please use `get_table` instead; version=0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please use `get_table` instead.",
            category=DeprecationWarning,
        )
        return self.get_table(name)


class Identifier(Sequence):
    """A reference (path) to a catalog object.

    Example:
    >>> id = Identifier("a", "b")
    >>> assert len(id) == 2
    """

    _ident: PyIdentifier

    def __init__(self, *parts: str):
        """Creates an Identifier from its parts.

        Example:
        >>> Identifier("namespace", "table")

        Returns:
            Identifier: A new identifier.
        """
        if len(parts) < 1:
            raise ValueError("Identifier requires at least one part.")
        self._ident = PyIdentifier(parts[:-1], parts[-1])

    @staticmethod
    def _from_pyidentifier(ident: PyIdentifier) -> Identifier:
        i = Identifier.__new__(Identifier)
        i._ident = ident
        return i

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
        i._ident = PyIdentifier.from_sql(input, normalize)
        return i

    @staticmethod
    def from_str(input: str) -> Identifier:
        """Parses an Identifier from a dot-delimited Python string without normalization."""
        return Identifier(*input.split("."))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Identifier):
            return False
        return self._ident.eq(other._ident)

    def __getitem__(self, index: int | slice) -> str | Sequence[str]:
        if isinstance(index, slice):
            raise IndexError("slicing not supported")
        if isinstance(index, int):
            return self._ident.getitem(index)

    def __len__(self) -> int:
        return self._ident.__len__()

    def __repr__(self) -> str:
        return f"Identifier('{self._ident.__repr__()}')"

    def __str__(self) -> str:
        return ".".join(self)


class Table(ABC):
    """Interface for python table implementations."""

    @staticmethod
    def from_df(name: str, dataframe: DataFrame) -> Table:
        """Returns a read-only table backed by the DataFrame."""
        from daft.catalog.__memory import MemoryTable

        return MemoryTable(name, dataframe)

    @staticmethod
    def from_iceberg(obj: object) -> Table:
        """Returns a Daft Table instance from an Iceberg table."""
        try:
            from daft.catalog.__iceberg import IcebergTable

            return IcebergTable._from_obj(obj)
        except ImportError:
            raise ImportError("Iceberg support not installed: pip install -U 'getdaft[iceberg]'")

    @staticmethod
    def from_unity(obj: object) -> Table:
        """Returns a Daft Table instance from a Unity table."""
        try:
            from daft.catalog.__unity import UnityTable

            return UnityTable._from_obj(obj)
        except ImportError:
            raise ImportError("Unity support not installed: pip install -U 'getdaft[unity]'")

    @staticmethod
    def _from_obj(obj: object) -> Table:
        """Returns a Daft Table from a supported object type or raises an error."""
        raise ValueError(f"Unsupported table type: {type(obj)}")

    # TODO catalog APIs part 3
    # @property
    # @abstractmethod
    # def name(self) -> str:
    #     """Returns the table name."""

    # TODO catalog APIs part 3
    # @property
    # @abstractmethod
    # def inner(self) -> object | None:
    #     """Returns the inner table object if this is an adapter."""

    @abstractmethod
    def read(self) -> DataFrame:
        """Returns a DataFrame from this table."""

    # TODO deprecated catalog APIs #3819
    def to_dataframe(self) -> DataFrame:
        """DEPRECATED: Please use `read` instead; version 0.5.0!"""
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.5.0, please use `read` instead.",
            category=DeprecationWarning,
        )
        return self.read()

    def select(self, *columns: ColumnInputType) -> DataFrame:
        """Returns a DataFrame from this table with the selected columns."""
        return self.read().select(*columns)

    def show(self, n: int = 8) -> None:
        """Shows the first n rows from this table."""
        return self.read().show(n)


class TableSource:
    """A TableSource is used to create a new table; this could be a Schema or DataFrame."""

    _source: PyTableSource

    def __init__(self) -> None:
        raise ValueError("We do not support creating a TableSource via __init__")

    @staticmethod
    def from_df(df: DataFrame) -> TableSource:
        s = TableSource.__new__(TableSource)
        s._source = PyTableSource.from_builder(df._builder._builder)
        return s

    @staticmethod
    def _from_obj(obj: object = None) -> TableSource:
        # TODO for future sources, consider https://github.com/Eventual-Inc/Daft/pull/2864
        if obj is None:
            return TableSource._from_none()
        elif isinstance(obj, DataFrame):
            return TableSource.from_df(obj)
        elif isinstance(obj, str):
            return TableSource._from_path(obj)
        elif isinstance(obj, Schema):
            return TableSource._from_schema(obj)
        else:
            raise Exception(f"Unknown table source: {obj}")

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
        return TableSource.from_df(DataFrame._from_pylist([]))

    @staticmethod
    def _from_path(path: str) -> TableSource:
        # for supporting daft.create_table("t", "/path/to/data") <-> CREATE TABLE t AS '/path/to/my.data'
        raise NotImplementedError("creating a table source from a path is not yet supported.")
