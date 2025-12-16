"""The `daft.catalog` module contains functionality for Catalogs.

A Catalog can be understood as a system/service for users to discover, access and query their data.
Most commonly, users' data is represented as a "table". Some more modern Catalogs such as Unity Catalog
also expose other types of data including files, ML models, registered functions and more.

Examples of Catalogs include AWS Glue, Hive Metastore, Unity Catalog and S3 Tables.

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
from daft.daft import PyIdentifier

from daft.dataframe import DataFrame

from typing import TYPE_CHECKING, Any, Literal, overload

from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.utils import ColumnInputType
    from daft.convert import InputListType
    from daft.io.partitioning import PartitionField


__all__ = [
    "Catalog",
    "Identifier",
    "Schema",
    "Table",
]


Properties = dict[str, Any]


class NotFoundError(Exception):
    """Raised when some catalog object is not able to be found."""


class Catalog(ABC):
    """Interface for Python catalog implementations.

    A Catalog is a service for discovering, accessing, and querying
    tabular and non-tabular data. You can instantiate a Catalog using
    one of the static `from_` methods.

    Examples:
        >>> import daft
        >>> from daft.catalog import Catalog
        >>>
        >>> data = {"users": {"id": [1, 2, 3], "name": ["a", "b", "c"]}}
        >>> catalog = Catalog.from_pydict(data)
        >>> catalog.list_tables()
        ['users']
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the catalog's name."""

    @abstractmethod
    def _create_namespace(self, ident: Identifier) -> None:
        """Create a namespace in the catalog, erroring if the namespace already exists."""

    @abstractmethod
    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        """Create a table in the catalog, erroring if the table already exists."""

    @abstractmethod
    def _drop_namespace(self, ident: Identifier) -> None:
        """Remove a namespace from the catalog, erroring if the namespace did not exist."""

    @abstractmethod
    def _drop_table(self, ident: Identifier) -> None:
        """Remove a table from the catalog, erroring if the table did not exist."""

    @abstractmethod
    def _get_table(self, ident: Identifier) -> Table:
        """Get a table from the catalog."""

    @abstractmethod
    def _has_namespace(self, ident: Identifier) -> bool:
        """Check if a namespace exists in the catalog."""

    @abstractmethod
    def _has_table(self, ident: Identifier) -> bool:
        """Check if a table exists in the catalog."""

    @abstractmethod
    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List all namespaces in the catalog. When a pattern is specified, list only namespaces matching the pattern."""

    @abstractmethod
    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        """List all tables in the catalog. When a pattern is specified, list only tables matching the pattern."""

    @staticmethod
    def from_pydict(tables: dict[Identifier | str, object], name: str = "default") -> Catalog:
        """Returns an in-memory catalog from a dictionary of table-like objects.

        The table-like objects can be pydicts, dataframes, or a Table implementation.
        For qualified tables, namespaces are created if necessary.

        Args:
            tables (dict[str,object]): a dictionary of table-like objects (pydicts, dataframes, and tables)

        Returns:
            Catalog: new catalog instance with name 'default'

        Examples:
            >>> import daft
            >>> from daft.catalog import Catalog, Table
            >>>
            >>> dictionary = {"x": [1, 2, 3]}
            >>> dataframe = daft.from_pydict(dictionary)
            >>> table = Table.from_df("temp", dataframe)
            >>>
            >>> catalog = Catalog.from_pydict(
            ...     {
            ...         "R": dictionary,
            ...         "S": dataframe,
            ...         "T": table,
            ...     }
            ... )
            >>> catalog.list_tables()
            ['R', 'S', 'T']

        """
        from daft.catalog.__internal import MemoryCatalog

        catalog = MemoryCatalog._new(name)

        for ident, source in tables.items():
            ident = ident if isinstance(ident, Identifier) else Identifier.from_str(ident)

            # has namespace, create one if it doesn't exist
            if len(ident) > 1:
                namespace = Identifier(*ident[:-1])
                catalog.create_namespace_if_not_exists(namespace)

            df: DataFrame
            if isinstance(source, Table):
                df = source.read()
            elif isinstance(source, DataFrame):
                df = source
            elif isinstance(source, dict):
                df = DataFrame._from_pydict(source)
            else:
                raise ValueError(f"Unsupported table source {type(source)}")

            catalog.create_table(ident, df)

        return catalog

    @staticmethod
    def from_iceberg(catalog: object) -> Catalog:
        """Create a Daft Catalog from a PyIceberg catalog object.

        Args:
            catalog (object): a PyIceberg catalog instance

        Returns:
            Catalog: a new Catalog instance backed by the PyIceberg catalog.

        Examples:
            >>> from pyiceberg.catalog import load_catalog
            >>> iceberg_catalog = load_catalog("my_iceberg_catalog")
            >>> catalog = Catalog.from_iceberg(iceberg_catalog)
        """
        try:
            from daft.catalog.__iceberg import IcebergCatalog

            return IcebergCatalog._from_obj(catalog)
        except ImportError:
            raise ImportError("Iceberg support not installed: pip install -U 'daft[iceberg]'")

    @staticmethod
    def from_unity(catalog: object) -> Catalog:
        """Create a Daft Catalog from a Unity Catalog client.

        Args:
            catalog (object): a Unity Catalog client instance

        Returns:
            Catalog: a new Catalog instance backed by the Unity catalog.

        Examples:
            >>> from unity_sdk import UnityCatalogClient
            >>> unity_client = UnityCatalogClient(...)
            >>> catalog = Catalog.from_unity(unity_client)

        """
        try:
            from daft.catalog.__unity import UnityCatalog

            return UnityCatalog._from_obj(catalog)
        except ImportError:
            raise ImportError("Unity support not installed: pip install -U 'daft[unity]'")

    @staticmethod
    def from_gravitino(catalog: object) -> Catalog:
        """Create a Daft Catalog from a Gravitino client.

        Args:
            catalog (object): a Gravitino client instance

        Returns:
            Catalog: a new Catalog instance backed by the Gravitino catalog.

        Examples:
            >>> from daft.gravitino import GravitinoClient
            >>> gravitino_client = GravitinoClient(...)
            >>> catalog = Catalog.from_gravitino(gravitino_client)

        """
        try:
            from daft.catalog.__gravitino import GravitinoCatalog

            return GravitinoCatalog._from_obj(catalog)
        except ImportError:
            raise ImportError("Gravitino support not installed: pip install -U 'daft[gravitino]'")

    @staticmethod
    def from_s3tables(
        table_bucket_arn: str,
        client: object | None = None,
        session: object | None = None,
    ) -> Catalog:
        """Creates a Daft Catalog from S3 Tables bucket ARN, with optional client or session.

        If neither a boto3 client nor session is provided, the Iceberg REST
        client will be used under the hood.

        Args:
            table_bucket_arn (str): ARN of the S3 Tables bucket
            client (object, optional): a boto3 client
            session (object, optional): a boto3 session

        Returns:
            Catalog: a new Catalog instance backed by S3 Tables.

        Examples:
            >>> arn = "arn:aws:s3:::my-s3tables-bucket"
            >>> catalog = Catalog.from_s3tables(arn)
            >>> catalog.list_tables()
        """
        try:
            from daft.catalog.__s3tables import S3Catalog

            if client is not None and session is not None:
                raise ValueError("Can provide either a client or session but not both.")
            elif client is not None:
                return S3Catalog.from_client(table_bucket_arn, client)
            elif session is not None:
                return S3Catalog.from_session(table_bucket_arn, session)
            else:
                return S3Catalog.from_arn(table_bucket_arn)
        except ImportError:
            raise ImportError("S3 Tables support not installed: pip install -U 'daft[aws]'")

    @staticmethod
    def from_glue(
        name: str,
        client: object | None = None,
        session: object | None = None,
    ) -> Catalog:
        """Creates a Daft Catalog backed by the AWS Glue service, with optional client or session.

        Terms:
            - AWS Glue          -> Daft Catalog
            - AWS Glue Database -> Daft Namespace
            - AWS Glue Table    -> Daft Table

        Args:
            name (str): glue database name
            client: optional boto3 client
            session: optional boto3 session

        Returns:
            Catalog: new daft catalog instance backed by AWS Glue.
        """
        try:
            from daft.catalog.__glue import GlueCatalog

            if client is not None and session is not None:
                raise ValueError("Can provide either a client or session but not both.")
            elif client is not None:
                return GlueCatalog.from_client(name, client)
            elif session is not None:
                return GlueCatalog.from_session(name, session)
            else:
                raise ValueError("Must provide either a client or session.")
        except ImportError:
            raise ImportError("AWS Glue support not installed: pip install -U 'daft[aws]'")

    @staticmethod
    def from_postgres(connection_string: str, extensions: list[str] | None = ["vector"]) -> Catalog:
        """Create a Daft Catalog from a PostgreSQL connection string.

        Args:
            connection_string (str): a PostgreSQL connection string
            extensions (list[str], optional): List of PostgreSQL extensions to create if they don't exist.
                For each extension, "CREATE EXTENSION IF NOT EXISTS <extension>" will be executed.
                Defaults to ["vector"] (pgvector extension, if available).

        Returns:
            Catalog: a new Catalog instance to a PostgreSQL database.

        Warning:
            This features is early in development and will likely experience API changes.

        Examples:
            >>> catalog = Catalog.from_postgres("postgresql://user:password@host:port/database")
            >>> catalog = Catalog.from_postgres(
            ...     "postgresql://user:password@host:port/database", extensions=["vector", "pg_stat_statements"]
            ... )
        """
        try:
            from daft.catalog.__postgres import PostgresCatalog

            return PostgresCatalog.from_uri(connection_string, extensions)
        except ImportError:
            raise ImportError("PostgreSQL support not installed: pip install -U 'daft[postgres]'")

    @staticmethod
    def _from_obj(obj: object) -> Catalog:
        """Returns a Daft Catalog from a supported object type or raises a ValueError."""
        for factory in (Catalog.from_iceberg, Catalog.from_unity, Catalog.from_gravitino):
            try:
                return factory(obj)
            except ValueError:
                pass
            except ImportError:
                pass
        raise ValueError(
            f"Unsupported catalog type: {type(obj)}; please ensure all required extra dependencies are installed."
        )

    @staticmethod
    def _validate_options(method: str, input: dict[str, Any], valid: set[str]) -> None:
        """Validates input options against a set of valid options.

        Args:
            method (str): The method name to include in the error message
            input (dict[str, Any]): The input options dictionary
            valid (set[str]): Set of valid option keys

        Raises:
            ValueError: If any input options are not in the valid set
        """
        invalid_options = set(input.keys()) - valid
        if invalid_options:
            raise ValueError(f"Unsupported option(s) for {method}, found {invalid_options!s} not in {valid!s}")

    ###
    # create_*
    ###

    def create_namespace(self, identifier: Identifier | str) -> None:
        """Creates a namespace in this catalog.

        Args:
            identifier (Identifier | str): namespace identifier
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        self._create_namespace(identifier)

    def create_namespace_if_not_exists(self, identifier: Identifier | str) -> None:
        """Creates a namespace in this catalog if it does not already exist.

        Args:
            identifier (Identifier | str): namespace identifier
        """
        if not self.has_namespace(identifier):
            self.create_namespace(identifier)

    def create_table(
        self,
        identifier: Identifier | str,
        source: Schema | DataFrame,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> Table:
        """Creates a table in this catalog.

        Args:
            identifier (Identifier | str): table identifier
            source (Schema | DataFrame): table source object such as a Schema or DataFrame.

        Returns:
            Table: new table instance.
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)
        schema = source.schema() if isinstance(source, DataFrame) else source

        table = self._create_table(identifier, schema, properties, partition_fields)
        if isinstance(source, DataFrame):
            table.append(source)

        return table

    def create_table_if_not_exists(
        self,
        identifier: Identifier | str,
        source: Schema | DataFrame,
        properties: Properties | None = None,
    ) -> Table:
        """Creates a table in this catalog if it does not already exist.

        Args:
            identifier (Identifier | str): table identifier
            source (Schema | DataFrame): table source object such as a Schema or DataFrame.

        Returns:
            Table: the existing table (if exists) or the new table instance.
        """
        if self.has_table(identifier):
            return self.get_table(identifier)
        else:
            return self.create_table(identifier, source, properties)

    ###
    # has_*
    ###

    def has_namespace(self, identifier: Identifier | str) -> bool:
        """Returns True if the namespace exists, otherwise False."""
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        return self._has_namespace(identifier)

    def has_table(self, identifier: Identifier | str) -> bool:
        """Returns True if the table exists, otherwise False."""
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        return self._has_table(identifier)

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str) -> None:
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        self._drop_namespace(identifier)

    def drop_table(self, identifier: Identifier | str) -> None:
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        self._drop_table(identifier)

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str) -> Table:
        """Get a table by its identifier or raises if the table does not exist.

        Args:
            identifier (Identifier|str): table identifier

        Returns:
            Table: matched table or raises if the table does not exist.
        """
        if isinstance(identifier, str):
            identifier = Identifier.from_str(identifier)

        return self._get_table(identifier)

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List namespaces in the catalog which match the given pattern.

        Args:
            pattern (str): pattern to match such as a namespace prefix

        Returns:
            list[Identifier]: list of namespace identifiers matching the pattern.
        """
        return self._list_namespaces(pattern)

    def list_tables(self, pattern: str | None = None) -> list[Identifier]:
        """List tables in the catalog which match the given pattern.

        Args:
            pattern (str): pattern to match such as a namespace prefix

        Returns:
            list[str]: list of table identifiers matching the pattern.
        """
        return self._list_tables(pattern)

    ###
    # read_*
    ###

    def read_table(self, identifier: Identifier | str, **options: dict[str, Any]) -> DataFrame:
        """Returns the table as a DataFrame or raises an exception if it does not exist."""
        return self.get_table(identifier).read(**options)

    ###
    # write_*
    ###

    def write_table(
        self,
        identifier: Identifier | str,
        df: DataFrame,
        mode: Literal["append", "overwrite"] = "append",
        **options: dict[str, Any],
    ) -> None:
        return self.get_table(identifier).write(df, mode=mode, **options)

    ###
    # python methods
    ###

    def __repr__(self) -> str:
        return f"Catalog('{self.name}')"


class Identifier(Sequence[str]):
    """A reference (path) to a catalog object.

    Examples:
        >>> id = Identifier("a", "b")
        >>> assert len(id) == 2
    """

    _ident: PyIdentifier

    def __init__(self, *parts: str):
        """Creates an Identifier from its parts.

        Examples:
            >>> from daft.catalog import Identifier
            >>> Identifier("namespace", "table")
        """
        if len(parts) < 1:
            raise ValueError("Identifier requires at least one part.")
        self._ident = PyIdentifier(parts)

    @staticmethod
    def _from_pyidentifier(ident: PyIdentifier) -> Identifier:
        i = Identifier.__new__(Identifier)
        i._ident = ident
        return i

    @staticmethod
    def from_sql(input: str, normalize: bool = False) -> Identifier:
        """Parses an Identifier from an SQL string, normalizing to lowercase if specified.

        Args:
            input (str): input sql string
            normalize (bool): flag to case-normalize the identifier text

        Returns:
            Identifier: new identifier instance

        Examples:
            >>> from daft.catalog import Identifier
            >>> Identifier.from_sql("namespace.table") == Identifier("namespace", "table")
            >>> Identifier.from_sql('"a.b"') == Identifier('"a.b."')
            >>> Identifier.from_sql('ABC."xYz"', normalize=True) == Identifier("abc", "xYz")

        """
        i = Identifier.__new__(Identifier)
        i._ident = PyIdentifier.from_sql(input, normalize)
        return i

    @staticmethod
    def from_str(input: str) -> Identifier:
        """Parses an Identifier from a dot-delimited Python string without normalization.

        Args:
            input (str): input identifier string

        Returns:
            Identifier: new identifier instance

        Examples:
            >>> from daft.catalog import Identifier
            >>> Identifier.from_str("namespace.table") == Identifier("namespace", "table")

        """
        return Identifier(*input.split("."))

    def drop(self, n: int = 1) -> Identifier:
        """Returns a new Identifier with the first n parts removed.

        Args:
            n (int): Number of parts to drop from the beginning. Defaults to 1.

        Returns:
            Identifier: A new Identifier with the first n parts removed.

        Raises:
            ValueError: If dropping n parts would result in an empty Identifier.
        """
        if n <= 0:
            return Identifier(*self)
        if n >= len(self):
            raise ValueError(f"Cannot drop {n} parts from Identifier with {len(self)} parts")
        parts = tuple(self)
        return Identifier(*parts[n:])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Identifier):
            return False
        return self._ident.eq(other._ident)

    def __hash__(self) -> int:
        return self._ident.__hash__()

    @overload
    def __getitem__(self, index: int, /) -> str: ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[str]: ...

    def __getitem__(self, index: int | slice, /) -> str | Sequence[str]:
        if isinstance(index, int):
            return self._ident.getitem(index)
        parts = tuple(self)
        return parts[index]

    def __len__(self) -> int:
        return self._ident.__len__()

    def __add__(self, suffix: Identifier) -> Identifier:
        return Identifier(*(tuple(self) + tuple(suffix)))

    def __repr__(self) -> str:
        return f"Identifier('{self._ident.__repr__()}')"

    def __str__(self) -> str:
        return ".".join(self)


class Table(ABC):
    """Interface for python table implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the table's name."""

    @abstractmethod
    def schema(self) -> Schema:
        """Returns the table's schema."""

    @staticmethod
    def from_pydict(name: str, data: dict[str, InputListType]) -> Table:
        """Returns a read-only table backed by the given data.

        Args:
            name (str): table table
            data dict[str,object]: keys are column names and the values are python lists, numpy arrays, or arrow arrays.

        Returns:
            DataFrame: new read-only table instance

        Examples:
            >>> from daft.catalog import Table
            >>> table = Table.from_pydict({"foo": [1, 2]})
            >>> table.show()
            ╭───────╮
            │ foo   │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 1     │
            ├╌╌╌╌╌╌╌┤
            │ 2     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        df = DataFrame._from_pydict(data)
        return Table.from_df(name, df)

    @staticmethod
    def from_df(name: str, dataframe: DataFrame) -> Table:
        """Returns a read-only table backed by the DataFrame.

        Args:
            name (str): table name
            dataframe (DataFrame): table source dataframe

        Returns:
            Table: new table instance

        Examples:
            >>> import daft
            >>> from daft.catalog import Table
            >>> Table.from_df("my_table", daft.from_pydict({"x": [1, 2, 3]}))

        """
        from daft.catalog.__internal import MemoryTable

        table = MemoryTable._new(name, dataframe.schema())
        table.append(dataframe)

        return table

    @staticmethod
    def from_iceberg(table: object) -> Table:
        """Creates a Daft Table instance from an Iceberg table.

        Args:
            table (object): a pyiceberg table

        Returns:
            Table: new daft table instance
        """
        try:
            from daft.catalog.__iceberg import IcebergTable

            return IcebergTable._from_obj(table)
        except ImportError:
            raise ImportError("Iceberg support not installed: pip install -U 'daft[iceberg]'")

    @staticmethod
    def from_unity(table: object) -> Table:
        """Returns a Daft Table instance from a Unity table.

        Args:
            table (object): unity table instance.
        """
        try:
            from daft.catalog.__unity import UnityTable

            return UnityTable._from_obj(table)
        except ImportError:
            raise ImportError("Unity support not installed: pip install -U 'daft[unity]'")

    @staticmethod
    def from_gravitino(table: object) -> Table:
        """Returns a Daft Table instance from a Gravitino table.

        Args:
            table (object): gravitino table instance.
        """
        try:
            from daft.catalog.__gravitino import GravitinoTable

            return GravitinoTable._from_obj(table)
        except ImportError:
            raise ImportError("Gravitino support not installed: pip install -U 'daft[gravitino]'")

    @staticmethod
    def _from_obj(obj: object) -> Table:
        for factory in (Table.from_iceberg, Table.from_unity, Table.from_gravitino):
            try:
                return factory(obj)
            except ValueError:
                pass
            except ImportError:
                pass
        raise ValueError(
            f"Unsupported table type: {type(obj)}; please ensure all required extra dependencies are installed."
        )

    @staticmethod
    def _validate_options(method: str, input: dict[str, Any], valid: set[str]) -> None:
        """Validates input options against a set of valid options.

        Args:
            method (str): The method name to include in the error message
            input (dict[str, Any]): The input options dictionary
            valid (set[str]): Set of valid option keys

        Raises:
            ValueError: If any input options are not in the valid set
        """
        invalid_options = set(input.keys()) - valid
        if invalid_options:
            raise ValueError(f"Unsupported option(s) for {method}, found {invalid_options!s} not in {valid!s}")

    ###
    # read methods
    ###

    @abstractmethod
    def read(self, **options: Any) -> DataFrame:
        """Creates a new DataFrame from this table.

        Args:
            **options (Any): additional format-dependent read options

        Returns:
            DataFrame: new DataFrame instance
        """

    def select(self, *columns: ColumnInputType) -> DataFrame:
        """Creates a new DataFrame from the table applying the provided expressions.

        Args:
            *columns (Expression|str): columns to select from the current DataFrame

        Returns:
            DataFrame: new DataFrame instance with the select columns
        """
        return self.read().select(*columns)

    def show(self, n: int = 8) -> None:
        """Shows the first n rows from this table.

        Args:
            n (int): number of rows to show

        Returns:
            None
        """
        return self.read().show(n)

    ###
    # write methods
    ###

    def write(self, df: DataFrame, mode: Literal["append", "overwrite"] = "append", **options: Any) -> None:
        """Writes the DataFrame to this table.

        Args:
            df (DataFrame): datafram to write
            mode (str): write mode such as 'append' or 'overwrite'
            **options (Any): additional format-dependent write options
        """
        if mode == "append":
            return self.append(df, **options)
        else:
            return self.overwrite(df, **options)

    @abstractmethod
    def append(self, df: DataFrame, **options: Any) -> None:
        """Appends the DataFrame to this table.

        Args:
            df (DataFrame): dataframe to append
            **options (Any): additional format-dependent write options
        """

    @abstractmethod
    def overwrite(self, df: DataFrame, **options: Any) -> None:
        """Overwrites this table with the given DataFrame.

        Args:
            df (DataFrame): dataframe to overwrite this table with
            **options (Any): additional format-dependent write options
        """

    ###
    # python methods
    ###

    def __repr__(self) -> str:
        return f"Table('{self.name}')"
