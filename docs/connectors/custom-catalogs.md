# Custom Catalogs

Daft ships with built-in catalog integrations for [Iceberg](iceberg.md), [Unity](unity_catalog.md), [AWS Glue](glue.md), and others. But if your tables live behind a proprietary metadata service, an internal API, or a convention like "every subdirectory in S3 is a Delta table," you can implement your own catalog and plug it into Daft. This also applies if you use a public catalog system that Daft doesn't have a built-in integration for yet - you can bridge the gap yourself rather than waiting for native support!

The `Catalog` and `Table` abstract base classes in `daft.catalog` are public interfaces designed for this. Subclass them to teach Daft how to discover and read your tables. Once registered, your catalog works everywhere - the Python API, SQL queries, and Sessions all resolve tables through it. Tables are resolved lazily, so you can expose thousands of tables without paying the cost of eagerly constructing DataFrames for all of them upfront.

## Overview

Daft's catalog system has two core interfaces:

- **`Catalog`** - discovers and manages tables and namespaces
- **`Table`** - reads and writes data for a single table

Both are abstract base classes. You implement the private `_`-prefixed abstract methods, and the base classes provide the public API (argument normalization, `if_not_exists` variants, etc.) on top.

Once implemented, custom catalogs work with Sessions and SQL just like the built-in ones:

```python
import daft
from daft.session import Session

sess = Session()
sess.attach(my_catalog)
sess.sql("SELECT * FROM my_namespace.my_table").show()
```

## Catalog Interface

A `Catalog` subclass implements these abstract methods:

| Method | Description |
|---|---|
| `name` (property) | Returns the catalog's name |
| `_get_table(ident)` | Look up a table by identifier |
| `_has_table(ident)` | Check if a table exists |
| `_list_tables(pattern)` | List tables, optionally filtered by pattern |
| `_has_namespace(ident)` | Check if a namespace exists |
| `_list_namespaces(pattern)` | List namespaces, optionally filtered by pattern |
| `_create_namespace(ident)` | Create a namespace |
| `_create_table(ident, schema, ...)` | Create a table |
| `_drop_namespace(ident)` | Drop a namespace |
| `_drop_table(ident)` | Drop a table |

The base class provides the public methods (`get_table`, `list_tables`, `create_table_if_not_exists`, `read_table`, `write_table`, etc.) on top of these.

## Table Interface

A `Table` subclass implements these abstract methods:

| Method | Description |
|---|---|
| `name` (property) | Returns the table's name |
| `schema()` | Returns the table's `Schema` |
| `read(**options)` | Returns a `DataFrame` for this table |
| `append(df, **options)` | Appends a DataFrame to this table |
| `overwrite(df, **options)` | Overwrites this table with a DataFrame |

The base class provides `write(df, mode)`, `select(*columns)`, and `show(n)` on top of these.

## Walkthrough: Iceberg Catalog Implementation

Daft's [Iceberg Catalog integration](https://github.com/Eventual-Inc/Daft/blob/main/daft/catalog/__iceberg.py) is a complete built-in implementation that covers every abstract method. The snippets below are trimmed to highlight the pattern - see the full source for details.

### IcebergCatalog

The catalog wraps a PyIceberg `Catalog` client. Each abstract method converts between Daft's `Identifier` type and PyIceberg's tuple-based identifiers, then delegates to the inner client:

```python
from daft.catalog import Catalog, Identifier, NotFoundError, Properties, Schema, Table

class IcebergCatalog(Catalog):
    _inner: InnerCatalog  # pyiceberg.catalog.Catalog

    @property
    def name(self) -> str:
        return self._inner.name

    def _get_table(self, identifier: Identifier) -> IcebergTable:
        ident = tuple(identifier)  # Identifier -> tuple for PyIceberg
        try:
            return IcebergTable._from_obj(self._inner.load_table(ident))
        except NoSuchTableError as ex:
            raise NotFoundError() from ex

    def _has_table(self, identifier: Identifier) -> bool:
        try:
            self._inner.load_table(tuple(identifier))
            return True
        except NoSuchTableError:
            return False

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        if pattern is None:
            tables = []
            for ns in self.list_namespaces():
                tables.extend(self._inner.list_tables(str(ns)))
        else:
            tables = self._inner.list_tables(pattern)
        return [Identifier(*tup) for tup in tables]

    def _has_namespace(self, identifier: Identifier) -> bool:
        try:
            self._inner.list_namespaces(tuple(identifier))
            return True
        except NoSuchNamespaceError:
            return False

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        prefix = () if pattern is None else tuple(pattern)
        return [Identifier(*tup) for tup in self._inner.list_namespaces(prefix)]

    def _create_namespace(self, identifier: Identifier) -> None:
        self._inner.create_namespace(tuple(identifier))

    def _create_table(self, identifier, schema, properties=None, partition_fields=None):
        ident = tuple(identifier)
        iceberg_schema = ...  # convert Daft Schema -> PyIceberg Schema
        return IcebergTable._from_obj(self._inner.create_table(ident, schema=iceberg_schema))

    def _drop_namespace(self, identifier: Identifier) -> None:
        self._inner.drop_namespace(tuple(identifier))

    def _drop_table(self, identifier: Identifier) -> None:
        self._inner.drop_table(tuple(identifier))
```

The key pattern: convert `Identifier` to your backend's native format (here, a tuple via `tuple(identifier)`), call the backend, and translate exceptions to `NotFoundError`.

### IcebergTable

The table wraps a PyIceberg `Table` object. `read()` delegates to `daft.read_iceberg` and write methods delegate to `DataFrame.write_iceberg`:

```python
from daft.catalog import Table
from daft.dataframe import DataFrame
from daft.logical.schema import Schema

class IcebergTable(Table):
    _inner: InnerTable  # pyiceberg.table.Table

    @property
    def name(self) -> str:
        return self._inner.name()[-1]

    def schema(self) -> Schema:
        return self.read().schema()

    def read(self, **options) -> DataFrame:
        return daft.read_iceberg(self._inner, **options)

    def append(self, df: DataFrame, **options) -> None:
        df.write_iceberg(self._inner, mode="append")

    def overwrite(self, df: DataFrame, **options) -> None:
        df.write_iceberg(self._inner, mode="overwrite")
```

## Using a Custom Catalog

Custom catalogs integrate with the full Daft API - Sessions, SQL, and the Python API:

```python
import daft
from daft.session import Session

# Create your catalog
catalog = MyCatalog(...)

# Use it directly
catalog.list_tables()
catalog.read_table("my_namespace.my_table")

# Or attach it to a session for SQL support
sess = Session()
sess.attach(catalog)
sess.set_namespace("my_namespace")

# SQL queries resolve tables through your catalog
sess.sql("SELECT * FROM my_table").show()
```

## Reference

For complete API documentation, see the [Catalogs & Tables API reference](../api/catalogs_tables.md).

Other built-in implementations for reference:

- [Unity Catalog](https://github.com/Eventual-Inc/Daft/blob/main/daft/catalog/__unity.py) - read-only, simplest implementation
- [AWS Glue Catalog](https://github.com/Eventual-Inc/Daft/blob/main/daft/catalog/__glue.py) - AWS Glue metadata service
- [PostgreSQL Catalog](https://github.com/Eventual-Inc/Daft/blob/main/daft/catalog/__postgres.py) - full read/write with PostgreSQL
