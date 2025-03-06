# Catalog Usage Guide

Catalogs are a centralized place to organize and govern your data. It is often responsible for creating objects such as tables and namespaces, managing transactions, and access control. Most importantly, the catalog abstracts away physical storage details, letting you focus on the logical structure of your data without worrying about file formats, partitioning schemes, or storage locations.

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs which make it easy to manage catalog objects (tables and namespaces), while also making it easy to leverage daft's great I/O support for reading and writing open table formats like [Iceberg](integrations/iceberg.md) and [Delta Lake](integrations/delta_lake.md).

These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues?q=is%3Aissue%20state%3Aopen%20label%3Adata-catalogs) for any bugs you may encounter. Thank you! 🤘


## TL;DR

Daft provides several high-level interfaces to simplify accessing your data.

* [Session]() - interface for
* [Catalog](api_docs/catalog.html) - interface for creating and accessing both tables and namespaces.
* [Table](api_docs/catalog.html#daft.Table) -


```python
import daft

"""
When you `import daft` there is an implicit session, `daft.current_session()`

This example shows how to 'attach' an Iceberg catalog and read tables.
"""

# import the catalog classes
from daft import Catalog, Session, Table

# load an iceberg catalog
from pyiceberg.catalog import load_catalog

# suppose our iceberg catalog has a table named 'ns.tbl'
pyiceberg_catalog = load_catalog("...")

# attach to the active session, `daft.current_session()`
daft.attach_catalog(pyiceberg_catalog, alias="tldr")

# read your tables as daft dataframes
df = daft.read_table("ns.tbl")

# use just like any other dataframe!
df.show()
"""
╭─────────┬───────╮
│ a       ┆ b     │
│ ---     ┆ ---   │
│ Boolean ┆ Int64 │
╞═════════╪═══════╡
│ true    ┆ 1     │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ true    ┆ 2     │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ false   ┆ 3     │
╰─────────┴───────╯
"""
```

## Usage

*This section covers detailed usage of the current APIs with some code snippets.*

### Working with Sessions

Sessions hold attached objects, temporary objects, and stateful variables like the `current_catalog` and `current_namespace`.
When you create a session,

**Example**

```python
import daft

# when you do `import daft`, there is an implicit global session
sess = daft.current_session()

# ...

from daft import Session

# you can also create an empty session object
sess = Session()

cat1 = # pyiceberg catalog
cat2 = # unity catalog

# we can attach supported catalog instances to a session
sess.attach_catalog(cat1, alias="cat1")
sess.attach_catalog(cat2, alias="cat2")

# we can then get tables using catalog-qualified names
catalog1_tbl = sess.get_table("cat1.ns.tbl")
catalog2_tbl = sess.get_table("cat2.ns.tbl")

# we can also set the current_catalog variable to simplify name resolution
sess.set_catalog("cat2")
sess.get_table("ns.tbl") # <-- this table came from "cat2.ns.tbl"

# we can even run statements against the session!
sess.sql("select * from cat1.ns.tbl, cat2.ns.tbl")
```

**APIs**

For complete documentation, please see the [Session API docs](api_docs/session.html).

| Method              | Description                                                        |
| ------------------- | ------------------------------------------------------------------ |
| `attach`            | Attaches a session.                                                |
| `attach_catalog`    | Attaches a catalog to this session, err if already exists.         |
| `attach_table`      | Attaches a table to this session, err if already exists.           |
| `create_temp_table` | Creates a temp table scoped to this session from an existing view. |
| `current_catalog`   | Returns the session's current catalog.                             |
| `current_namespace` | Returns the session's current namespace.                           |
| `detach_catalog`    | Detaches a catalog from this session, err if does not exist.       |
| `detach_table`      | Detaches a table from this session, err if does not exist.         |
| `get_catalog`       | Returns the catalog or an object not found error.                  |
| `get_table`         | Returns the table or an object not found error.                    |
| `has_catalog`       | Returns true iff the session has access to a matching catalog.     |
| `has_table`         | Returns true iff the session has access to a matching table.       |
| `list_catalogs`     | Lists all catalogs matching the pattern.                           |
| `list_tables`       | Lists all tables matching the pattern.                             |
| `read_table`        | Reads a table from the session.                                    |
| `write_table`       | Writes a dataframe to the table.                                   |
| `set_catalog`       | Sets the current catalog.                                          |
| `set_namespace`     | Sets the current namespace.                                        |
| `sql`               | Executes SQL against the session.                                  |
| `use`               | Sets the current catalog *and* namespace.                          |

**Notes**

* You can call attach/detach on `daft.` to use the current environment's session.
* You can call attach/detach on a `Session` instance.
### Attach & Detach

The attach/detach APIs make it possible to use existing catalog and table objects with daft.

**Example**

```python
import daft

# we have an existing pyiceberg Catalog
my_pyiceberg_catalog = load_catalog("...")

# we then `attach` it to the current environment
daft.attach_catalog(my_pyiceberg_catalog, alias="")

# we can create temporary tables from dataframes
daft.create_temp_table("my_temp_table", daft.from_pydict({ ... }))

# we can now read our tables using just their identifiers!
df1 = daft.read_table("my_namespace.my_table")
df2 = daft.read_table("my_temp_table)
```

### Working with Catalogs

The `Catalog` interface allows you to perform catalog actions like `get_table` and `list_tables`.

**Example**

```python
from daft import Catalog

# create a catalog from a pyiceberg catalog object
_ = Catalog.from_iceberg(pyiceberg_catalog)

# create a catalog from a unity catalog object
_ = Catalog.from_unity(unity_catalog)

# create a catalog from a pydict mapping names to tables!
catalog = Catalog.from_pydict({
    "my_table_t": df_t,
    "my_table_s": df_s,
})


# list available tables (for iceberg, the pattern is a prefix)
catalog.list_tables(pattern=None)

# get a table by name
table_t = catalog.get_table("T")
table_s = catalog.get_table("S")
```

###  Memory

### Iceberg Catalog


**Notes**

* You can create a Catalog from `pyiceberg` and `daft.unity` catalog objects.
* We are actively working on additional catalog implementations.

### Working with Tables

The `Table` interface is a bridge from catalogs to dataframes. We can read tables into dataframes, and we can write dataframes to tables. You can work with a table independently of a catalog by using one of the factory methods, but it might not appear to provide that much utility over the existing `daft.read_` and `daft.write_` APIs. You would be correct in assuming that this is what is happening under the hood! The `Table` interface provides indirection over the table format itself and serves as a single abstraction for reading and writing that our catalogs can work with.

**Examples**

```python
from daft import Table
from pyiceberg.table import StaticTable

# suppose you have a pyiceberg table
pyiceberg_table = StaticTable("metadata.json")

# we can make it a daft table to use daft's table APIS
table = Table.from_iceberg(pyiceberg_table)

# we can read a dataframe like `daft.read_iceberg(pyiceberg_table)`
df = table.read()

# you can also create temporary tables from dataframes
daft.create_temp_table("my_temp_table", df.from_pydict({ ... }))

# these will be resolved just like other tables
df = daft.read_table("my_temp_table")
```

**Notes**

* Today you can read from `pyiceberg` and `daft.unity` table objects.
* The daft catalog interface always returns tables.



## SQL Support

You can execute SQL against your catalogs and tables using a session.

```python
from daft import Session

# create an empty session
sess = Session()

# create a temporary table from a dataframe
sess.create_temp_table("T", df.read_csv("/path/to/file.csv"))

# attaching an existing external table
sess.attach_table(my_iceberg_table, alias="S")

# joining against a temp table and an iceberg table
sess.sql("SELECT * FROM T, S")
```
