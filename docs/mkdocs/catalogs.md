# Daft Catalogs

!!! warning ""

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues?q=is%3Aissue%20state%3Aopen%20label%3Adata-catalogs) for any bugs you may encounter. Thank you! 🤘

Catalogs are a centralized place to organize and govern your data. It is often responsible for creating objects such as tables and namespaces, managing transactions, and access control. Most importantly, the catalog abstracts away physical storage details, letting you focus on the logical structure of your data without worrying about file formats, partitioning schemes, or storage locations.

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs which make it easy to manage catalog objects (tables and namespaces), while also making it easy to leverage daft's great I/O support for reading and writing open table formats like [Iceberg](integrations/iceberg.md) and [Delta Lake](integrations/delta_lake.md).

## Example

Daft provides several high-level interfaces to simplify accessing your data.

* [Session]() - interface for
* [Catalog](api_docs/catalog.html) - interface for creating and accessing both tables and namespaces.
* [Table](api_docs/catalog.html#daft.Table) -

When you `import daft` there is an implicit session which is accessible via `daft.current_session()`.

```python
import daft

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

This section covers detailed usage of the current APIs with some code snippets.

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

#### Creating Catalogs

You

```python

cat2 = Catalog.from_pydict(name="my_cat", table={
    "tbl1": {
        "x": [ 1,2,3 ],
        "y": [ 4,5,6 ],
    },
    "tbl2": daft.read_parquet("/path/to/file.parquet"),
})
```

#### Attach & Detach

```python
# TODO
```

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



## Reference

!!! note ""

    For complete documentation, please see the [Catalog & Table API docs](api_docs/catalog.html).

* [Catalog](api_docs/catalog.html) - Interface for creating and accessing both tables and namespaces
* [Identifier](api_docs/catalog.html#daft.Identifier) - Paths to objects e.g. `catalog.namespace.table`
* [Table](api_docs/catalog.html#daft.Table) - Interface for reading and writing dataframes
* [TableSource](api_docs/catalog.html#daft.TableSource) - Sources for table creation like a schema or dataframe.
