# Daft Catalogs

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love hear what you would like, thank you! 🤘

Catalogs are a centralized place to organize and govern your data. It is often responsible for creating objects such as tables and namespaces, managing transactions, and access control. Most importantly, the catalog abstracts away physical storage details, letting you focus on the logical structure of your data without worrying about file formats, partitioning schemes, or storage locations.

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs to manage catalog objects (tables and namespaces), while also making it easy to leverage Daft's existing `daft.read_` and `df.write_` APIs for open table formats like [Iceberg](../io/iceberg.md) and [Delta Lake](../io/delta_lake.md).

## Example

!!! note "Note"

    These examples use the Iceberg Catalog from the [Daft Sessions](../sessions.md) tutorial.

```python
import daft

from daft import Catalog

# iceberg_catalog from the  'Sessions' tutorial
iceberg_catalog = load_catalog(...)

# create a daft catalog from the pyiceberg catalog instance
catalog = Catalog.from_iceberg(iceberg_catalog)

# verify
catalog
"""
Catalog('default')
"""

# we can read as a dataframe
catalog.read_table("example.tbl").schema()
"""
╭─────────────┬─────────╮
│ column_name ┆ type    │
╞═════════════╪═════════╡
│ x           ┆ Boolean │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ y           ┆ Int64   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ z           ┆ Utf8    │
╰─────────────┴─────────╯
"""

# give a dataframe...
df = daft.from_pylist([{ "x": False, "y": -1, "z": "xyz" }])

# we can write to tables
catalog.write_table("example.tbl", df, mode="append")

# we can get also get table instances
t = catalog.get_table("example.tbl")

# see 'Working with Tables' for what we can do!
t
"""
Table('tbl')
"""
```

## Usage

This section covers detailed usage of the current APIs with some code snippets.

### Working with Catalogs

The `Catalog` interface allows you to perform catalog actions like `get_table` and `list_tables`.

**Example**

```python
import daft

from daft import Catalog, Table

# create a catalog from a pyiceberg catalog object
_ = Catalog.from_iceberg(pyiceberg_catalog)

# create a catalog from a unity catalog object
_ = Catalog.from_unity(unity_catalog)

# we can register various types as tables, note that all are equivalent
example_dict = { "x": [ 1, 2, 3 ] }
example_df = daft.from_pydict(example_dict)
example_table = Table.from_df("temp", example_df)

# create a catalog from a pydict mapping names to tables
catalog = Catalog.from_pydict(
    {
        "R": example_dict,
        "S": example_df,
        "T": example_table,
    }
)

# list available tables (for iceberg, the pattern is a prefix)
catalog.list_tables(pattern=None)
"""
['R', 'S', 'T']
"""

# get a table by name
table_t = catalog.get_table("T")

#
table_t.show()
"""
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ 1     │
├╌╌╌╌╌╌╌┤
│ 2     │
├╌╌╌╌╌╌╌┤
│ 3     │
╰───────╯
"""
```

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

!!! note "Note"

    Today you can read from `pyiceberg` and `daft.unity` table objects.


## Reference

!!! note "Note"

    For complete documentation, please see the [Catalog & Table API docs](../api/catalogs_tables.md).

* [Catalog][daft.catalog.Catalog] - Interface for creating and accessing both tables and namespaces
* [Identifier][daft.catalog.Identifier] - Paths to objects e.g. `catalog.namespace.table`
* [Table][daft.catalog.Table] - Interface for reading and writing dataframes
