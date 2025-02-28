# Catalog Usage Guide

Catalogs are a centralized place to organize and govern your data. It is often responsible for creating objects such as tables and namespaces, managing transactions, and access control. Most importantly, the catalog abstracts away physical storage details, letting you focus on the logical structure of your data without worrying about file formats, partitioning schemes, or storage locations.

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs which make it easy to manage catalog objects (tables and namespaces), while also making it easy to leverage daft's great I/O support for reading and writing open table formats like [Iceberg](https://www.getdaft.io/projects/docs/en/stable/integrations/iceberg/) and [DeltaLake](https://www.getdaft.io/projects/docs/en/stable/integrations/unity_catalog/).

Our latest release ([0.4.5](https://github.com/Eventual-Inc/Daft/releases/tag/v0.4.5)) covers the basics for connecting to catalogs and reading tables, our next release will include write support and additional catalog integrations 🤘


## TL;DR

```python
import daft

from daft.catalog import Catalog, Table
from pyiceberg.catalog import load_catalog

# load iceberg catalog (assume table 'ns.tbl' exists)
pyiceberg_catalog = load_catalog("...")

# attach to the environment
daft.attach_catalog(pyiceberg_catalog, alias="tldr")

# read your tables as daft dataframes
df = daft.read_table("ns.tbl")
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
daft.create_temp_table("my_temp_table", df.from_pydict({ ... }))

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

# create a catalog from a pydict mapping names to tables
catalog = Catalog.from_pydict({
    "T": table_t,
    "S": table_s,
})

# list available tables (for iceberg, the pattern is a prefix)
catalog.list_tables(pattern=None)

# get a table by name
table_t = catalog.get_table("T")
table_s = catalog.get_table("S")
```

**APIs**

```python
class Catalog(ABC):

    def get_table(self, identifier: Identifier | str) -> Table: ...

    def list_tables(self, pattern: str | None = None) -> list[str]: ...
```

**Notes**

* You can create a Catalog from `pyiceberg` and `daft.unity` catalog objects.
* We are actively working on additional catalog implementations.
* Our next release will include DDL actions like `create_*` and `drop_*`.

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

**APIs**

```python
class Table(ABC):

    def read(self) -> DataFrame: ...

    def show(self) -> None: ...

    # coming soon!
    # def append(self, df: DataFrame) -> None:  ...

    # coming soon!
    # def overwrite(self, df: DataFrame) -> None:  ...
```

**Notes**

* Today you can read from `pyiceberg` and `daft.unity` table objects.
* The daft catalog interface always returns tables.
* We are actively working on additional table implementations and write APIs.

### Working with Sessions

Sessions hold attached objects, temporary objects, and stateful variables like the `current_catalog` and `current_namespace`.

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

# we can attach external catalog instances to a session
sess.attach_catalog(cat1, alias="cat1")
sess.attach_catalog(cat2, alias="cat2")

# we can then get tables using catalog-qualified names
catalog1_tbl = sess.get_table("cat1.ns.tbl")
catalog2_tbl = sess.get_table("cat2.ns.tbl")

# we can also set the current_catalog variable to simplify name resolution
sess.set_catalog("cat2")
sess.get_table("ns.tbl") # <-- this table came from "cat2.ns.tbl"

# we can even run statements against the session!
sess.sql("select * from ice1.ns.tbl, ice2.ns.tbl")
```

**APIs**

```python
def attach_catalog(catalog: Catalog | object, alias: str | None = None) -> Catalog: ...

def detach_catalog(alias: str): ...

def attach_table(table: Table | object, alias: str | None = None) -> Table: ...

def detach_table(alias: str): ...
```

**Notes**

* You can call attach/detach on `daft.` to use the current environment's session.
* You can call attach/detach on a `Session` instance.

### Catalogs and Tables with SQL

You can execute SQL against your catalogs and tables using a session.

```python
from daft import Session

# create an empty session
sess = Session()

# create a temporary table from a dataframe
sess.create_temp_table("T", )

# attaching an existing external table
sess.attach_table(my_iceberg_table, alias="S")

# joining against a temp table and an iceberg table
sess.sql("SELECT * T, S")
```
