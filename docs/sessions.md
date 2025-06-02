# Daft Sessions

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love hear want you would like, thank you! 🤘


Sessions enable you to attach catalogs, tables, and create temporary objects which are accessible through both the Python and SQL APIs. Sessions hold configuration state such as `current_catalog` and `current_namespace` which are used in name resolution and can simplify your workflows.

## Example

```python
import daft

# `import daft` defines an implicit session `daft.current_session()`

from daft.session import Session

# create a new session
sess = Session()

# create a temp table from a DataFrame
sess.create_temp_table("T", daft.from_pydict({ "x": [1,2,3] }))

# read table as dataframe from the session
_ = sess.read_table("T")

# get the table instance
t = sess.get_table("T")

# read table instance as a datadrame
_ = t.read()

# execute sql against the session
sess.sql("SELECT * FROM T").show()
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
```

## Usage

This section covers detailed usage of the current APIs with some code snippets.

### Setup

!!! note "Note"

    For these examples, we are using sqlite Iceberg which requires `pyiceberg[sql-sqlite]`.

```python
from daft import Catalog
from pyiceberg.catalog.sql import SqlCatalog

# don't forget to `mkdir -p /tmp/daft/example`
tmpdir = "/tmp/daft/example"

# create a pyiceberg catalog backed by sqlite
iceberg_catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{tmpdir}/catalog.db",
        "warehouse": f"file://{tmpdir}",
    },
)

# creating a daft catalog from the pyiceberg catalog implementation
catalog = Catalog.from_iceberg(iceberg_catalog)

# check
catalog.name
"""
'default'
"""
```

### Session State

Let's get started by creating an empty session and checking the state.

```python
import daft

from daft.session import Session

# create a new empty session
sess = Session()

# check the current catalog (None)
sess.current_catalog()

# get the current namespace (None)
sess.current_namespace()
```

### Attach & Detach

The attach and detach methods make it easy to use catalogs and tables in a session. This example shows how we can attach our newly created catalog. When you attach a catalog to an empty session, it automatically becomes the current active catalog.

```python
# attach makes it possible to use existing catalogs in the session
sess.attach(catalog)

# check the current catalog was set automatically
sess.current_catalog()
"""
Catalog('default')
"""

# detach would remove the catalog
# sess.detach_catalog("default")
```

### Create & Drop

We can create tables and namespaces directly through a catalog or via our session.

```python
# create a namespace 'example'
sess.create_namespace("example")

# verify it was created
sess.list_namespaces()
"""
[Identifier('example')]
"""

# you can create an empty table with a schema
# sess.create_table("example.tbl", schema)

# but suppose we have some data..
df = daft.from_pydict({
    "x": [ True, True, False ],
    "y": [ 1, 2, 3 ],
    "z": [ "abc", "def", "ghi" ],
})

# create a table from the dataframe, which will create + append
sess.create_table("example.tbl", df)

# you can also create temporary tables from dataframes
# > echo "x,y,z\nFalse,4,jkl" > /tmp/daft/row.csv
sess.create_temp_table("temp", daft.read_csv("/tmp/daft/row.csv"))

# you can drop too
# sess.drop_table("example.tbl")
# sess.drop_namespace("example")
```

### Read & Write

Using sessions abstracts away underlying catalog and table implementations so you can easily read and write DataFrames.

```python
# we can read our table back as a DataFrame instance
sess.read_table("example.tbl").show()
"""
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 1     ┆ abc  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 2     ┆ def  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ false   ┆ 3     ┆ ghi  │
╰─────────┴───────┴──────╯
"""

# create a single row to append
row = daft.from_pylist([{ "x": True, "y": 4, "z": "jkl" }])

# we can write a DataFrame to the Table via the Session
sess.write_table("example.tbl", row, mode="append")

# we can use session state and table objects!
sess.set_namespace("example")

# name resolution is trivial
tbl = sess.get_table("tbl")

# to read, we have .read() .select(*cols) or .show()
tbl.show()
"""
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 4     ┆ jkl  │  <--- `row` was inserted
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 1     ┆ abc  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 2     ┆ def  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ false   ┆ 3     ┆ ghi  │
╰─────────┴───────┴──────╯
"""

# to write, we have .write(df, mode), .append(df), .overwrite(df)
tbl.append(row)

# row is now inserted twice
tbl.show()
"""
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 4     ┆ jkl  │ <-- append via tbl.append(...)
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 4     ┆ jkl  │ <-- append via sess.write(...)
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 1     ┆ abc  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ true    ┆ 2     ┆ def  │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ false   ┆ 3     ┆ ghi  │
╰─────────┴───────┴──────╯
"""
```

### Using SQL

The session enables executing Daft SQL against your catalogs.

```python

# use the given catalog and namespace (like the use/set_ methods)
sess.sql("USE default.example")

# we support both qualified and unqualified names by leveraging the session state
sess.sql("SELECT * FROM tbl LIMIT 1").show()
╭─────────┬───────┬──────╮
│ x       ┆ y     ┆ z    │
│ ---     ┆ ---   ┆ ---  │
│ Boolean ┆ Int64 ┆ Utf8 │
╞═════════╪═══════╪══════╡
│ true    ┆ 4     ┆ jkl  │
╰─────────┴───────┴──────╯

# we can even combine our queries with the temp table from earlier
sess.sql("SELECT * FROM example.tbl, temp LIMIT 1").show()
╭─────────┬───────┬──────┬────────────┬────────┬────────╮
│ x       ┆ y     ┆ z    ┆      …     ┆ temp.y ┆ temp.z │
│ ---     ┆ ---   ┆ ---  ┆            ┆ ---    ┆ ---    │
│ Boolean ┆ Int64 ┆ Utf8 ┆ (1 hidden) ┆ Int64  ┆ Utf8   │
╞═════════╪═══════╪══════╪════════════╪════════╪════════╡
│ true    ┆ 4     ┆ jkl  ┆ …          ┆ 4      ┆ jkl    │
╰─────────┴───────┴──────┴────────────┴────────┴────────╯
```

!!! note "Note"

    We aim to support SQL DDL in future releases!

## Reference

For complete documentation, please see the [Session API docs](api/sessions.md).

| Method                                                                          | Description                                                        |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------|
| [`attach`][daft.Session.attach]                                                 | Attaches a catalog, table, or function to this session.            |
| [`attach_catalog`][daft.Session.attach_catalog]                                 | Attaches an exiting catalog to this session                        |
| [`attach_table`][daft.Session.attach_table]                                     | Attaches an existing table to this session                         |
| [`attach_function`][daft.Session.attach_function]                               | Attaches a user-defined function to this session                   |
| [`create_namespace`][daft.Session.create_namespace]                             | Creates a new namespace                                            |
| [`create_namespace_if_not_exists`][daft.Session.create_namespace_if_not_exists] | Creates a new namespace if it doesn't already exist                |
| [`create_table`][daft.Session.create_table]                                     | Creates a new table from the source                                |
| [`create_table_if_not_exists`][daft.Session.create_table_if_not_exists]         | Creates a new table from the source if it doesn't already exist    |
| [`create_temp_table`][daft.Session.create_temp_table]                           | Creates a temp table scoped to this session from an existing view. |
| [`current_catalog`][daft.Session.current_catalog]                               | Returns the session's current catalog.                             |
| [`current_namespace`][daft.Session.current_namespace]                           | Returns the session's current namespace.                           |
| [`detach_catalog`][daft.Session.detach_catalog]                                 | Detaches the catalog from this session                             |
| [`detach_table`][daft.Session.detach_table]                                     | Detaches the table from this session                               |
| [`drop_namespace`][daft.Session.drop_namespace]                                 | Drop the namespace in the session's current catalog                |
| [`drop_table`][daft.Session.drop_table]                                         | Drop the table in the session's current catalog                    |
| [`get_catalog`][daft.Session.get_catalog]                                       | Returns the catalog or an object not found error.                  |
| [`get_table`][daft.Session.get_table]                                           | Returns the table or an object not found error.                    |
| [`has_catalog`][daft.Session.has_catalog]                                       | Returns true iff the session has access to a matching catalog.     |
| [`has_namespace`][daft.Session.has_namespace]                                   | Returns true iff the session has access to a matching namespace.   |
| [`has_table`][daft.Session.has_table]                                           | Returns true iff the session has access to a matching table.       |
| [`list_catalogs`][daft.Session.list_catalogs]                                   | Lists all catalogs matching the pattern.                           |
| [`list_namespaces`][daft.Session.list_namespaces]                               | Lists all namespaces matching the pattern.                         |
| [`list_tables`][daft.Session.list_tables]                                       | Lists all tables matching the pattern.                             |
| [`read_table`][daft.Session.read_table]                                         | Reads a table from the session.                                    |
| [`write_table`][daft.Session.write_table]                                       | Writes a dataframe to the table.                                   |
| [`set_catalog`][daft.Session.set_catalog]                                       | Sets the current catalog.                                          |
| [`set_namespace`][daft.Session.set_namespace]                                   | Sets the current namespace.                                        |
| [`sql`][daft.Session.sql]                                                       | Executes SQL against the session.                                  |
| [`use`][daft.Session.use]                                                       | Sets the current catalog and namespace.                            |
