# Connectors

Daft offers a variety of approaches to reading from and writing to various data sources (in-memory data, files, data catalogs, and integrations). Please see [Daft Connectors API docs](../api/io.md) for API details.

## In-Memory

| Function                                          | Description                                             |
|---------------------------------------------------|---------------------------------------------------------|
| [`from_arrow`][daft.from_arrow]                   | Create a DataFrame from PyArrow Tables or RecordBatches |
| [`from_dask_dataframe`][daft.from_dask_dataframe] | Create a DataFrame from a Dask DataFrame                |
| [`from_glob_path`][daft.from_glob_path]           | Create a DataFrame from files matching a glob pattern   |
| [`from_pandas`][daft.from_pandas]                 | Create a DataFrame from a Pandas DataFrame              |
| [`from_pydict`][daft.from_pydict]                 | Create a DataFrame from a python dictionary             |
| [`from_pylist`][daft.from_pylist]                 | Create a DataFrame from a python list                   |
| [`from_ray_dataset`][daft.from_ray_dataset]       | Create a DataFrame from a Ray Dataset                   |

## CSV

| Function                                          | Description                                            |
|---------------------------------------------------|--------------------------------------------------------|
| [`read_csv`][daft.io.read_csv]                    | Read a CSV file or multiple CSV files into a DataFrame |
| [`write_csv`][daft.dataframe.DataFrame.write_csv] | Write a DataFrame to CSV files                         |


## Delta Lake

| Function                                                      | Description                              |
|---------------------------------------------------------------|------------------------------------------|
| [`read_deltalake`][daft.io.read_deltalake]                    | Read a Delta Lake table into a DataFrame |
| [`write_deltalake`][daft.dataframe.DataFrame.write_deltalake] | Write a DataFrame to a Delta Lake table  |

See also [Delta Lake](delta_lake.md) for detailed integration.

## Hudi

| Function                         | Description                        |
|----------------------------------|------------------------------------|
| [`read_hudi`][daft.io.read_hudi] | Read a Hudi table into a DataFrame |

See also [Apache Hudi](hudi.md) for detailed integration.

## Iceberg

| Function                                                  | Description                            |
|-----------------------------------------------------------|----------------------------------------|
| [`read_iceberg`][daft.io.read_iceberg]                    | Read an Iceberg table into a DataFrame |
| [`write_iceberg`][daft.dataframe.DataFrame.write_iceberg] | Write a DataFrame to an Iceberg table  |

See also [Iceberg](iceberg.md) for detailed integration.


## JSON

| Function                         | Description                                              |
|----------------------------------|----------------------------------------------------------|
| [`read_json`][daft.io.read_json] | Read a JSON file or multiple JSON files into a DataFrame |


## Lance

| Function                                              | Description                           |
|-------------------------------------------------------|---------------------------------------|
| [`read_lance`][daft.io.read_lance]                    | Read a Lance dataset into a DataFrame |
| [`write_lance`][daft.dataframe.DataFrame.write_lance] | Write a DataFrame to a Lance dataset  |

<!-- See also [Lance](lance.md) for detailed integration. -->

## Parquet

| Function                                                  | Description                                                    |
|-----------------------------------------------------------|----------------------------------------------------------------|
| [`read_parquet`][daft.io.read_parquet]                    | Read a Parquet file or multiple Parquet files into a DataFrame |
| [`write_parquet`][daft.dataframe.DataFrame.write_parquet] | Write a DataFrame to Parquet files                             |


## SQL

| Function                       | Description                                    |
|--------------------------------|------------------------------------------------|
| [`read_sql`][daft.io.read_sql] | Read data from a SQL database into a DataFrame |


## Video

| Function                                         | Description                        |
|--------------------------------------------------|------------------------------------|
| [`read_video_frames`][daft.io.read_video_frames] | Read video frames into a DataFrame |


## WARC

| Function                         | Description                                              |
|----------------------------------|----------------------------------------------------------|
| [`read_warc`][daft.io.read_warc] | Read a WARC file or multiple WARC files into a DataFrame |


## User-Defined

| Function                                            | Description                                                        |
|-----------------------------------------------------|--------------------------------------------------------------------|
| [`DataSink`][daft.io.sink.DataSink]                 | Interface for writing data from DataFrames                         |
| [`DataSource`][daft.io.source.DataSource]           | Interface for reading data into DataFrames                         |
| [`DataSourceTask`][daft.io.source.DataSourceTask]   | Represents a partition of data that can be processed independently |
| [`WriteResult`][daft.io.sink.WriteResult]           | Wrapper for intermediate results written by a DataSink             |
| [`write_sink`][daft.dataframe.DataFrame.write_sink] | Write a DataFrame to the given DataSink                            |

## Daft Catalogs

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love to hear what you would like, thank you! 🤘

Daft also provides APIs to work with catalogs. Catalogs are a centralized place to organize and govern your data. It is often responsible for creating objects such as tables and namespaces, managing transactions, and access control. Most importantly, the catalog abstracts away physical storage details, letting you focus on the logical structure of your data without worrying about file formats, partitioning schemes, or storage locations.

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs to manage catalog objects (tables and namespaces), while also making it easy to leverage Daft's existing `daft.read_` and `df.write_` APIs for open table formats like [Iceberg](iceberg.md) and [Delta Lake](delta_lake.md).

### Example

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

### Usage

This section covers detailed usage of the current APIs with some code snippets.

#### Working with Catalogs

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

#### Working with Tables

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
daft.create_temp_table("my_temp_table", daft.from_pydict({ ... }))

# these will be resolved just like other tables
df = daft.read_table("my_temp_table")
```

!!! note "Note"

    Today you can read from `pyiceberg` and `daft.unity` table objects.


### Reference

!!! note "Note"

    For complete documentation, please see the [Catalog & Table API docs](../api/catalogs_tables.md).

* [Catalog][daft.catalog.Catalog] - Interface for creating and accessing both tables and namespaces
* [Identifier][daft.catalog.Identifier] - Paths to objects e.g. `catalog.namespace.table`
* [Table][daft.catalog.Table] - Interface for reading and writing dataframes
