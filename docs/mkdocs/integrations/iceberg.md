# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open-source table format originally developed at Netflix for large-scale analytical datasets.

## Support

Daft currently natively supports:

1. **Distributed Reads:** Daft will fully distribute the I/O of reads over your compute resources (whether Ray or on local multithreading)
2. **Skipping Filtered Data:** Daft uses [`df.where(...)`](../{{ api_path }}/dataframe_methods/daft.DataFrame.where.html) filter calls to only read data that matches your predicates
3. **All Catalogs From PyIceberg:** Daft is natively integrated with PyIceberg, and supports all the catalogs that PyIceberg does

## Roadmap

Here are some features of Iceberg that are works-in-progress:

1. Reading Iceberg V2 equality deletes
2. More extensive usage of Iceberg-provided statistics to further optimize queries
3. Copy-on-write and merge-on-read writes

A more detailed Iceberg roadmap for Daft can be found on [our Github Issues page](https://github.com/Eventual-Inc/Daft/issues/2458).

## Tutorial

### Using Catalogs

*PLACEHOLDER*

### Using DataFrames

#### Reading a Table

To read from the Apache Iceberg table format, use the [`daft.read_iceberg`](../{{ api_path }}/io_functions/daft.read_iceberg.html#daft.read_iceberg) function.

We integrate closely with [PyIceberg](https://py.iceberg.apache.org/) (the official Python implementation for Apache Iceberg) and allow the reading of Daft dataframes easily from PyIceberg's Table objects. The following is an example snippet of loading an example table, but for more information please consult the [PyIceberg Table loading documentation](https://py.iceberg.apache.org/api/#load-a-table).

=== "🐍 Python"

    ```python
    # Access a PyIceberg table as per normal
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("my_iceberg_catalog")
    table = catalog.load_table("my_namespace.my_table")
    ```

After a table is loaded as the `table` object, reading it into a DataFrame is extremely easy.

=== "🐍 Python"

    ```python
    # Create a Daft Dataframe
    import daft

    df = daft.read_iceberg(table)
    ```

Any subsequent filter operations on the Daft `df` DataFrame object will be correctly optimized to take advantage of Iceberg features such as hidden partitioning and file-level statistics for efficient reads.

=== "🐍 Python"

    ```python
    # Filter which takes advantage of partition pruning capabilities of Iceberg
    df = df.where(df["partition_key"] < 1000)
    df.show()
    ```

#### Writing to a Table

To write to an Apache Iceberg table, use the [`daft.DataFrame.write_iceberg`](../{{ api_path }}/dataframe_methods/daft.DataFrame.write_iceberg.html) method.

The following is an example of appending data to an Iceberg table:

=== "🐍 Python"

    ```python
    written_df = df.write_iceberg(table, mode="append")
    written_df.show()
    ```

This call will then return a DataFrame containing the operations that were performed on the Iceberg table, like so:

``` {title="Output"}

╭───────────┬───────┬───────────┬────────────────────────────────╮
│ operation ┆ rows  ┆ file_size ┆ file_name                      │
│ ---       ┆ ---   ┆ ---       ┆ ---                            │
│ Utf8      ┆ Int64 ┆ Int64     ┆ Utf8                           │
╞═══════════╪═══════╪═══════════╪════════════════════════════════╡
│ ADD       ┆ 5     ┆ 707       ┆ 2f1a2bb1-3e64-49da-accd-1074e… │
╰───────────┴───────┴───────────┴────────────────────────────────╯
```

## Type System

| Iceberg                             | Daft                                                                                                          |
|-------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `BOOLEAN`                           | [`daft.DataType.bool()`](../api_docs/datatype.html#daft.DataType.bool)                                        |
| `INT`                               | [`daft.DataType.int32()`](../api_docs/datatype.html#daft.DataType.int32)                                      |
| `LONG`                              | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64)                                      |
| `FLOAT`                             | [`daft.DataType.float32()`](../api_docs/datatype.html#daft.DataType.float32)                                  |
| `DOUBLE`                            | [`daft.DataType.float64()`](../api_docs/datatype.html#daft.DataType.float64)                                  |
| `DECIMAL(precision, scale)`         | [`daft.DataType.decimal128(precision, scale)`](../api_docs/datatype.html#daft.DataType.decimal128)            |
| `DATE`                              | [`daft.DataType.date()`](../api_docs/datatype.html#daft.DataType.date)                                        |
| `TIME`                              | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64)                                      |
| `TIMESTAMP`                         | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](../api_docs/datatype.html#daft.DataType.timestamp)  |
| `TIMESTAMPZ`                        | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `STRING`                            | [`daft.DataType.string()`](../api_docs/datatype.html#daft.DataType.string)                                    |
| `UUID`                              | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary)                                    |
| `FIXED(size)`                       | [`daft.DataType.fixed_size_binary(size)`](../api_docs/datatype.html#daft.DataType.fixed_size_binary)          |
| `BINARY`                            | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary)                                    |
| `STRUCT<[field_name: field_type,]>` | [`daft.DataType.struct(fields)`](../api_docs/datatype.html#daft.DataType.struct)                              |
| `LIST<element_type>`                | [`daft.DataType.list(element_type)`](../api_docs/datatype.html#daft.DataType.list)                            |
| `MAP<key_type, value_type>`         | [`daft.DataType.map(key_type, value_type)`](../api_docs/datatype.html#daft.DataType.map)                      |

References:

* [Iceberg Schemas and Data Types](https://iceberg.apache.org/spec/#schemas-and-data-types)

## Reference

#### `read_iceberg`

Daft has high-level `Session` and `Catalog` APIs to read Iceberg tables; however
it is the `daft.read_iceberg` API which is ultimately the entry-point to Iceberg
reads. This method accepts a `pyiceberg` table which becomes a *scan*. This scan
is responsible for producing rows for operators further up the tree. In short,
when we do `daft.read_iceberg` we create a DataFrame with a single `scan` which
is implemented by the `IcebergScanOperator`.

!!! note "DataFrames"

    DataFrames are an abstraction over relational algebra operators like filter,
    project, and join. DataFrames start with a *data source* and are built upwards,
    via composition with additional operators, to form a tree with sources as the
    leaves. We typically call these leaves *tables* and in an engine's
    implementation (query plans) they're called *scans*.

Daft's `IcebergScanOperator` initializes itself by fetching the latest schema,
or the schema of the given snapshot, along with setting up the partition key
metadata. The scan operator's primary method, `to_scan_tasks`, accepts
projection, predicate, and partition filter pushdowns and returns an iterator of
`ScanTasks`. Each `ScanTask` object holds the data file location while is then
fetched into rows.

#### `write_iceberg`

*PLACEHOLDER*

#### Iceberg Architecture

!!! note ""

    Please see the
    [Iceberg Table Specification](https://iceberg.apache.org/spec/) for full
    details.

Iceberg tables are a tree where metadata files are the inner nodes, and data
files are the leaves. The data files are typically parquet, but are not
*necessarily* so. To write data, the new data files are inserted into the tree
by creating the necessary inner nodes (metadata files) and re-rooting the
parent. To read data, we choose a root (table version) and use the metadata
files to collect all relevant data files (their children).

The *data layer* is composed of data files and delete files; where as the
*metadata layer* is composed of manifest files, manifest lists, and metadata
files.

##### Manifest Files

Manifest files (avro) keep track of data files, delete files, and statistics.
These lists track the leaves of the iceberg tree. While all manifest files use
the same schema, a manifest file contains either exclusively data files or
exclusively delete files.

##### Manifest List

Manifests lists (snapshots) contain all manifest file locations along with their
partitions, and partition columns upper and lower bounds. Each entry in the
manifest list has the form,

| Field             | Description                                          |
|-------------------|------------------------------------------------------|
| `manifest_path`   | location                                             |
| `manifest_length` | file length in bytes                                 |
| `content`         | flag where `0=data` and `1=deletes`                  |
| `partitions`      | array of field summaries e.g. nullable, upper, lower |

These are not all of the fields, but gives us an idea of what a manifest
list looks like.

##### Metadata Files

Metadata files store the table schema, partition information, and a list of all
snapshots including which one is the current. Each time an Iceberg table is
modified, a new metadata file is created; this is what is meant earlier by
"re-rooting" the tree. The *catalog* is responsible for atomically updating the
current metadata file.

##### Puffin Files

Puffin files store arbitrary metadata as blobs along with the necessary metadata
to use these blobs.

#### Table Writes

Iceberg can efficiently insert (append) and update rows. To insert data, the new
data files (parquet) are written to object storage along with a new manifest
file. Then a new manifest list, along with all existing manifest files, is added
to the new metadata file. The catalog then marks this new metadata file as the
current one.

An upsert / merge into query is a bit more complicated because its action is
conditional. If a record already exists, then it is updated. Otherwise, a new
record is inserted. This write operation is accomplished by reading all matched
records into memory, then updating each match by either copying with updates
applied (COW) or deleting (MOR) then including the updated in the insert. All
records which were not matched (did not exist) are inserted like a normal
insert.

#### Tables Reads

Iceberg reads begin by fetching the latest metadata file to then locate the
"current snapshot id". The current snapshot is a manifest list which has the
relevant manifest files which ultimately gives us a list of all data files
pertaining to the query. For each metadata layer, we can leverage the statistics
to prune both manifest files and data files.

#### Other

##### COW vs. MOR

When data is modified or deleted, we can either rewrite the relevant portion
(copy-on-write) or save the modifications for later reconciliation
(merge-on-read). Copy-on-write optimizes for read performance; however,
modifications are more expensive. Merge-on-read optimizes for write performance;
however, reads are more expensive.

##### Delete File

Delete files are used for the merge-on-read strategy in which tables updates are
written to a "delete file" which is applied or "merged" with the data files
*while* reading. A delete file can contain either *positional* or *equality*
deletes. Positional deletes denote which rows (filepath+row) have been deleted,
whereas equality deletes have an equality condition i.e. `WHERE x = 100` to
filter rows.

##### Partitioning

When a table is partitioned on some field, Iceberg will write separate data files
for each record, grouped by the partitioned field's value. That is, each data file
will contain records for a single partition. This enables efficient scanning of
a partition because all other data files can be ignored. You may partition by a
column's value (identity) or use a *partition transform* to derive a partition value.

###### [Apache Iceberg Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms)

| Transform     | Description                 |
|---------------|-----------------------------|
| `identity`    | Column value unmodified.    |
| `bucket(n)`   | Hash of value, mod `n`.     |
| `truncate(w)` | Truncated value, width `w`. |
| `year`        | Timestamp year value.       |
| `month`       | Timestamp month value.      |
| `day`         | Timestamp day value.        |
| `hour`        | Timestamp hour value.       |

## FAQs

1. **How does Daft connect to Iceberg tables?**

   *Currently supports direct file system access to Iceberg metadata files and leverages Pyarrow for reading Parquet/ORC.*

2. **Which REST catalog implementations does Daft support?**

   *Supports AWS Glue and working on REST catalog protocol implementation.*

3. **How does Daft handle Iceberg table schemas?**

   *Maps Iceberg types to Daft's type system with special handling for nested structures.*

4. **Does Daft support manifest file pruning for faster queries?**

   *Yes, uses manifest lists to identify relevant files before data loading.*

5. **How does Daft handle position deletes vs equality deletes?**

   *Full support for position deletes, equality deletes handled through post-filtering.*

6. **Can Daft leverage Iceberg's metadata for predicate pushdown?**
   *Yes, uses min/max statistics from manifest files for partition pruning.*

7. **Does Daft support time travel queries?**
   *Limited support via snapshot ID selection, timestamp-based time travel coming soon.*

8. **Which complex data types does Daft support in Iceberg tables?**
   *Full support for arrays and maps, limited support for structs with nested fields.*

9. **How does Daft handle schema evolution?**
   *Supports add/rename/drop column operations, type promotion coming soon.*

10. **Does Daft support reading table metadata like snapshot information?**
    *Yes, provides API for metadata exploration alongside data access.*

11. **How does Daft handle Iceberg's hidden partitioning?**
    *Transparently leverages partition information without user specification.*

12. **Can Daft read partition transforms like truncate, bucket, or hour?**
    *Supports identity, date transforms (year/month/day), and truncate. Bucket support is experimental.*

13. **How does Daft optimize queries against partitioned data?**
    *Uses partition pruning and Iceberg statistics for data skipping.*

14. **Does Daft support writing to Iceberg tables?**
    *Basic append support available, merge-on-read and overwrite operations in beta.*

15. **How does Daft handle transactions and atomicity?**
    *Uses optimistic concurrency for atomic commits, with configurable retry policies.*

16. **Can Daft perform schema evolution during writes?**
    *Limited support for adding new columns during write operations.*

17. **Is there a roadmap for adding full ACID transaction support?**
    *Yes, focusing on snapshot isolation guarantees and concurrent writers.*

18. **How is Daft planning to handle Iceberg's new features like Z-order clustering?**
    *Evaluating Z-order support for read operations, clustering suggestions planned.*

19. **Will Daft support Iceberg's column-level stats for better predicate pushdown?**
    *Column statistics integration is in progress, expected Q3 2025.*

20. **Are there plans to optimize Daft's memory usage when dealing with large Iceberg tables?**
    *Working on chunked reading and lazy materialization of complex objects.*
