# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open-source table format originally developed at Netflix for large-scale analytical tables and datasets. It provides a way to manage and organize data files (like Parquet and ORC) as tables, offering benefits like ACID transactions, time travel, and schema evolution.

Daft currently natively supports:

1. **Distributed Reads:** Daft will fully distribute the I/O of reads over your compute resources (whether Ray or on local multithreading)
2. **Skipping Filtered Data:** Daft uses [`df.where()`][daft.DataFrame.where] filter calls to only read data that matches your predicates
3. **All Catalogs From PyIceberg:** Daft is natively integrated with PyIceberg, and supports all the catalogs that PyIceberg does

A detailed Iceberg roadmap for Daft can be found on [our Github issues](https://github.com/Eventual-Inc/Daft/issues/2458). For the overall Daft development plan, see [Daft Roadmap](../roadmap.md).

## Tutorial

### Reading a Table

To read from the Apache Iceberg table format, use the [`daft.read_iceberg`][daft.read_iceberg] function.

We integrate closely with [PyIceberg](https://py.iceberg.apache.org/) (the official Python implementation for Apache Iceberg) and allow the reading of DataFrames easily from PyIceberg's Table objects. The following is an example snippet of loading an example table, but for more information please consult the [PyIceberg Table loading documentation](https://py.iceberg.apache.org/api/#load-a-table).

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
    # Create a DataFrame
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

### Writing to a Table

To write to an Apache Iceberg table, use the [`df.write_iceberg()`][daft.DataFrame.write_iceberg] method.

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

| Iceberg                             | Daft                                                                                         |
|-------------------------------------|----------------------------------------------------------------------------------------------|
| `BOOLEAN`                           | [`daft.DataType.bool()`][daft.datatype.DataType.bool]                                        |
| `INT`                               | [`daft.DataType.int32()`][daft.datatype.DataType.int32]                                      |
| `LONG`                              | [`daft.DataType.int64()`][daft.datatype.DataType.int64]                                      |
| `FLOAT`                             | [`daft.DataType.float32()`][daft.datatype.DataType.float32]                                  |
| `DOUBLE`                            | [`daft.DataType.float64()`][daft.datatype.DataType.float64]                                  |
| `DECIMAL(precision, scale)`         | [`daft.DataType.decimal128(precision, scale)`][daft.datatype.DataType.decimal128]            |
| `DATE`                              | [`daft.DataType.date()`][daft.datatype.DataType.date]                                        |
| `TIME`                              | [`daft.DataType.int64()`][daft.datatype.DataType.int64]                                      |
| `TIMESTAMP`                         | [`daft.DataType.timestamp(timeunit="us", timezone=None)`][daft.datatype.DataType.timestamp]  |
| `TIMESTAMPZ`                        | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`][daft.datatype.DataType.timestamp] |
| `STRING`                            | [`daft.DataType.string()`][daft.datatype.DataType.string]                                    |
| `UUID`                              | [`daft.DataType.binary()`][daft.datatype.DataType.binary]                                    |
| `FIXED(size)`                       | [`daft.DataType.fixed_size_binary(size)`][daft.datatype.DataType.fixed_size_binary]          |
| `BINARY`                            | [`daft.DataType.binary()`][daft.datatype.DataType.binary]                                    |
| `STRUCT<[field_name: field_type,]>` | [`daft.DataType.struct(fields)`][daft.datatype.DataType.struct]                              |
| `LIST<element_type>`                | [`daft.DataType.list(element_type)`][daft.datatype.DataType.list]                            |
| `MAP<key_type, value_type>`         | [`daft.DataType.map(key_type, value_type)`][daft.datatype.DataType.map]                      |

See also [Iceberg Schemas and Data Types](https://iceberg.apache.org/spec/#schemas-and-data-types).

## Reference

Daft has high-level [Session](../sessions.md) and [Catalog](../catalogs/index.md) APIs
to read and write Iceberg tables; however it is the [`daft.read_iceberg`][daft.read_iceberg] and
[`df.write_iceberg`][daft.DataFrame.write_iceberg] API which is ultimately the entry-point to Iceberg reads and
writes respectively. This section gives a short reference on those APIs and how
they relate to both DataFrames and Iceberg.

Daft's DataFrames are an abstraction over relational algebra operators like
filter, project, and join. DataFrames start with a *data source* and are built
upwards, via composition with additional operators, to form a tree with sources
as the leaves. We typically call these leaves *tables* or *sources* and their
algebraic operator is called a *scan*.

### [`read_iceberg`][daft.read_iceberg]

Daft's [`daft.read_iceberg`][daft.read_iceberg] method creates a DataFrame from the the given PyIceberg
table. It produces rows by traversing the table's metadata tree to locate all
the data files for the given snapshot which is handled by our
`IcebergScanOperator`.

Daft's `IcebergScanOperator` initializes itself by fetching the latest schema,
or the schema of the given snapshot, along with setting up the partition key
metadata. The scan operator's primary method, `to_scan_tasks`, accepts pushdowns
(projections, predicates, partition filters) and returns an iterator of
`ScanTasks`. Each `ScanTask` object holds a data file, optional delete files,
and the associated pushdowns. Finally, we read each data file's parquet to
produce a stream of record batches which later operators consume and transform.

### [`write_iceberg`][daft.DataFrame.write_iceberg]

Daft's [`write_iceberg`][daft.DataFrame.write_iceberg] method writes the DataFrame's contents to the given PyIceberg table.
It works by creating a special *sink* operator which consumes all inputs and writes
data files to the table's location.

Daft's sink operator will apply the Iceberg partition transform and distribute
records to an appropriate data file writer. Each writer is responsible for
actually writing the parquet to storage and keeping track of metadata like total
bytes written. Once the sink has exhausted its input, it will close all open
writers.

Finally, we update the Iceberg table's metadata to include these new data files,
and use a transaction to update the latest metadata pointer.

### Iceberg Architecture

!!! note "Note"

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

#### Manifest Files

Manifest files (avro) keep track of data files, delete files, and statistics.
These lists track the leaves of the iceberg tree. While all manifest files use
the same schema, a manifest file contains either exclusively data files or
exclusively delete files.

#### Manifest List

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

#### Metadata Files

Metadata files store the table schema, partition information, and a list of all
snapshots including which one is the current. Each time an Iceberg table is
modified, a new metadata file is created; this is what is meant earlier by
"re-rooting" the tree. The *catalog* is responsible for atomically updating the
current metadata file.

#### Puffin Files

Puffin files store arbitrary metadata as blobs along with the necessary metadata
to use these blobs.

### Table Writes

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

### Table Reads

Iceberg reads begin by fetching the latest metadata file to then locate the
"current snapshot id". The current snapshot is a manifest list which has the
relevant manifest files which ultimately gives us a list of all data files
pertaining to the query. For each metadata layer, we can leverage the statistics
to prune both manifest files and data files.

### Other

#### COW vs. MOR

When data is modified or deleted, we can either rewrite the relevant portion
(copy-on-write) or save the modifications for later reconciliation
(merge-on-read). Copy-on-write optimizes for read performance; however,
modifications are more expensive. Merge-on-read optimizes for write performance;
however, reads are more expensive.

#### Delete File

Delete files are used for the merge-on-read strategy in which tables updates are
written to a "delete file" which is applied or "merged" with the data files
*while* reading. A delete file can contain either *positional* or *equality*
deletes. Positional deletes denote which rows (filepath+row) have been deleted,
whereas equality deletes have an equality condition i.e. `WHERE x = 100` to
filter rows.

#### Partitioning

When a table is partitioned on some field, Iceberg will write separate data files
for each record, grouped by the partitioned field's value. That is, each data file
will contain records for a single partition. This enables efficient scanning of
a partition because all other data files can be ignored. You may partition by a
column's value (identity) or use a *partition transform* to derive a partition value.

##### [Apache Iceberg Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms)

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

1. **How does Daft read Iceberg tables?**

    *Daft reads Iceberg tables by reading a snapshot's data files into its arrow-based record batches. For more detail, please see the [`read_iceberg`](#read_iceberg) reference.*

2. **How does Daft write Iceberg tables?**

    *Daft writes Iceberg tables by writing the new, possibly partitioned, data files to storage then atomically committing an updated Iceberg metadata file. For more detail, please see [`write_iceberg`](#write_iceberg)*.

3. **How do Daft's data types compare to Iceberg's data types?**

    *The type systems are quite similar because they are both based around Apache Arrow's types. Please see our comprehensive type comparison table.*

4. **Does Daft support Iceberg REST catalog implementations?**

    *Yes! Daft uses PyIceberg to interface with Iceberg catalogs which has extensive Iceberg REST catalog support.*

5. **How does Daft handle positional deletes vs equality deletes?**

    *Daft currently only supports positional deletes, but V2 equality deletes are on the roadmap.*

6. **Can Daft leverage Iceberg's metadata for predicate pushdown?**

    *Yes, uses min/max statistics from manifest files for partition pruning.*

7. **Does Daft support time travel queries?**

    *Daft supports reading by snapshot id, and snapshot slices are on the roadmap*.

8. **Which complex data types does Daft support in Iceberg tables?**

    *Daft supports arrays, structs, and maps.*

9. **How does Daft handle schema evolution?**

    *Daft currently does not expose any data definition operators beyond [create_table][daft.session.Session.create_table].*

10. **Does Daft support reading table metadata like snapshot information?**

    *Daft does not have native APIs for this, and we recommend using PyIceberg for reading metadata.*

11. **How does Daft handle Iceberg's hidden partitioning?**

    *Transparently leverages partition information without user specification.*

12. **Can Daft read and write partition transforms like truncate, bucket, or hour?**

    *Daft can read and write all Iceberg partition transform types: identity, bucket, date transforms (year/month/day), and truncate.*

13. **How does Daft optimize queries against partitioned data?**

    *Daft will shuffle partitioned data when possible and based upon your execution environment.*

14. **Which writes operations does Daft support?**

    *Daft supports basic overwrite and append, it does not support upserts like copy-on-write updates.*
