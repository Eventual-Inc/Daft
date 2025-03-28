# Iceberg Reference

*The purpose of this document is to document Daft's Iceberg support.*

## Iceberg Background

Iceberg tables are a tree where metadata files are the inner nodes, and data
files are the leaves. The data files are typically parquet, but are not
*necessarily* so. To write data, the new data files are inserted into the tree
by creating the necessary inner nodes (metadata files) and re-rooting the
parent. To read data, we choose a root (table version) and use the metadata
files to collect all relevant data files (their children).

### Architecture

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
| ----------------- | ---------------------------------------------------- |
| `manifest_path`   | location                                             |
| `manifest_length` | file length in bytes                                 |
| `content`         | flag where `0=data` and `1=deletes`                  |
| `partitions`      | array of field summaries e.g. nullable, upper, lower |

!!! note ""

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

### Tables Reads

Iceberg reads begin by fetching the latest metadata file to then locate the
"current snapshot id". The current snapshot is a manifest list which has the
relevant manifest files which ultimately gives us a list of all data files
pertaining to the query. For each metadata layer, we can leverage the statistics
to prune both manifest and data files.

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

> *PLACEHOLDER*

## Daft

### `read_iceberg`

Daft has high-level `Session` and `Catalog` APIs to read Iceberg tables; however
it is the `daft.read_iceberg` API which is ultimately the entry-point to Iceberg
reads. This method accepts a `pyiceberg` table which becomes a *scan*. This scan
is responsible for producing rows for operators futher up the tree. In short,
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

### `write_iceberg`

> *PLACEHOLDER*
