# Reading from and Writing to Apache Paimon

[Apache Paimon](https://paimon.apache.org/) is an open-source lakehouse storage format designed for high-throughput streaming and batch analytics. It supports ACID transactions, primary-key tables with upserts (via an LSM-tree merge engine), append-only tables, and flexible partition strategies — making it a popular choice on top of object stores like HDFS, OSS, and S3.

Daft integrates with Paimon through [pypaimon](https://pypi.org/project/pypaimon/), the official Apache Paimon Python SDK.

Daft currently supports:

1. **Distributed Reads:** Daft distributes I/O across all available compute resources (Ray workers or local threads).
2. **Predicate & Partition Pushdown:** `df.where()` filter expressions are pushed down to Paimon's scan planner for partition pruning and file-level skipping.
3. **Column Projection:** Only the requested columns are read from disk.
4. **Append-only and Primary-Key Tables:** Both table types are supported; append-only tables use Daft's native high-performance Parquet reader, while PK tables that require LSM merge fall back to pypaimon's built-in reader.
5. **Catalog Abstraction:** Paimon catalogs integrate with Daft's unified `Catalog` / `Table` interfaces, enabling SQL queries and `daft.read_table()` access.
6. **BLOB Type:** Tables with BLOB columns (pypaimon 1.4+) are read as `DataType.file()` references instead of materializing the full binary content.
7. **Truncate:** Full table and partition-level truncation via `PaimonTable.truncate()` and `PaimonTable.truncate_partitions()`.

## Installation

```bash
pip install daft
pip install pypaimon
```

For S3 / S3-compatible storage (e.g. MinIO, Ceph), also install the AWS extra:

```bash
pip install "daft[aws]"
```

OSS (Alibaba Cloud Object Storage) is supported via Daft's built-in OpenDAL backend — no extra is required beyond `daft` and `pypaimon`.

## Tutorial

### Reading a Table

Use [`daft.read_paimon`][daft.read_paimon] to create a DataFrame from a Paimon table. First, obtain a table object through pypaimon:

=== "Python"
    ```python
    import pypaimon

    catalog = pypaimon.CatalogFactory.create({"warehouse": "/path/to/warehouse"})
    table = catalog.get_table("my_db.my_table")
    ```

Then create a DataFrame:

=== "Python"
    ```python
    import daft

    df = daft.read_paimon(table)
    df.show()
    ```

Filter operations are pushed down to Paimon's scan planner for efficient partition pruning:

=== "Python"
    ```python
    # Partition pruning — only reads the matching partition files
    df = df.where(df["dt"] == "2024-01-01")
    df.show()
    ```

For tables on object stores, pass an `IOConfig` to supply credentials:

=== "Python"
    ```python
    from daft.io import IOConfig, S3Config

    catalog = pypaimon.CatalogFactory.create({
        "warehouse": "s3://my-bucket/warehouse",
        "fs.s3.accessKeyId": "...",
        "fs.s3.accessKeySecret": "...",
    })
    table = catalog.get_table("my_db.my_table")

    df = daft.read_paimon(table)
    df.show()
    ```

### Writing to a Table

Use [`df.write_paimon()`][daft.DataFrame.write_paimon] to write a DataFrame back to Paimon. Two modes are supported:

#### Append

=== "Python"
    ```python
    result = df.write_paimon(table, mode="append")
    result.show()
    ```

The returned DataFrame summarises the written files:

```
╭───────────┬───────┬───────────┬────────────────────────────────╮
│ operation ┆ rows  ┆ file_size ┆ file_name                      │
│ ---       ┆ ---   ┆ ---       ┆ ---                            │
│ Utf8      ┆ Int64 ┆ Int64     ┆ Utf8                           │
╞═══════════╪═══════╪═══════════╪════════════════════════════════╡
│ APPEND    ┆ 1000  ┆ 48291     ┆ data-00001.parquet             │
╰───────────┴───────┴───────────┴────────────────────────────────╯
```

#### Overwrite

=== "Python"
    ```python
    result = df.write_paimon(table, mode="overwrite")
    result.show()
    ```

### Using the Catalog Abstraction

Daft's `Catalog` and `Table` interfaces let you access Paimon tables through the same API used by Iceberg, Unity, Glue, and other built-in integrations.

#### Creating a Catalog

=== "Python"
    ```python
    import pypaimon
    from daft.catalog import Catalog

    inner = pypaimon.CatalogFactory.create({"warehouse": "/path/to/warehouse"})
    catalog = Catalog.from_paimon(inner, name="my_paimon")
    ```

You can also wrap a single pypaimon table directly:

=== "Python"
    ```python
    from daft.catalog import Table

    inner_table = inner.get_table("my_db.my_table")
    table = Table.from_paimon(inner_table)
    ```

#### Browsing the Catalog

=== "Python"
    ```python
    # List all namespaces (databases)
    catalog.list_namespaces()
    # [Identifier('my_db'), ...]

    # List all tables
    catalog.list_tables()
    # [Identifier('my_db', 'my_table'), ...]

    # Check existence
    catalog.has_namespace("my_db")    # True
    catalog.has_table("my_db.my_table")  # True
    ```

#### Reading and Writing Through the Catalog

=== "Python"
    ```python
    # Read into a DataFrame
    df = catalog.read_table("my_db.my_table")
    df.show()

    # Append data
    catalog.write_table("my_db.my_table", df, mode="append")

    # Or use the Table object directly
    table = catalog.get_table("my_db.my_table")
    table.append(df)
    table.overwrite(df)
    ```

#### Truncating Tables

=== "Python"
    ```python
    table = catalog.get_table("my_db.my_table")

    # Remove all data
    table.truncate()

    # Remove data from specific partitions only
    table.truncate_partitions([{"dt": "2024-01-01"}])
    ```

#### Dropping Tables and Namespaces

=== "Python"
    ```python
    catalog.drop_table("my_db.my_table")
    catalog.drop_namespace("my_db")
    ```

#### Creating Tables

=== "Python"
    ```python
    import daft
    from daft.io.partitioning import PartitionField

    # Infer schema from an existing DataFrame
    schema = daft.from_pydict({"id": [1], "name": ["a"], "dt": ["2024-01-01"]}).schema()

    # Provide partition fields (identity transform — Paimon's native model)
    dt_field = schema["dt"]
    partition_fields = [PartitionField.create(dt_field)]

    table = catalog.create_table(
        "my_db.new_table",
        schema,
        partition_fields=partition_fields,
    )
    ```

To create a primary-key table, pass `primary_keys` in the `properties` dict:

=== "Python"
    ```python
    table = catalog.create_table(
        "my_db.pk_table",
        schema,
        properties={"primary_keys": ["id", "dt"]},
        partition_fields=partition_fields,
    )
    ```

### Session and SQL Integration

Once attached to a `Session`, your Paimon catalog is available from SQL queries and `daft.read_table()`:

=== "Python"
    ```python
    from daft.session import Session

    sess = Session()
    sess.attach(catalog, "my_paimon")
    sess.set_namespace("my_db")

    # SQL query resolves through the Paimon catalog
    sess.sql("SELECT id, name FROM my_table WHERE dt = '2024-01-01'").show()

    # Fully-qualified read
    sess.read_table("my_paimon.my_db.my_table").show()
    ```

## Type System

Paimon types are mapped through PyArrow to Daft types:

| Paimon                          | Daft                                                    |
|---------------------------------|---------------------------------------------------------|
| `BOOLEAN`                       | `daft.DataType.bool()`                                  |
| `TINYINT`                       | `daft.DataType.int8()`                                  |
| `SMALLINT`                      | `daft.DataType.int16()`                                 |
| `INT`                           | `daft.DataType.int32()`                                 |
| `BIGINT`                        | `daft.DataType.int64()`                                 |
| `FLOAT`                         | `daft.DataType.float32()`                               |
| `DOUBLE`                        | `daft.DataType.float64()`                               |
| `DECIMAL(precision, scale)`     | `daft.DataType.decimal128(precision, scale)`            |
| `DATE`                          | `daft.DataType.date()`                                  |
| `TIME(precision)`               | `daft.DataType.int64()`                                 |
| `TIMESTAMP(precision)`          | `daft.DataType.timestamp(timeunit=..., timezone=None)`  |
| `TIMESTAMP_LTZ(precision)`      | `daft.DataType.timestamp(timeunit=..., timezone="UTC")` |
| `CHAR(n)` / `VARCHAR(n)` / `STRING` | `daft.DataType.string()`                           |
| `BINARY(n)` / `VARBINARY(n)` / `BYTES` | `daft.DataType.binary()`                       |
| `BLOB`                          | `daft.DataType.file()` (lazy `FileReference` with byte-range) |
| `ARRAY<element_type>`           | `daft.DataType.list(element_type)`                      |
| `MAP<key_type, value_type>`     | `daft.DataType.map(key_type, value_type)`               |
| `ROW<[field_name: field_type]>` | `daft.DataType.struct(fields)`                          |

## FAQs

1. **Do I need to install pypaimon separately?**
   *Yes. Run `pip install pypaimon`. pypaimon is not bundled with Daft.*

2. **Which Paimon catalog types are supported?**
   *pypaimon currently ships two catalog implementations: `FileSystemCatalog` (local / OSS / S3, selected by `metastore=filesystem` or by default) and `RESTCatalog` (selected by `metastore=rest`). Both can be wrapped with `Catalog.from_paimon()`.*

3. **Can I use Daft with an existing Paimon warehouse?**
   *Yes. Create a pypaimon catalog pointing at your warehouse and pass it to `Catalog.from_paimon()` or use `catalog.get_table()` and `daft.read_paimon()` directly.*

4. **Does Daft support schema evolution for Paimon tables?**
   *Reading tables with evolved schemas is handled by pypaimon's reader. Daft does not currently expose DDL operators beyond `create_table`, `drop_table`, `drop_namespace`, `truncate`, and `truncate_partitions`.*

5. **How do I configure credentials for OSS or S3?**
   *Pass an `IOConfig` to `daft.read_paimon()`, or include the relevant `fs.*` options in the catalog options dict when creating the pypaimon catalog (Daft will infer an `IOConfig` automatically from these).*

6. **Can I use the Daft Paimon connector inside a Ray cluster?**
   *Yes. Daft's distributed execution on Ray works with Paimon the same way it does with Iceberg — scan tasks are distributed across workers automatically.*
