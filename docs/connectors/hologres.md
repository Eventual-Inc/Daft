# Reading from and Writing to Hologres

[Hologres](https://www.alibabacloud.com/product/hologres) is an Alibaba Cloud OLAP database compatible with the PostgreSQL ecosystem. Daft provides a first-class Hologres integration with high-throughput Arrow reads and stream-COPY upsert writes.

## Installing Daft with Hologres Support

Install Daft with the `daft[hologres]` extra, or manually install the required packages: [psycopg](https://www.psycopg.org/), [connectorx](https://sfu-db.github.io/connector-x/), [SQLAlchemy](https://www.sqlalchemy.org/) and [sqlglot](https://sqlglot.com/).

```bash
pip install -U "daft[hologres]"
```

## Connecting

Use `Catalog.from_hologres()` with a standard PostgreSQL connection string (Hologres uses the PostgreSQL wire protocol; the default port is 80):

=== "🐍 Python"

    ```python
    import daft
    from daft.catalog import Catalog

    catalog = Catalog.from_hologres("postgresql://user:password@host:80/mydb")

    schemas = catalog.list_namespaces()
    tables = catalog.list_tables()
    table = catalog.get_table("public.my_table")
    ```

The catalog supports the full namespace/table lifecycle: `create_namespace`, `create_table`, `drop_table`, `has_table`, `list_tables` and so on. Logical partition tables are listed once (their child partitions are hidden) and their partition column is exposed through table metadata.

## Creating Tables

`create_table` maps Daft types to Hologres types and accepts Hologres-specific table options:

=== "🐍 Python"

    ```python
    schema = daft.Schema._from_field_name_and_types(
        [
            ("id", daft.DataType.int64()),
            ("name", daft.DataType.string()),
            ("score", daft.DataType.float64()),
            ("meta", daft.DataType.struct({"k": daft.DataType.string()})),  # stored as JSONB
            ("ds", daft.DataType.string()),
        ]
    )

    catalog.create_table(
        "public.my_table",
        schema,
        properties={
            "primary_key": "id",                 # str or list; required for upserts
            "distribution_key": "id",
            "orientation": "column",             # row | column | rowcolumn
            "clustering_key": "id",
            "time_to_live_in_days": 30,          # emitted as time_to_live_in_seconds
            "index": "name",                     # full-text inverted index (CREATE INDEX ... USING fulltext)
            "partition_by": "ds",                # logical partition table (PARTITION BY LIST)
            "table_properties": {                # free-form SET_TABLE_PROPERTY passthrough
                "bitmap_columns": "name",
            },
        },
    )
    ```

Notes:

- Hologres requires the logical partition column to be part of the primary key; Daft appends it automatically.
- `shard_count` is a table-group-level setting on Hologres and cannot be set per table.
- The `proxima_vectors` option exists for instances with the vector extension; availability depends on the deployment.

## Reading

=== "🐍 Python"

    ```python
    # High-level: Hologres Arrow COPY reads with pushdowns and parallelism
    df = table.read()
    df = daft.read_hologres("public.my_table", conn_uri)

    # Options
    df = table.read(
        read_mode="arrow",            # "arrow" (default) or "select" (generic SQL path)
        shard_parallelism=True,       # split reads by physical shard (hg_shard_id)
        partition_col="ds",           # or split by partition-column bounds
        num_partitions=4,
        partition_bound_strategy="min-max",  # or "percentile"
        disable_pushdowns=False,      # skip filter/limit pushdowns (shard/partition splits stay enabled)
    )
    ```

The Arrow path decodes `COPY (...) TO STDOUT WITH (FORMAT ARROW)` directly into RecordBatches with projection/predicate pushdowns applied to the inner query. `json`/`jsonb` projections automatically fall back to the `select` path (the Arrow export emits those as raw datum bytes); they read back as JSON strings.

## Writing

=== "🐍 Python"

    ```python
    table.append(df)                       # PK tables: upsert (ON CONFLICT UPDATE)
    table.append(df, on_conflict="ignore") # or "replace"/"none"
    table.overwrite(df)                    # TRUNCATE, then write
    ```

Write options:

| Option | Values | Default | Notes |
|---|---|---|---|
| `copy_mode` | `stream` / `bulk_load` / `stage` | `stream` | stream = fixed plan, row-level locks, upserts; bulk_load = table lock, max throughput (on_conflict needs >= 3.1); stage = Arrow upload to an internal stage + one `INSERT ... SELECT` (needs >= 4.1) |
| `copy_format` | `binary` / `text` | `binary` | text is the compatibility fallback; writes touching `float4[]` columns automatically use text |
| `on_conflict` | `update` / `ignore` / `replace` / `none` | `update` (with PK) | `none` raises on PK conflicts |

Writing to a logical partition table creates child partitions automatically for the partition values present in the data.

## Transactions

Hologres supports neither multi-statement DML transactions nor mixing DDL with DML in one transaction. Daft therefore runs every connection in autocommit mode and never opens explicit transactions, so all writes are individually committed.

## Generic SQL APIs

The PostgreSQL-generic Daft APIs also work against Hologres:

=== "🐍 Python"

    ```python
    # Read with the generic SQL reader (connectorx or SQLAlchemy drivers)
    df = daft.read_sql("SELECT * FROM public.my_table WHERE id < 10", uri)

    # Write with the generic pandas/SQLAlchemy path (default schema only)
    df.write_sql("my_table", "postgresql+psycopg://user:password@host:80/mydb", write_mode="fail")
    ```

For large data prefer the native Hologres path (`table.read()` / `table.append()`); the generic paths trade throughput for compatibility.

## Type Mapping

| Daft | Hologres | Notes |
|---|---|---|
| int8/int16 | smallint | |
| int32 / int64 | integer / bigint | |
| uint* | nearest signed | may overflow |
| float32 / float64 | real / double precision | |
| decimal128(p,s) | numeric(p,s) | precision required |
| string | text | |
| binary | bytea | |
| date / timestamp | date / timestamp | |
| timestamp (tz) | timestamptz | server stores milliseconds; reads are UTC |
| list (primitive) | t[] | |
| list\<float32\>, embedding | float4[] | vector indexes via table properties where supported |
| struct / map / tensor / image | jsonb | reads back as JSON strings |
