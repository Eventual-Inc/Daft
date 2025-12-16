# Reading from and Writing to PostgreSQL

[PostgreSQL](https://www.postgresql.org/) is an open-source object-relational database system known for its reliability, feature robustness, and performance.

## Installing Daft with PostgreSQL Support

Install Daft with the `daft[postgres]` extra, or manually install the required packages: [psycopg](https://www.psycopg.org/) and [pgvector](https://github.com/pgvector/pgvector-python) for vector support.

```bash
pip install -U "daft[postgres]"
```

## Tutorial

### Reading a Table

To read from PostgreSQL tables, use Daft's [Catalog API](../api/catalogs_tables.md). Daft provides high-level APIs for connecting to PostgreSQL databases and accessing tables as DataFrames.

To connect to a PostgreSQL database, provide a connection string to `Catalog.from_postgres()` and a list of PostgreSQL extensions to install. By default, the pgvector `vector` extension is automatically installed if available for vector support.

=== "🐍 Python"

    ```python
    import daft
    from daft.catalog import Catalog

    # Connect to PostgreSQL database
    # Note: The pgvector extension is enabled by default for vector support
    catalog = Catalog.from_postgres("postgresql://user:password@localhost:5432/mydb", extensions=["vector"])

    # List available schemas
    schemas = catalog.list_namespaces()
    print(schemas)

    # List tables in a schema
    tables = catalog.list_tables()
    print(tables)
    ```

After connecting to your database, reading a table is extremely easy:

=== "🐍 Python"

    ```python
    # Get a table from the catalog
    table = catalog.get_table("public.users")

    # Read the table as a DataFrame
    df = table.read()
    df.show()
    ```

Daft supports parallel reads by partitioning on a column:

=== "🐍 Python"

    ```python
    # Read with partitioning for better performance on large tables
    df = table.read(partition_col="id", num_partitions=4)
    df.show()
    ```

### Writing to a Table

To write DataFrames to PostgreSQL tables, you can create tables and use the append or overwrite operations.

=== "🐍 Python"

    ```python
    # Create a sample DataFrame
    df = daft.from_pydict({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 28],
        "score": [95.5, 87.2, 91.8, 88.9]
    })

    # Create a table in PostgreSQL
    table = catalog.create_table("public.scores", df.schema())

    # Write data to the table
    table.append(df)
    ```

You can also overwrite existing tables:

=== "🐍 Python"

    ```python
    # Overwrite the table with new data
    new_df = daft.from_pydict({
        "id": [5, 6],
        "name": ["Eve", "Frank"],
        "age": [32, 29],
        "score": [92.1, 89.3]
    })

    table.overwrite(new_df)
    ```

## Type System

| Daft Type | PostgreSQL Type |
|-----------|-----------------|
| [`daft.DataType.bool()`][daft.datatype.DataType.bool] | `BOOLEAN` |
| [`daft.DataType.int8()`][daft.datatype.DataType.int8] | `SMALLINT` |
| [`daft.DataType.int16()`][daft.datatype.DataType.int16] | `SMALLINT` |
| [`daft.DataType.int32()`][daft.datatype.DataType.int32] | `INTEGER` |
| [`daft.DataType.int64()`][daft.datatype.DataType.int64] | `BIGINT` |
| [`daft.DataType.float32()`][daft.datatype.DataType.float32] | `REAL` |
| [`daft.DataType.float64()`][daft.datatype.DataType.float64] | `DOUBLE PRECISION` |
| [`daft.DataType.decimal128(precision, scale)`][daft.datatype.DataType.decimal128] | `NUMERIC` |
| [`daft.DataType.string()`][daft.datatype.DataType.string] | `TEXT` |
| [`daft.DataType.binary()`][daft.datatype.DataType.binary] | `BYTEA` |
| [`daft.DataType.date()`][daft.datatype.DataType.date] | `DATE` |
| [`daft.DataType.timestamp(timeunit="us", timezone=None)`][daft.datatype.DataType.timestamp] | `TIMESTAMP` |
| [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`][daft.datatype.DataType.timestamp] | `TIMESTAMPTZ` |
| [`daft.DataType.list(element_type)`][daft.datatype.DataType.list] | `element_type[]` |
| [`daft.DataType.struct(fields)`][daft.datatype.DataType.struct] | `JSONB` |
| [`daft.DataType.map(key_type, value_type)`][daft.datatype.DataType.map] | `JSONB` |
| [`daft.DataType.embedding(size)`][daft.datatype.DataType.embedding] | `VECTOR(size)` |

## Reference

Daft provides high-level [Session](../api/sessions.md) and [Catalog](../api/catalogs_tables.md) APIs for reading and writing PostgreSQL tables. However, the [`Catalog.from_postgres`][daft.catalog.Catalog.from_postgres] and table operations are the primary entry points for PostgreSQL interactions.

### Catalog Operations

The PostgreSQL catalog supports the full range of catalog operations:

- **Namespaces (Schemas):** `create_namespace()`, `drop_namespace()`, `list_namespaces()`, `has_namespace()`
- **Tables:** `create_table()`, `create_table_if_not_exists()`, `drop_table()`, `get_table()`, `has_table()`, `list_tables()`, `read_table()`, `write_table()`

=== "🐍 Python"

    ```python
    # Create a new schema
    catalog.create_namespace("analytics")

    # Create a table with custom properties, enabling Row Level Security (default behavior)
    schema = daft.Schema.from_pydict({
        "user_id": daft.DataType.int64(),
        "event": daft.DataType.string(),
    })

    table = catalog.create_table(
        "analytics.events",
        schema,
        properties={"enable_rls": True}
    )
    ```

### Table Operations

PostgreSQL tables support read, append, and overwrite operations:

=== "🐍 Python"

    ```python
    # Read operations
    df = table.read()
    df_partitioned = table.read(partition_col="id", num_partitions=4)

    # Write operations
    table.append(df)
    table.overwrite(df)
    ```

## FAQs

1. **Does Daft support PostgreSQL extensions?**

    Yes! Daft can automatically install and configure PostgreSQL extensions if they're available. The pgvector extension `vector` is installed by default, if available.

2. **How does Daft handle PostgreSQL data types?**

    Daft provides comprehensive type mapping between Daft's Arrow-based types and PostgreSQL types. Complex types like arrays and JSON are supported, with fallbacks to JSONB for unsupported structures.

3. **Does Daft support PostgreSQL Row Level Security?**

    Yes, Daft automatically enables Row Level Security on newly created tables for enhanced security. This can be controlled via the `enable_rls` property when creating tables.

4. **How does Daft handle PostgreSQL schemas and search paths?**

    Daft respects PostgreSQL's schema search path. When no schema is specified, PostgreSQL uses its configured search path to resolve table names.

5. **Can Daft work with PostgreSQL in distributed environments?**

    Yes, when using Daft's distributed runner, PostgreSQL writes can be distributed across multiple machines for maximum performance.

6. **How does Daft handle vector embeddings in PostgreSQL?**

    Daft has native support for pgvector's VECTOR type. Embedding columns are automatically converted between Daft's embedding type and PostgreSQL's vector type during reads and writes.
