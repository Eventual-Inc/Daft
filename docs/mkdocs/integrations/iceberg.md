# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open-sourced table format originally developed at Netflix for large-scale analytical datasets.

Daft currently natively supports:

1. **Distributed Reads:** Daft will fully distribute the I/O of reads over your compute resources (whether Ray or on multithreading on the local PyRunner)
2. **Skipping Filtered Data:** Daft uses [`df.where(...)`](../{{ api_path }}/dataframe_methods/daft.DataFrame.where.html) filter calls to only read data that matches your predicates
3. **All Catalogs From PyIceberg:** Daft is natively integrated with PyIceberg, and supports all the catalogs that PyIceberg does

## Reading a Table

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

## Writing to a Table

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

Daft and Iceberg have compatible type systems. Here are how types are converted across the two systems.

When reading from an Iceberg table into Daft:

| Iceberg               | Daft                          |
| --------------------- | ----------------------------- |
| **Primitive Types** |
| `boolean` | [`daft.DataType.bool()`](../api_docs/datatype.html#daft.DataType.bool) |
| `int` | [`daft.DataType.int32()`](../api_docs/datatype.html#daft.DataType.int32) |
| `long` | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64) |
| `float` | [`daft.DataType.float32()`](../api_docs/datatype.html#daft.DataType.float32) |
| `double` | [`daft.DataType.float64()`](../api_docs/datatype.html#daft.DataType.float64) |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`](../api_docs/datatype.html#daft.DataType.decimal128) |
| `date` | [`daft.DataType.date()`](../api_docs/datatype.html#daft.DataType.date) |
| `time` | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `timestampz`| [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](../api_docs/datatype.html#daft.DataType.string) |
| `uuid` | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary) |
| `fixed(L)` | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary)
| `binary` | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary) |
| **Nested Types** |
| `struct(fields)` | [`daft.DataType.struct(fields)`](../api_docs/datatype.html#daft.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](../api_docs/datatype.html#daft.DataType.list) |
| `map(K, V)` | [`daft.DataType.struct({"key": K, "value": V})`](../api_docs/datatype.html#daft.DataType.struct) |

## Roadmap

Here are some features of Iceberg that are works-in-progress:

1. Reading Iceberg V2 equality deletes
2. More extensive usage of Iceberg-provided statistics to further optimize queries
3. Copy-on-write and merge-on-read writes

A more detailed Iceberg roadmap for Daft can be found on [our Github Issues page](https://github.com/Eventual-Inc/Daft/issues/2458).
