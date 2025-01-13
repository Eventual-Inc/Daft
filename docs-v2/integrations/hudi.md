# Apache Hudi

[Apache Hudi](https://hudi.apache.org/) is an open-sourced transactional data lake platform that brings database and data warehouse capabilities to data lakes. Hudi supports transactions, efficient upserts/deletes, advanced indexes, streaming ingestion services, data clustering/compaction optimizations, and concurrency all while keeping your data in open source file formats.

Daft currently supports:

1. **Parallel + Distributed Reads:** Daft parallelizes Hudi table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the [distributed Ray runner](../advanced/distributed.md).

2. **Skipping Filtered Data:** Daft ensures that only data that matches your [`df.where(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.where.html#daft.DataFrame.where) filter will be read, often skipping entire files/partitions.

3. **Multi-cloud Support:** Daft supports reading Hudi tables from AWS S3, Azure Blob Store, and GCS, as well as local files.

## Installing Daft with Apache Hudi Support

Daft supports installing Hudi through optional dependency.

```bash
pip install -U "getdaft[hudi]"
```

## Reading a Table

To read from an Apache Hudi table, use the [`daft.read_hudi`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/io_functions/daft.read_hudi.html#daft.read_hudi) function. The following is an example snippet of loading an example table:

=== "🐍 Python"

    ```python
    # Read Apache Hudi table into a Daft DataFrame.
    import daft

    df = daft.read_hudi("some-table-uri")
    df = df.where(df["foo"] > 5)
    df.show()
    ```

## Type System

Daft and Hudi have compatible type systems. Here are how types are converted across the two systems.

When reading from a Hudi table into Daft:

| Apachi Hudi               | Daft                          |
| --------------------- | ----------------------------- |
| **Primitive Types** |
| `boolean` | [`daft.DataType.bool()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.bool) |
| `byte` | [`daft.DataType.int8()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int8) |
| `short` | [`daft.DataType.int16()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int16)|
| `int` | [`daft.DataType.int32()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int32) |
| `long` | [`daft.DataType.int64()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.int64) |
| `float` | [`daft.DataType.float32()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.float32) |
| `double` | [`daft.DataType.float64()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.float64) |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.decimal128) |
| `date` | [`daft.DataType.date()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.date) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.timestamp) |
| `timestampz`| [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.string) |
| `binary` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.binary) |
| **Nested Types** |
| `struct(fields)` | [`daft.DataType.struct(fields)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.list) |
| `map(K, V)` | [`daft.DataType.struct({"key": K, "value": V})`](https://www.getdaft.io/projects/docs/en/stable/api_docs/datatype.html#daft.DataType.struct) |

## Roadmap

Currently there are limitations of reading Hudi tables

- Only support snapshot read of Copy-on-Write tables
- Only support reading table version 5 & 6 (tables created using release 0.12.x - 0.15.x)
- Table must not have `hoodie.datasource.write.drop.partition.columns=true`

Support for more Hudi features are tracked as below:

1. Support incremental query for Copy-on-Write tables [issue](https://github.com/Eventual-Inc/Daft/issues/2153)).
2. Read support for 1.0 table format ([issue](https://github.com/Eventual-Inc/Daft/issues/2152)).
3. Read support (snapshot) for Merge-on-Read tables ([issue](https://github.com/Eventual-Inc/Daft/issues/2154)).
4. Write support ([issue](https://github.com/Eventual-Inc/Daft/issues/2155)).
