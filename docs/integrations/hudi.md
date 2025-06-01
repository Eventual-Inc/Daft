# Apache Hudi

[Apache Hudi](https://hudi.apache.org/) is an open-sourced transactional data lake platform that brings database and data warehouse capabilities to data lakes. Hudi supports transactions, efficient upserts/deletes, advanced indexes, streaming ingestion services, data clustering/compaction optimizations, and concurrency all while keeping your data in open source file formats.

Daft currently supports:

1. **Parallel + Distributed Reads:** Daft parallelizes Hudi table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the [distributed Ray runner](../distributed.md).

2. **Skipping Filtered Data:** Daft ensures that only data that matches your [`df.where()`][daft.DataFrame.where] filter will be read, often skipping entire files/partitions.

3. **Multi-cloud Support:** Daft supports reading Hudi tables from AWS S3, Azure Blob Store, and GCS, as well as local files.


A detailed Apache Hudi roadmap for Daft can be found on [our Github Issues page](https://github.com/Eventual-Inc/Daft/issues/4389). For the overall Daft development plan, see [Daft Roadmap](../roadmap.md).

## Installing Daft with Apache Hudi Support

Daft supports installing Hudi through optional dependency.

```bash
pip install -U "daft[hudi]"
```

## Reading a Table

To read from an Apache Hudi table, use the [`daft.read_hudi()`][daft.read_hudi] function. The following is an example snippet of loading an example table into Daft:

=== "🐍 Python"

    ```python
    # Read Apache Hudi table
    import daft

    df = daft.read_hudi("some-table-uri")
    df = df.where(df["foo"] > 5)
    df.show()
    ```

Currently there are limitations of reading Hudi tables:

- Only support snapshot read of Copy-on-Write tables
- Only support reading table version 5 & 6 (tables created using release 0.12.x - 0.15.x)
- Table must not have `hoodie.datasource.write.drop.partition.columns=true`

## Type System

Daft and Hudi have compatible type systems. Here are how types are converted across the two systems.

When reading from a Hudi table into Daft:

| Apachi Hudi               | Daft                          |
| --------------------- | ----------------------------- |
| **Primitive Types** |
| `boolean`                   | [`daft.DataType.bool()`][daft.datatype.DataType.bool] |
| `byte`                      | [`daft.DataType.int8()`][daft.datatype.DataType.int8] |
| `short`                     | [`daft.DataType.int16()`][daft.datatype.DataType.int16]|
| `int`                       | [`daft.DataType.int32()`][daft.datatype.DataType.int32] |
| `long`                      | [`daft.DataType.int64()`][daft.datatype.DataType.int64] |
| `float`                     | [`daft.DataType.float32()`][daft.datatype.DataType.float32] |
| `double`                    | [`daft.DataType.float64()`][daft.datatype.DataType.float64] |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`][daft.datatype.DataType.decimal128] |
| `date`                      | [`daft.DataType.date()`][daft.datatype.DataType.date] |
| `timestamp`                 | [`daft.DataType.timestamp(timeunit="us", timezone=None)`][daft.datatype.DataType.timestamp] |
| `timestampz`                | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`][daft.datatype.DataType.timestamp] |
| `string`                    | [`daft.DataType.string()`][daft.datatype.DataType.string] |
| `binary`                    | [`daft.DataType.binary()`][daft.datatype.DataType.binary] |
| **Nested Types** |
| `struct(fields)`            | [`daft.DataType.struct(fields)`][daft.datatype.DataType.struct] |
| `list(child_type)`          | [`daft.DataType.list(child_type)`][daft.datatype.DataType.list] |
| `map(K, V)`                 | [`daft.DataType.struct({"key": K, "value": V})`][daft.datatype.DataType.struct] |
