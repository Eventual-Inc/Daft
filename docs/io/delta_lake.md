# Delta Lake

[Delta Lake](https://delta.io/) is an open-source storage framework for data analytics on data lakes. It provides ACID transactions, scalable metadata handling, and a unification of streaming and batch data processing, all on top of Parquet files in cloud storage.

Daft currently supports:

1. **Parallel + Distributed Reads:** Daft parallelizes Delta Lake table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the [distributed Ray runner](../distributed.md).

2. **Skipping Filtered Data:** Daft ensures that only data that matches your [`df.where()`][daft.DataFrame.where] filter will be read, often skipping entire files/partitions.

3. **Multi-cloud Support:** Daft supports reading Delta Lake tables from AWS S3, Azure Blob Store, and GCS, as well as local files.

A detailed Delta Lake roadmap for Daft can be found on [our Github issues](https://github.com/Eventual-Inc/Daft/issues/2457). For the overall Daft development plan, see [Daft Roadmap](../roadmap.md).

## Installing Daft with Delta Lake Support

Daft internally uses the [deltalake](https://pypi.org/project/deltalake/) Python package to fetch metadata about the Delta Lake table, such as paths to the underlying Parquet files and table statistics. The `deltalake` package therefore must be installed to read Delta Lake tables with Daft, either manually or with the below `daft[deltalake]` extras install of Daft.

```bash
pip install -U "daft[deltalake]"
```

## Reading a Table

A Delta Lake table can be read by providing [`daft.read_deltalake`][daft.read_deltalake] with the URI for your table.

The below example uses the [deltalake](https://pypi.org/project/deltalake/) Python package to create a local Delta Lake table for Daft to read, but Daft can also read Delta Lake tables from all of the major cloud stores.

=== "🐍 Python"

    ```python
    # Create a local Delta Lake table.
    from deltalake import write_deltalake
    import pandas as pd

    df = pd.DataFrame({
        "group": [1, 1, 2, 2, 3, 3, 4, 4],
        "num": list(range(8)),
        "letter": ["a", "b", "c", "d", "e", "f", "g", "h"],
    })

    # This will write out separate partitions for group=1, group=2, group=3, group=4.
    write_deltalake("some-table", df, partition_by="group")
    ```

After writing this local example table, we can easily read it into Daft.

=== "🐍 Python"

    ```python
    # Read a Delta Lake table
    import daft

    df = daft.read_deltalake("some-table")
    ```

## Data Skipping Optimizations

Subsequent filters on the partition column `group` will efficiently skip data that doesn't match the predicate. In the below example, the `group != 2` partitions (files) will be pruned, i.e. they will never be read into memory.

=== "🐍 Python"

    ```python
    # Filter on partition columns will result in efficient partition pruning; non-matching partitions will be skipped.
    df2 = df.where(df["group"] == 2)
    df2.show()
    ```

Filters on non-partition columns will still benefit from automatic file pruning via file-level statistics. In the below example, the `group=2` partition (file) will have `2 <= df["num"] <= 3` lower/upper bounds for the `num` column, and since the filter predicate is `df["num"] < 2`, Daft will prune the file from the read. Similar is true for `group=3` and `group=4` partitions, with none of the data from those files being read into memory.

=== "🐍 Python"

    ```python
    # Filter on non-partition column, relying on file-level column stats to efficiently prune unnecessary file reads.
    df3 = df.where(df["num"] < 2)
    df3.show()
    ```

## Write to Delta Lake

You can use [`df.write_deltalake()`][daft.DataFrame.write_deltalake] to write a DataFrame to a Delta table:

=== "🐍 Python"

    ```python
    df.write_deltalake("tmp/daft-recordbatch", mode="overwrite")
    ```

Daft supports multiple write modes. See the API docs for [`df.write_deltalake()`][daft.DataFrame.write_deltalake] for more details.

## Type System

Daft and Delta Lake have compatible type systems. Here are how types are converted across the two systems.

When reading from a Delta Lake table into Daft:

| Delta Lake                          | Daft                                                                                         |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `BOOLEAN`                           | [`daft.DataType.bool()`][daft.datatype.DataType.bool]                                        |
| `BYTE`                              | [`daft.DataType.int8()`][daft.datatype.DataType.int8]                                        |
| `SHORT`                             | [`daft.DataType.int16()`][daft.datatype.DataType.int16]                                      |
| `INT`                               | [`daft.DataType.int32()`][daft.datatype.DataType.int32]                                      |
| `LONG`                              | [`daft.DataType.int64()`][daft.datatype.DataType.int64]                                      |
| `FLOAT`                             | [`daft.DataType.float32()`][daft.datatype.DataType.float32]                                  |
| `DOUBLE`                            | [`daft.DataType.float64()`][daft.datatype.DataType.float64]                                  |
| `DECIMAL(precision, scale)`         | [`daft.DataType.decimal128(precision, scale)`][daft.datatype.DataType.decimal128]            |
| `DATE`                              | [`daft.DataType.date()`][daft.datatype.DataType.date]                                        |
| `TIMESTAMP_NTZ`                     | [`daft.DataType.timestamp(timeunit="us", timezone=None)`][daft.datatype.DataType.timestamp]  |
| `TIMESTAMP`                         | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`][daft.datatype.DataType.timestamp] |
| `STRING`                            | [`daft.DataType.string()`][daft.datatype.DataType.string]                                    |
| `BINARY`                            | [`daft.DataType.binary()`][daft.datatype.DataType.binary]                                    |
| `MAP<key_type, value_type>`         | [`daft.DataType.map(key_type, value_type)`][daft.datatype.DataType.map]                      |
| `STRUCT<[field_name: field_type,]>` | [`daft.DataType.struct(fields)`][daft.datatype.DataType.struct]                              |
| `ARRAY<element_type>`               | [`daft.DataType.list(element_type)`][daft.datatype.DataType.list]                            |

References:

* [Python `unitycatalog` type name code reference](https://github.com/unitycatalog/unitycatalog-python/blob/main/src/unitycatalog/types/table_info.py)
* [Spark types documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)
* [Databricks types documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes)
