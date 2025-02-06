# Delta Lake

[Delta Lake](https://delta.io/) is an open-source storage framework for data analytics on data lakes. It provides ACID transactions, scalable metadata handling, and a unification of streaming and batch data processing, all on top of Parquet files in cloud storage.

Daft currently supports:

1. **Parallel + Distributed Reads:** Daft parallelizes Delta Lake table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the [distributed Ray runner](../distributed.md).

2. **Skipping Filtered Data:** Daft ensures that only data that matches your [`df.where(...)`](../{{ api_path }}/dataframe_methods/daft.DataFrame.where.html) filter will be read, often skipping entire files/partitions.

3. **Multi-cloud Support:** Daft supports reading Delta Lake tables from AWS S3, Azure Blob Store, and GCS, as well as local files.

## Installing Daft with Delta Lake Support

Daft internally uses the [deltalake](https://pypi.org/project/deltalake/) Python package to fetch metadata about the Delta Lake table, such as paths to the underlying Parquet files and table statistics. The `deltalake` package therefore must be installed to read Delta Lake tables with Daft, either manually or with the below `getdaft[deltalake]` extras install of Daft.

```bash
pip install -U "getdaft[deltalake]"
```

## Reading a Table

A Delta Lake table can be read by providing [`daft.read_deltalake`](../{{ api_path }}/io_functions/daft.read_deltalake.html) with the URI for your table.

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

After writing this local example table, we can easily read it into a Daft DataFrame.

=== "🐍 Python"

    ```python
    # Read Delta Lake table into a Daft DataFrame.
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

You can use [`df.write_deltalake`](../{{ api_path }}/dataframe_methods/daft.DataFrame.write_deltalake.html) to write a Daft DataFrame to a Delta table:

=== "🐍 Python"

    ```python
    df.write_deltalake("tmp/daft-table", mode="overwrite")
    ```

Daft supports multiple write modes. See the API docs for [`daft.DataFrame.write_deltalake`](../{{ api_path }}/dataframe_methods/daft.DataFrame.write_deltalake.html) for more details.

## Type System

Daft and Delta Lake have compatible type systems. Here are how types are converted across the two systems.

When reading from a Delta Lake table into Daft:

| Delta Lake               | Daft                          |
| --------------------- | ----------------------------- |
| **Primitive Types** |
| `boolean` | [`daft.DataType.bool()`](../api_docs/datatype.html) |
| `byte` | [`daft.DataType.int8()`](../api_docs/datatype.html#daft.DataType.int8) |
| `short` | [`daft.DataType.int16()`](../api_docs/datatype.html#daft.DataType.int16)|
| `int` | [`daft.DataType.int32()`](../api_docs/datatype.html#daft.DataType.int32) |
| `long` | [`daft.DataType.int64()`](../api_docs/datatype.html#daft.DataType.int64) |
| `float` | [`daft.DataType.float32()`](../api_docs/datatype.html#daft.DataType.float32) |
| `double` | [`daft.DataType.float64()`](../api_docs/datatype.html#daft.DataType.float64) |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`](../api_docs/datatype.html#daft.DataType.decimal128) |
| `date` | [`daft.DataType.date()`](../api_docs/datatype.html#daft.DataType.date) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `timestampz`| [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](../api_docs/datatype.html#daft.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](../api_docs/datatype.html#daft.DataType.string) |
| `binary` | [`daft.DataType.binary()`](../api_docs/datatype.html#daft.DataType.binary) |
| **Nested Types** |
| `struct(fields)` | [`daft.DataType.struct(fields)`](../api_docs/datatype.html#daft.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](../api_docs/datatype.html#daft.DataType.list) |
| `map(K, V)` | [`daft.DataType.struct({"key": K, "value": V})`](../api_docs/datatype.html#daft.DataType.struct) |

## Roadmap

Here are Delta Lake features that are on our roadmap. Please let us know if you would like to see support for any of these features!

1. Read support for [deletion vectors](https://docs.delta.io/latest/delta-deletion-vectors.html) ([issue](https://github.com/Eventual-Inc/Daft/issues/1954)).

2. Read support for [column mappings](https://docs.delta.io/latest/delta-column-mapping.html>) ([issue](https://github.com/Eventual-Inc/Daft/issues/1955)).

3. Writing new Delta Lake tables ([issue](https://github.com/Eventual-Inc/Daft/issues/1967)).

<!-- todo(docs - jay): ^ this needs to be updated, issue is already closed -->

4. Writing back to an existing table with appends, overwrites, upserts, or deletes ([issue](https://github.com/Eventual-Inc/Daft/issues/1968)).
