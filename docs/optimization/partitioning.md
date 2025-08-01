# Partitioning

Daft is a **distributed** dataframe. This means internally, data is represented as partitions which are then spread out across your system.

## Why do we need partitions?

When running in a distributed settings (a cluster of machines), Daft spreads your dataframe's data across these machines. This means that your workload is able to efficiently utilize all the resources in your cluster because each machine is able to work on its assigned partition(s) independently.

Additionally, certain global operations in a distributed setting requires data to be partitioned in a specific way for the operation to be correct, because all the data matching a certain criteria needs to be on the same machine and in the same partition. For example, in a groupby-aggregation Daft needs to bring together all the data for a given key into the same partition before it can perform a definitive local groupby-aggregation which is then globally correct. Daft refers to this as a "clustering specification", and you are able to see this in the plans that it constructs as well.

!!! note "Note"

    When running locally on just a single machine, Daft does not use partitions. Instead of using partitioning to control parallelism, the local execution engine performs a streaming-based execution on small "morsels" of data, which provides much more stable memory utilization while improving the user experience with not having to worry about partitioning.

    Daft's local execution engine, the native runner, is enabled by default. You can also explicitly enable it with the `DAFT_RUNNER=native` environment variable or using [`daft.context.set_runner_native()`][daft.context.set_runner_native].

This user guide helps you think about how to correctly partition your data to improve performance as well as memory stability in Daft.

General rule of thumb:

1. **Have Enough Partitions**: our general recommendation for high throughput and maximal resource utilization is to have *at least* `2 x TOTAL_NUM_CPUS` partitions, which allows Daft to fully saturate your CPUs.

2. **More Partitions**: if you are observing memory issues (excessive spilling or out-of-memory (OOM) issues) then you may choose to increase the number of partitions. This increases the amount of overhead in your system, but improves overall memory stability (since each partition will be smaller).

3. **Fewer Partitions**: if you are observing a large amount of overhead (e.g. if you observe that shuffle operations such as joins and sorts are taking too much time), then you may choose to decrease the number of partitions. This decreases the amount of overhead in the system, at the cost of using more memory (since each partition will be larger).

!!! tip "See Also"

    [Managing Memory Usage](memory.md) - a guide for dealing with memory issues when using Daft

## How is my data partitioned?

Daft will automatically use certain heuristics to determine the number of partitions for you when you create a DataFrame. When reading data from files (e.g. Parquet, CSV or JSON), Daft will group small files/split large files appropriately
into nicely-sized partitions based on their estimated in-memory data sizes.

To interrogate the partitioning of your current DataFrame, you may use the [`df.explain(show_all=True)`][daft.DataFrame.explain] method. Here is an example output from a simple `df = daft.read_parquet(...)` call on a fairly large number of Parquet files.

=== "🐍 Python"

    ```python
    df = daft.read_parquet("s3://bucket/path_to_100_parquet_files/**")
    df.explain(show_all=True)
    ```

``` {title="Output"}

    == Unoptimized Logical Plan ==

    * GlobScanOperator
    |   Glob paths = [s3://bucket/path_to_100_parquet_files/**]
    |   ...


    ...


    == Physical Plan ==

    * TabularScan:
    |   Num Scan Tasks = 3
    |   Estimated Scan Bytes = 72000000
    |   Clustering spec = { Num partitions = 3 }
    |   ...
```

In the above example, the call to [`daft.read_parquet()`][daft.read_parquet] read 100 Parquet files, but the Physical Plan indicates that Daft will only create 3 partitions. This is because these files are quite small (in this example, totalling about 72MB of data) and Daft recognizes that it should be able to read them as just 3 partitions, each with about 33 files each!

## How can I change the way my data is partitioned?

You can change the way your data is partitioned by leveraging certain DataFrame methods:

1. [`daft.DataFrame.repartition()`][daft.DataFrame.repartition]: repartitions your data into `N` partitions by performing a hash-bucketing that ensure that all data with the same values for the specified columns ends up in the same partition. Expensive, requires data movement between partitions and machines.

2. [`daft.DataFrame.into_partitions()`][daft.DataFrame.into_partitions]: splits or coalesces adjacent partitions to meet the specified target number of total partitions. This is less expensive than a call to `df.repartition()` because it does not require shuffling or moving data between partitions.

3. Many global dataframe operations such as [`daft.DataFrame.join()`][daft.DataFrame.join], [`daft.DataFrame.sort()`][daft.DataFrame.sort] and [`daft.GroupedDataframe.agg()`][daft.dataframe.GroupedDataFrame.agg] will change the partitioning of your data. This is because they require shuffling data between partitions to be globally correct.

Note that many of these methods will change both the *number of partitions* as well as the *clustering specification* of the new partitioning. For example, when calling `df.repartition(8, col("x"))`, the resultant dataframe will now have 8 partitions in total with the additional guarantee that all rows with the same value of `col("x")` are in the same partition! This is called "hash partitioning".

=== "🐍 Python"

    ```python
    df = df.repartition(8, daft.col("x"))
    df.explain(show_all=True)
    ```

``` {title="Output"}

    == Unoptimized Logical Plan ==

    * Repartition: Scheme = Hash
    |   Num partitions = Some(8)
    |   By = col(x)
    |
    * GlobScanOperator
    |   Glob paths = [s3://bucket/path_to_1000_parquet_files/**]
    |   ...

    ...

    == Physical Plan ==

    * ReduceMerge
    |
    * FanoutByHash: 8
    |   Partition by = col(x)
    |
    * TabularScan:
    |   Num Scan Tasks = 3
    |   Estimated Scan Bytes = 72000000
    |   Clustering spec = { Num partitions = 3 }
    |   ...
```
