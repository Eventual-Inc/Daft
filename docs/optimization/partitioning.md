# Partitioning and Batching

Daft runs as both a single-node and a distributed dataframe. In distributed environments, Daft partitions data across workers so the cluster can evaluate your query in parallel. Within each worker, Daft further processes records in batches to use the machine's cores efficiently. This guide explains how partitions and batches interact, how Daft chooses default values, and which APIs to reach for when you need to adjust them.

> **TL;DR**
>
> - On Ray or other distributed runners, manage cross-worker parallelism with `df.into_partitions(...)` or `df.repartition(...)`.
> - On the native runner, there are no partitions—use `df.into_batches(...)` and related options to control parallelism.

## Partitioning: Parallelism Across Workers

When you run on a cluster such as Ray, Daft represents your dataframe as partitions. Each partition is scheduled on a worker, giving the cluster a natural unit of parallelism and a boundary for memory usage.

- **Default sizing:** Daft estimates partition sizes when you create a dataframe. File readers (Parquet, CSV, JSON, ...) group small files and split large ones so partitions fit comfortably in memory.
- **Dynamic repartitioning:** Global operators such as joins, sorts, and grouped aggregations shuffle data to satisfy their clustering requirements, which can change both the number of partitions and how rows are distributed across them.
- **Heuristics:** A good starting point is at least `2 × total_worker_cpus` partitions so every slot in the cluster has work. Increase partitions when you hit memory pressure; decrease them if shuffles spend more time moving metadata than data.

You can inspect the current partitioning with [`df.explain(show_all=True)`][daft.DataFrame.explain].

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

Although this example reads 100 files, Daft coalesces them into 3 partitions because the files are small.

## Batching: Parallelism Within a Worker

After a worker receives a partition, it pipelines work in batches. Batch size dictates how much data each core processes at once:

- On the native runner (`DAFT_RUNNER=native` or `daft.context.set_runner_native()`), batching is the only lever for parallelism. There are no partitions to manage.
- On distributed runners, partitions determine how many workers run concurrently, while batches control concurrency within each worker.

Smaller batches reduce peak memory usage at the cost of more scheduling overhead. This trade-off is especially important before inflationary steps like image decoding or embedding generation.

## Adjusting Partitions and Batches

Use the following APIs to reshape your workload:

- `df.repartition(...)` builds a new hash-partitioned layout, moving data between workers. Use it when correctness requires specific clustering (e.g. downstream group-by or join keys).
- `df.into_partitions(...)` splits or merges adjacent partitions without a full shuffle. It is the lightweight way to scale partition counts up or down.
- `df.into_batches(...)` sets the batch size for subsequent operators, shrinking the per-task memory footprint before decode, explode, or UDF-heavy stages.

Many global operators implicitly change partitioning and batch size. After any significant transformation, re-run `df.explain(show_all=True)` to confirm Daft's inferred layout matches your expectations.

## Choosing the Right Strategy

- **Distributed runners (Ray, Kubernetes):** Start with Daft's defaults, then tune partitions to balance CPU utilization and memory headroom. Combine `into_partitions()` with targeted `into_batches()` calls before memory-intensive operators to avoid spilling or OOMs. Consult the [Ray](../distributed/ray.md) and [Kubernetes](../distributed/kubernetes.md) guides for cluster-level scaling tips.
- **Native runner:** Focus on batching. Add `into_batches()` before UDFs, downloads, or decoding steps to bound the data each core processes at once. If you later move to Ray, you can pair the same batching strategy with additional partition tuning.

For additional memory-focused tips, see [Managing Memory Usage](memory.md).
