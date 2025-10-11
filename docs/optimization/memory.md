# Managing Memory Usage

Daft is a [streaming execution engine](../architecture/index.md) where data flows through the pipeline defined by your query. Operators process data in bounded batches, but some steps can inflate data or materialize large intermediate results. When those steps outgrow available memory, workloads slow down, spill to disk, or fail with out-of-memory (OOM) errors. This page walks through the most common sources of memory pressure and the tuning levers you can use to keep pipelines stable.

## Frequent Sources of Memory Pressure

The majority of OOM incidents we observe come from a handful of patterns:

- **User-defined functions (UDFs):** Python or library functions use a lot of memory, such as when doing model inference.
- **URL or object store downloads:** Highly concurrent downloads queue large responses in memory before downstream operators consume them.
- **Inflationary operators:** Operations like decoding, decompression, or exploding increase the size of each batch.
- **Materializing operators:** Aggregations, sorts, and joins need to hold intermediate data in memory to produce a final result.

Understanding which operations your query contains can guide you to tuning the proper parameters.

## Techniques to Reduce Memory Footprint

### Tune UDF Batch Size and Concurrency

- Lower the [`batch_size`][daft.udf.UDF.batch_size] argument on the UDF so each invocation handles less data at once.
- Cap UDF [`concurrency`][daft.udf.UDF.concurrency] if each invocation is memory hungry. Fewer concurrent tasks often outperforms repeated worker restarts caused by OOM kills.

### Limit Download Concurrency

For remote reads (HTTP, S3, GCS, etc.), reduce the `max_connections` parameter when using [`download()`][daft.functions.download]. Lower download concurrency can keep memory bounded while allowing downstream operators to continue consuming data.


### Batch Before Inflationary Operators

Daft parallelizes work according to batch size. If the default batch size  is too large, explicitly insert [`df.into_batches(...)`][daft.DataFrame.into_batches] ahead of decoding, exploding, or other expansion steps. Smaller batches prevent any single task from growing beyond memory limits.

### Experiment with Execution Configs (Use Caution)

!!! warning "Experimental Feature"

    Execution configuration parameters are experimental and subject to change between releases. Use with caution in production environments.

Advanced [execution parameters][daft.context.set_execution_config] in `daft.context` expose finer-grained controls (e.g. default batch sizing, target partition sizes, operator-specific buffers). These settings are subject to change between releases, so record any overrides you rely on and review them during upgrades.

### Scale Resources

If tuning alone is insufficient:

- Move to machines with more memory per CPU.
- On the Kubernetes or Ray, scale out to more workers or increase the number of partitions so each worker handles less data. See [Kubernetes](../distributed/kubernetes.md), [Ray](../distributed/ray.md), and [Partitioning and Batching](partitioning.md) for guidance.


If workloads continue to fail, we may be under-estimating required memory or you may have discovered a gap in our heuristics. Please reach out on [Slack](https://daft.ai/slack) or [open a GitHub issue](https://github.com/Eventual-Inc/Daft/issues/new/choose) so we can help investigate.
