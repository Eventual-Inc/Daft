# Managing Memory Usage

Daft is a [streaming execution engine](../architecture/index.md) where data flows through the pipeline defined by your query. Most operators process data in bounded batches, but some steps can inflate data or materialize large intermediate results. When those steps outgrow available memory, workloads slow down, spill to disk, or fail with out-of-memory (OOM) errors. This page walks through the most common sources of memory pressure and the tuning levers you can use to keep pipelines stable.

## Frequent Sources of Memory Pressure

The majority of OOM incidents we observe come from a handful of patterns:

- **User-defined functions (UDFs):** Python or library calls that expand data in-memory (e.g. decoding, image manipulation, large tensors).
- **URL or object store downloads:** Highly concurrent downloads queue large responses before downstream operators consume them.
- **Inflationary operators:** Steps like decoding, exploding, or generating embeddings increase the size of each record.
- **Materializing operators:** Aggregations, sorts, joins, and other shuffles need to hold working sets in memory to produce a final result.

Understanding which category your pipeline falls into guides which knobs to adjust.

## Techniques to Reduce Memory Footprint

### Tune UDF Batch Size and Concurrency

- Lower the `batch_size` argument on map-like APIs so each invocation handles less data at once.
- Cap UDF concurrency if each invocation is memory hungry. Fewer concurrent tasks often outperforms repeated worker restarts caused by OOM kills.

### Limit Download Concurrency

For remote reads (HTTP, S3, GCS, etc.), reduce the maximum number of in-flight connections. Smaller download pools slow peak throughput slightly but keep memory bounded while responses are buffered.

### Control Read Parallelism

Source readers accept concurrency controls so you can match inflight data to available memory. Dial back parallel reads when working with extremely wide or inflationary schemas.

### Batch Before Inflationary Operators

Daft parallelizes work according to batch size. If the default batch size is too large, explicitly insert `df.into_batches(...)` ahead of decoding, exploding, or other expansion steps. Smaller batches prevent any single task from growing beyond memory limits.

### Experiment with Execution Configs (Use Caution)

Advanced execution options in `daft.api.config` expose finer-grained controls (e.g. default batch sizing, operator-specific buffers). These settings are subject to change between releases, so record any overrides you rely on and review them during upgrades.

### Scale Cluster Resources

If tuning alone is insufficient:

- Move to machines with more memory per CPU.
- On the Ray runner or Kubernetes, scale out to more workers or increase the number of partitions so each worker handles less data. See [Kubernetes](../distributed/kubernetes.md), [Ray](../distributed/ray.md), and [Partitioning and Batching](partitioning.md) for guidance.

## Monitor Out-of-Core Processing

When running on Ray, Daft supports [out-of-core processing](https://en.wikipedia.org/wiki/External_memory_algorithm) through Ray object spilling. When the working set exceeds memory, Ray writes data to disk and later re-reads it. You will see log messages similar to:

```
(raylet, ip=xx.xx.xx.xx) Spilled 16920 MiB, 9 objects, write throughput 576 MiB/s.
```

Spilling keeps jobs alive but increases latency and can exhaust disk:

- Heavy spilling (hundreds of GiB) risks filling disks, leading to eviction by your cloud provider.
- Spilling large volumes of data can dominate runtime.

Mitigations include provisioning nodes with more memory, adding nodes to the cluster, or using local NVMe SSDs for faster spill throughput. Consult the [Ray object spilling guide](https://docs.ray.io/en/latest/ray-core/objects/object-spilling.html) for additional tuning.

## Handling Persistent OOM Errors

Even with the interventions above, you might still encounter `OOMKilled` processes locally, `Workers ... killed due to memory pressure (OOM)` on Ray, or pod restarts in orchestrators such as Kubernetes. When that happens:

- Filter early (`df.where(...)`) to avoid reading unnecessary data.
- Request additional memory for heavy UDFs (see [Resource Requests](../custom-code/udfs.md#resource-requests)).
- Repartition into more (smaller) pieces or insert `df.into_batches(...)` to shrink per-task memory needs.
- Re-run workloads with the [Ray](../distributed/ray.md) runner on a larger cluster if you started locally.

If workloads continue to fail, we may be under-estimating required memory or you may have discovered a gap in our heuristics. Please reach out on [Slack](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg) or [open a GitHub issue](https://github.com/Eventual-Inc/Daft/issues/new/choose) so we can help investigate.
