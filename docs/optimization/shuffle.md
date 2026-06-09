# Shuffle Algorithms

A *shuffle* is the all-to-all data movement behind [`df.repartition(...)`][daft.DataFrame.repartition], hash joins, sorts, and groupbys.

The `shuffle_algorithm` config option controls how Daft executes that movement, and the right choice depends on how big the shuffle is. Shuffles only happen on the distributed (Ray) runner — the native (single-machine) runner executes the entire query in one process and has no shuffle step.

If you're picking a partition count for `repartition` or thinking about batch size, start with [Partitioning and Batching](partitioning.md). Partition count is the input to shuffle cost, and `into_batches` controls the batch sizes shuffles produce.

> **TL;DR**
>
> - Stay on the default `shuffle_algorithm="auto"` for most queries. Daft picks between `map_reduce` and `pre_shuffle_merge` based on partition count.
> - If your shuffle is **>10 GB of data** or **>500K partition slots** (`input_partitions × output_partitions`), switch to `flight_shuffle`. Daft prints a hint in the query plan when it sees one.
> - When you enable `flight_shuffle`, point `flight_shuffle_dirs` at a fast local disk. The default is `["/tmp"]`. Spill files are `lz4`-compressed by default; set `flight_shuffle_compression="zstd"` on EBS or other networked volumes.

## Shuffle algorithms

`shuffle_algorithm` takes four values: `auto` (the default) and three concrete algorithms. `auto` selects between `map_reduce` and `pre_shuffle_merge` at plan time; any of the three concrete options can also be set directly.

| `shuffle_algorithm` | Data plane | Best for |
|---|---|---|
| `map_reduce` | Ray object store, one object per `(input, output)` slot | Small to medium shuffles with moderate partition counts. |
| `pre_shuffle_merge` | Ray object store, with input partitions merged first to reduce slot count | Shuffles where `input_partitions × output_partitions` is large but total bytes are moderate. |
| `flight_shuffle` | Local disk plus Arrow Flight gRPC between workers | Large shuffles (≳ 10 GB or thousands of partitions on each side). Avoids the head-node bookkeeping cost. |

### How `auto` chooses

Under `auto`, Daft picks between `map_reduce` and `pre_shuffle_merge` based on the geometric mean of input and output partition counts. If `sqrt(input_partitions × output_partitions) > pre_shuffle_merge_partition_threshold` (default `200`), Daft uses `pre_shuffle_merge`; otherwise `map_reduce`.

`auto` does not switch to `flight_shuffle` automatically. Instead, when Daft sees a shuffle likely to hit the object-store ceiling (input size ≥ 10 GiB or partition product ≥ 500,000), it prints a hint in the query plan with the configuration to enable.

### Why `map_reduce` falls over at scale

`map_reduce` writes one Ray object per `(input, output)` slot, and each tracked object costs about 3 KB of metadata on the Ray driver. Multiply by `M` mappers × `N` reducers:

| Mappers × Reducers | Slots  | Head-node metadata |
|---|---|---|
| 1024 × 1024 | 1.0M  | ~3 GB   |
| 2048 × 2048 | 4.2M  | ~12 GB  |
| 4096 × 4096 | 16.8M | ~50 GB  |
| 8192 × 8192 | 67M   | ~200 GB |

At `4096 × 4096` the driver holds 50 GB of pointers before any data has moved, which usually shows up as a head-node OOM or as a scheduler stall.

`pre_shuffle_merge` reduces this cost by coalescing small input partitions before the shuffle, lowering `M`, but it can't change the underlying `M × N` shape. `flight_shuffle` writes shuffle bytes to local disk and serves them between workers over Arrow Flight, reducing head-node cost from `M × N × 3 KB` to roughly `(M + N) × 200 B` of descriptors.

Symptoms that point to `flight_shuffle`:

- Head node OOM, or high memory pressure on the head node.
- Slow scheduling of tasks, with workers idle.
- A high volume of Ray object store spill messages in the worker logs.

## Turning on `flight_shuffle`

```python
import daft

daft.context.set_execution_config(
    shuffle_algorithm="flight_shuffle",
    flight_shuffle_dirs=["/mnt/nvme0", "/mnt/nvme1"],  # round-robins across them
    flight_shuffle_compression="lz4",                   # the default; set "zstd" on slower disk
)
```

This applies to every shuffle in the session.

### `flight_shuffle_dirs`

Local directories where Daft writes shuffle spill files. Defaults to `["/tmp"]`.

- **Point this at the fastest local disk you have.** On AWS that means the local NVMe on instances like `i8ge.*` or `i4i.*`. On Kubernetes it's whatever is mounted from node-local SSD.
- **Give it more than one device when you can.** Daft round-robins writes across the list, so two NVMe volumes roughly double aggregate write bandwidth.
- **Size it for the shuffle, not the dataset.** Plan for `dataset_size ÷ compression_ratio` of free space per node. A 10 TB shuffle across 32 workers is about 310 GB of spill per worker uncompressed; at the default `lz4` (~2×) it's closer to 155 GB, and at `zstd` (~3×) closer to 100 GB.
- Daft cleans the dirs up when the query exits.

### `flight_shuffle_compression`

Arrow IPC compression for the spill files. One of `"lz4"` (the default), `"zstd"`, or `"none"`.

| Storage | Recommended | Why |
|---|---|---|
| Local NVMe | `"lz4"` (default) | Cheap enough on CPU that it wins even when disk isn't the ceiling — about 10% in our benchmarks. |
| gp3 EBS or network-attached | `"zstd"` | When bandwidth is the limit, compression is the biggest knob available — worth ~2.3× over uncompressed — and zstd's tighter ratio consistently beats lz4. |
| HDD or slow shared FS, or a network-constrained cluster | `"zstd"` | Same reasoning: bytes are the bottleneck. |

Set `"none"` only if you're CPU-bound on very fast storage — for example, several local NVMe drives whose aggregate bandwidth outruns what the CPU can compress through.

## Choosing between algorithms

For most queries, leave `shuffle_algorithm="auto"`. Override when:

- **`auto` printed a `flight_shuffle` hint in your query plan.** Enable `flight_shuffle` and set `flight_shuffle_dirs` to a fast local disk.
- **A large shuffle is OOMing the head node or stalling the scheduler, with no hint shown.** The hint thresholds are conservative; switch to `flight_shuffle` anyway.
- **You want to compare the object-store paths directly.** Set `shuffle_algorithm="map_reduce"` for small partition counts, or `"pre_shuffle_merge"` when the partition product is large but total bytes are moderate. These are mainly useful for benchmarking.

If you're unsure whether your shuffle has crossed the thresholds, run it once with `auto` and read the hint in the query plan.

## Related

- [Partitioning and Batching](partitioning.md): how to pick the number of partitions for `repartition` (the input to shuffle cost) and how `into_batches` controls batch sizes within a partition.
- [Managing Memory Usage](memory.md): general memory tuning, including reducer-side memory.
- [Join Strategies](join-strategies.md): hash joins are one of the main shuffle producers. Covers when each join strategy triggers one.
