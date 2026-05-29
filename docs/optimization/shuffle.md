# Shuffle Algorithms

A *shuffle* is the all-to-all data movement behind [`df.repartition(...)`][daft.DataFrame.repartition], hash joins, sorts, and cross-cluster group-bys. With `M` input partitions and `N` output partitions it is `M × N` logical transfers — modest at `64 × 64`, 16.7 million at `4096 × 4096`. How Daft executes that movement is controlled by the `shuffle_algorithm` config option, and the right choice depends on how big the shuffle is.

This page covers when each algorithm applies and how to tune the disk-based one. If you're here because you're picking a partition count for `repartition` or thinking about batch size, start with [Partitioning and Batching](partitioning.md) — partition count is the input to shuffle cost, and `into_batches` controls the units shuffles produce.

> **TL;DR**
>
> - Stay on the default `shuffle_algorithm="auto"` for typical jobs — Daft picks between `map_reduce` and `pre_shuffle_merge` based on partition count.
> - If your shuffle is **>10 GB of data** or **>500K partition slots** (`input_partitions × output_partitions`), switch to `flight_shuffle`. Daft prints a hint in the query plan when it sees one.
> - When you enable `flight_shuffle`, point `flight_shuffle_dirs` at fast local disk — it defaults to `["/tmp"]`, which is rarely the right choice on a real cluster. Enable `flight_shuffle_compression="zstd"` on EBS or other networked volumes.

## The four algorithms

All shuffle algorithms are only used by the distributed (Ray) runner. The native runner shuffles in-memory inside one process and ignores this setting.

| `shuffle_algorithm` | Data plane | Best for |
|---|---|---|
| `auto` *(default)* | `map_reduce` or `pre_shuffle_merge`, chosen at plan time | Most jobs. Hints at `flight_shuffle` when the shuffle gets too big. |
| `map_reduce` | Ray object store, one object per `(input, output)` slot | Small-to-medium shuffles (≲ a few GB) with modest partition counts. |
| `pre_shuffle_merge` | Ray object store, but inputs are merged first to cut slot count | Shuffles where `input_partitions × output_partitions` is large but total bytes are still moderate. |
| `flight_shuffle` | Local disk + Arrow Flight gRPC between workers | Large shuffles (≳ 10 GB or thousands of partitions on each side). Avoids the head-node bookkeeping wall. |

### `auto` — what it actually does

Under `auto`, Daft chooses between `map_reduce` and `pre_shuffle_merge` based on the geometric mean of input and output partition counts. If `sqrt(input_partitions × output_partitions) > pre_shuffle_merge_partition_threshold` (default `200`), Daft uses `pre_shuffle_merge`; otherwise `map_reduce`.

`auto` does not automatically switch to `flight_shuffle`, because `flight_shuffle` requires the user to choose where spill files go (`flight_shuffle_dirs`). Instead, when Daft sees a shuffle likely to hit the object-store ceiling — input size ≥ 10 GiB or partition product ≥ 500,000 — it prints a hint in the query plan with the configuration to enable.

### Why `map_reduce` falls over at scale

`map_reduce` writes one Ray object per `(input, output)` slot, and each tracked object costs about 3 KB of metadata on the Ray driver. Multiply that by `M × N`:

| Mappers × Reducers | Slots  | Head-node metadata |
|---|---|---|
| 1024 × 1024 | 1.0M  | ~3 GB   |
| 2048 × 2048 | 4.2M  | ~12 GB  |
| 4096 × 4096 | 16.8M | ~50 GB  |
| 8192 × 8192 | 67M   | ~200 GB |

At `4096 × 4096` the driver holds 50 GB of pointers before any data has moved, which typically manifests as a head-node OOM or as a scheduler stall before workers receive work. `pre_shuffle_merge` reduces this cost by coalescing small input partitions before the shuffle, lowering `M`, but it cannot change the underlying `M × N` shape. `flight_shuffle` writes shuffle bytes to local disk and serves them between workers over Arrow Flight, which reduces head-node cost from `M × N × 3 KB` to roughly `(M + N) × 200 B` of descriptors.

If you see any of these symptoms on a large shuffle, you're almost certainly hitting the object-store ceiling and should switch to `flight_shuffle`:

- Head node OOMs or runs out of memory before workers are saturated.
- The job hangs in "scheduling" with workers idle.
- `df.repartition(N, ...)` with `N` in the thousands silently never starts moving data.

## Turning on `flight_shuffle`

```python
import daft

daft.context.set_execution_config(
    shuffle_algorithm="flight_shuffle",
    flight_shuffle_dirs=["/mnt/nvme0", "/mnt/nvme1"],  # round-robins across them
    flight_shuffle_compression=None,                    # set to "lz4" or "zstd" on slower disk
)
```

This applies to every shuffle in the session. There is no per-DataFrame override.

### `flight_shuffle_dirs`

Local directories where Daft writes shuffle spill files (one combined Arrow IPC file per map task). Defaults to `["/tmp"]`, which works for small experiments but is rarely the right choice on a real cluster.

- **Point this at the fastest local disk you have.** On AWS that's the local NVMe on instances like `i8ge.*` / `i4i.*`; on Kubernetes it's whatever's mounted from the underlying node-local SSD.
- **Give it more than one device when you can.** Daft round-robins writes across the list, so two NVMe volumes roughly double aggregate write bandwidth.
- **Size it for the shuffle, not the dataset.** Plan for `dataset_size ÷ compression_ratio` of free space per node. A 10 TB shuffle uncompressed across 32 workers is ~310 GB of spill per worker; at `zstd` 3× it's closer to 100 GB.
- Daft cleans the dirs up when the query exits. There is no manual cleanup step.

### `flight_shuffle_compression`

Arrow IPC compression for the spill files. One of `"lz4"`, `"zstd"`, or `"none"` (default).

| Storage | Recommended | Why |
|---|---|---|
| Local NVMe | `"none"` | The disk isn't the bottleneck — compression just spends CPU you'd rather give the map task. |
| gp3 EBS / network-attached | `"zstd"` | Compresses ~3× before the write, roughly tripling effective volume bandwidth. |
| HDD or slow shared FS | `"zstd"` | Same reasoning, more pronounced. |

`"lz4"` is the lighter middle ground if `zstd` shows up as CPU-bound in your profile.

## Choosing between algorithms

For most workloads, leaving `shuffle_algorithm="auto"` and letting Daft decide is the right call. Override when:

- **`auto` printed a `flight_shuffle` hint in your query plan.** Enable `flight_shuffle` and set `flight_shuffle_dirs` to a fast local disk. The hint is emitted precisely when this is the recommended path.
- **A large shuffle is OOMing the head node or stalling the scheduler, with no hint shown.** The hint thresholds are conservative; enabling `flight_shuffle` is still the right move.
- **You want to compare the object-store paths directly.** Set `shuffle_algorithm="map_reduce"` for small partition counts, or `"pre_shuffle_merge"` when the partition product is large but total bytes are moderate. These are primarily useful for benchmarking.

If you are unsure whether your shuffle has crossed the thresholds, run it once with `auto`; the hint message in the query plan will indicate whether `flight_shuffle` is recommended.

## Related

- [Partitioning and Batching](partitioning.md) — how to pick the number of partitions for `repartition` (the input to shuffle cost) and how `into_batches` controls batch sizes within a partition.
- [Managing Memory Usage](memory.md) — general memory tuning, including reducer-side memory.
- [Join Strategies](join-strategies.md) — hash joins are one of the main shuffle producers; this page explains when each join strategy triggers one.
