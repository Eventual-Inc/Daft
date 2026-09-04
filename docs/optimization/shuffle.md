# Shuffle Algorithms

A *shuffle* is the all-to-all data movement behind [`df.repartition(...)`][daft.DataFrame.repartition], hash joins, sorts, and groupbys.

The `shuffle_algorithm` config option controls how Daft executes that movement, and the right choice depends on how big the shuffle is. Shuffles only happen on the distributed (Ray) runner — the native (single-machine) runner executes the entire query in one process and has no shuffle step.

If you're picking a partition count for `repartition` or thinking about batch size, start with [Partitioning and Batching](partitioning.md). Partition count is the input to shuffle cost, and `into_batches` controls the batch sizes shuffles produce.

> **TL;DR**
>
> - Stay on the default `shuffle_algorithm="auto"` for most queries. Daft picks between `map_reduce` and `pre_shuffle_merge` based on partition count.
> - If your shuffle is **>10 GB of data** or **>500K partition slots** (`input_partitions × output_partitions`), switch to `flight_shuffle`. Daft prints a hint in the query plan when it sees one.
> - When you enable `flight_shuffle`, point `flight_shuffle_dirs` at a fast local disk. The default is `["/tmp"]`. Spill files are `lz4`-compressed by default; set `flight_shuffle_compression="zstd"` on EBS or other networked volumes.
> - If your cluster has a shared POSIX filesystem and you are losing workers mid-query, set `flight_shuffle_placement="shared_only"` so a lost worker's partitions stay readable.

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

## Writing shuffle data to a shared filesystem

By default `flight_shuffle` keeps map output on node-local disks and serves it to other workers over gRPC. That makes a partition reachable only while the worker that wrote it is alive: if that worker dies after finishing its map task, the data and the in-memory index describing it are both gone, and the query fails.

If your cluster has a cache-coherent POSIX filesystem every node can see — Lustre (including FSx for Lustre), GPFS, BeeGFS, CephFS — you can write shuffle data there instead:

```python
daft.context.set_execution_config(
    shuffle_algorithm="flight_shuffle",
    flight_shuffle_placement="shared_only",
    flight_shuffle_shared_dir="/mnt/shared",   # must be set in the same call
)
```

This buys two things:

- **Any node can read any partition directly**, without proxying through the writing worker. That removes a network hop and the writer's CPU from the read path.
- **Losing a worker stops being fatal.** If a gRPC fetch fails before returning data, the reduce task finishes the read from the shared copy instead.

The trade is write bandwidth: every node now writes to one filesystem, so the shared mount's aggregate write throughput becomes a cluster-wide ceiling rather than a per-node one. Measure that ceiling before moving a large shuffle onto it.

!!! warning "NFS needs mount options"

    Plain NFS is not cache-coherent: a file another node just created can stay invisible for up to `acdirmax` (60 s by default) because of negative directory-entry caching, and a reduce task that opens it in that window fails with "file not found". If you must use NFS, mount the shuffle directory with `lookupcache=positive` (or `actimeo=0`). Data visibility itself is safe — Daft closes every file before publishing its name, which is what NFS's close-to-open semantics require.

!!! note "Applies to repartition-style shuffles"

    Shared placement covers the shuffles that dominate large queries — `repartition`, `df.shuffle()`, and the exchanges inside joins, sorts, and aggregations. Gather and `into_partitions` always write node-locally, because their per-partition layout has no on-disk index for a peer to resolve.

### What the format guarantees

Distributed shuffles are correct only if every row is read exactly once, and the shared layout is built so that nothing short of that can pass silently:

- **Each attempt of a map task writes its own file.** Ray reports a worker as unavailable on a transient error and Daft re-dispatches the task, but the original attempt may still be running — even in the same process. The two attempts are not interchangeable: random repartitioning assigns rows by arrival order, and upstream operators may be nondeterministic. So every file name carries an attempt token, the coordinator records which attempt's output it accepted, and every reader — shared mount, gRPC, or in-process — asks for exactly that attempt. A superseded attempt's output is never addressed.
- **Every partition range is checksummed.** With the default background durability the file is published before it is `fsync`ed. If the writer node dies in that window, the filesystem keeps whichever pages reached it; a hole in a record batch decodes as zeros, and a hole on a message boundary looks like end-of-stream. Both would be wrong answers, not errors. The index therefore stores a CRC-32 per partition, and a reader that finds a short, holed, or altered range fails the task instead of returning what it has. Every route checks: reading the mount directly, fetching over gRPC, and serving in-process all verify the same length and the same checksum, so a shuffle cannot become correct or incorrect depending on which way it was read. Output written by the per-partition layout (gather, `into_partitions`) carries no checksum, and is instead required to end at the writer's end-of-stream marker — a file that was never finished is rejected rather than read as one with fewer batches.
- **Requests are all-or-nothing.** A gRPC request for refs a worker does not hold is refused outright rather than answered with the subset it does hold, so a reader can never mistake a partial response for a complete one.
- **Shuffle identities are unique across drivers.** Two Daft processes sharing a cluster and a mount would otherwise generate the same shuffle directory for their first query and overwrite — and clean up — each other's data.

What is *not* covered: a map attempt that dies before its file is published has left no copy anywhere, so the query fails; recomputing it from lineage is future work.

### `flight_shuffle_shared_durability`

How hard the map side works to make a shared write survive losing its writer. One of `"background"` (the default), `"none"`, or `"sync"`.

This knob exists because `fsync` on a shared filesystem is often far more expensive than its local equivalent, and frequently *size-independent* — on the Lustre deployment this was developed against, a single `fsync` cost about a second whether the file was 0 B or 64 MiB, and only ~25 completed per second even at 32-way concurrency. Paying that on the map task's critical path cut write throughput from ~610 MiB/s to 140–340 MiB/s.

The way out is that visibility and durability are separable. A reduce task reading from a *live* writer needs only visibility, which the filesystem's close-to-open coherency already provides for free. `fsync` only matters for the narrower case where the writer node dies with data still in its page cache.

| Value | Behavior | Use when |
|---|---|---|
| `"background"` (default) | Publishes the file immediately, then `fsync`s off the critical path | Almost always. Full write throughput, with durability following shortly behind. |
| `"none"` | Never `fsync`s | You would rather re-run the query than pay for durability at all. A shared copy can be lost if its writer node dies. |
| `"sync"` | `fsync`s before publishing, so a visible file is a durable one | A filesystem with cheap `fsync`, or a job where losing a worker mid-query is expensive enough to justify several-fold slower writes. |

Both levels that promise anything sync twice: once for the file's bytes, and once for the directory that names it. A map file is published by renaming it into place, and syncing a file commits its data and inode but not the directory entry pointing at them — so without the second sync a crash could leave durable bytes with nothing reaching them, which is indistinguishable from never having written the file. `"sync"` pays for both before the map task returns; `"background"` pays for both on the background thread.

### `flight_shuffle_read_source`

How *this worker* fetches partitions written by others. One of `"auto"` (the default), `"rpc"`, or `"shared"`. Unlike placement, this is a per-worker setting: which route is faster depends on the reader's own link to its peers versus to the shared mount.

- `"auto"` reads the shared mount directly when the data is there, and uses gRPC otherwise — falling back to whichever route it did not pick if the first one fails. Which route it *prefers* is still fixed rather than measured; this is the mode that will grow adaptive routing (weighing gRPC against the mount per worker).
- `"rpc"` always tries gRPC first.
- `"shared"` always reads the shared mount for shuffles written there. It requires `flight_shuffle_placement="shared_only"`. Shuffles that are always node-local (gather, `into_partitions`) are read over gRPC regardless, since they have no shared copy.

`"auto"` falls back in both directions: to the shared mount when a gRPC fetch fails, and to gRPC when the mount fails. Either route holds the same bytes, so a route being unavailable is not a reason to fail a query — a lost worker cannot interrupt a reduce task, and neither can a mount that will not serve a file while its writer is alive. `"rpc"` falls back to the mount but not the reverse (it never reads the mount first), and `"shared"` does not fall back at all, since its whole purpose is to say whether the mount can do the job.

Fallback only applies before the first batch of a read. Once batches have been handed downstream, re-reading the same refs by another route would deliver them twice, so a mid-stream failure surfaces as an error and the task is retried instead.

### `flight_shuffle_shared_read_concurrency`

How many map files a reduce task reads from the shared mount at once. Defaults to 16.

This is deliberately higher than `scantask_max_parallel` (8). Shared-mount reads are dominated by per-file round trips rather than by bytes: opening a file and reading its index costs a few milliseconds regardless of how much data follows. A reduce task with thousands of map inputs will sit on that latency floor unless it keeps many reads in flight. Raise it further on high-latency mounts.

It bounds one reduce task's reads for one shuffle, and does so regardless of how many workers wrote the files — the mount does not care who the writer was. A process-wide ceiling sits above it so that several concurrent reduce tasks cannot multiply into an open-file count that exhausts the node or buries the mount's metadata server; that ceiling is set well clear of any sensible value here and is not configurable.

### Sizing check: keep per-partition reads large

Each reduce task reads one byte range per map file, averaging roughly `total_shuffle_bytes ÷ (input_partitions × output_partitions)`. Shared filesystems fall off a cliff when that range gets small — on the reference Lustre mount, 4 MiB ranges read at 430 MiB/s per stream but 64 KiB ranges managed only 22.8 MiB/s.

Before moving a large shuffle to shared placement, check that figure. A 1 TB shuffle with 10,000 input and 8,192 output partitions gives ~13 KiB per range, which will read badly. Reduce the partition product — usually by lowering the output partition count — until the average range is comfortably above 1 MiB.

## Choosing between algorithms

For most queries, leave `shuffle_algorithm="auto"`. Override when:

- **`auto` printed a `flight_shuffle` hint in your query plan.** Enable `flight_shuffle` and set `flight_shuffle_dirs` to a fast local disk.
- **A large shuffle is OOMing the head node or stalling the scheduler, with no hint shown.** The hint thresholds are conservative; switch to `flight_shuffle` anyway.
- **You want to compare the object-store paths directly.** Set `shuffle_algorithm="map_reduce"` for small partition counts, or `"pre_shuffle_merge"` when the partition product is large but total bytes are moderate. These are mainly useful for benchmarking.
- **Workers are being lost mid-query, or you have a fast shared filesystem.** Add `flight_shuffle_placement="shared_only"` on top of `flight_shuffle`. See [Writing shuffle data to a shared filesystem](#writing-shuffle-data-to-a-shared-filesystem).

If you're unsure whether your shuffle has crossed the thresholds, run it once with `auto` and read the hint in the query plan.

## Related

- [Partitioning and Batching](partitioning.md): how to pick the number of partitions for `repartition` (the input to shuffle cost) and how `into_batches` controls batch sizes within a partition.
- [Managing Memory Usage](memory.md): general memory tuning, including reducer-side memory.
- [Join Strategies](join-strategies.md): hash joins are one of the main shuffle producers. Covers when each join strategy triggers one.
