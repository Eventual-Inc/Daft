---
name: "daft-distributed-scaling"
description: "Scale Daft workflows to distributed Ray clusters. Invoke when optimizing performance or handling large data."
---

# Daft Distributed Scaling

Scale single-node workflows to distributed execution.

## Core Strategies

| Strategy | API | Use Case | Pros/Cons |
|---|---|---|---|
| **Shuffle** | `repartition(N)` | Light data (e.g. file paths), Joins | **Global balance**. High memory usage (materializes data). |
| **Streaming** | `into_batches(N)` | Heavy data (images, tensors) | **Low memory** (streaming). High scheduling overhead if batches too small. |

## Quick Recipes

### 1. Light Data: Repartitioning
Best for distributing file paths before heavy reads.

```python
# Create enough partitions to saturate workers
df = daft.read_parquet("s3://metadata").repartition(100)
df = df.with_column("data", read_heavy_data(df["path"]))
```

### 2. Heavy Data: Streaming Batches
Best for processing large partitions without OOM.

```python
# Stream 1GB partition in 64-row chunks to control memory
df = df.read_parquet("heavy_data").into_batches(64)
df = df.with_column("embed", model.predict(df["img"]))
```

## Advanced Tuning: The ByteDance Formula

Target: Keep all actors busy without OOM or scheduling bottlenecks.

### Formula 1: Repartitioning (Light Data / Paths)
Calculate the **Max Partition Count** to ensure each task has enough data to feed local actors.

1.  **Min Rows Per Partition** = `Batch Size * (Total Concurrency / Nodes)`
2.  **Max Partitions** = `Total Rows / Min Rows Per Partition`

**Example**:
- 1M rows, 4 nodes, 16 total concurrency, Batch Size 64.
- **Min Rows**: `64 * (16/4) = 256`.
- **Max Partitions**: `1,000,000 / 256 ≈ 3906`.
- *Recommendation*: Use ~1000 partitions to run multiple batches per task.

```python
df = df.repartition(1000) # Balanced fan-out
```

### Formula 2: Streaming (Heavy Data / Images)
Avoid creating tiny partitions. Use `into_batches` to stream data within larger partitions.

**Strategy**: Keep partitions large (e.g. 1GB+), use `into_batches(Batch Size)` to control memory.

```python
# Stream batches to control memory usage per actor
df = df.into_batches(64).with_column("preds", model(max_concurrency=16).predict(df["img"]))
```
