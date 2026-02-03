# Skill: Distributed Scaling in Daft

This guide provides practical recipes for scaling single-node Daft workflows to distributed execution on a Ray cluster.

## Purpose

To enable an AI programming assistant or developer to efficiently convert a Daft script from local (`native`) execution to distributed execution. This involves understanding when and how to configure the runner and apply partitioning strategies.

## When to Use

- When a Daft script runs too slowly on a single machine.
- When data size exceeds the memory capacity of a single node.
- When you need to leverage a multi-node Ray cluster for parallel processing.

## How it Works

Daft uses a pluggable runner system. The `native` runner executes on a single node. The `ray` runner distributes work across a Ray cluster. Scaling a workflow involves two primary steps:

1.  **Configuring the Runner:** Setting the `DAFT_RUNNER` environment variable or using `daft.set_runner_ray()` to instruct Daft to use the Ray cluster.
2.  **Partitioning Data:** Strategically splitting the DataFrame into partitions to control parallelism and data distribution.

### Key APIs: `repartition` vs `into_batches`

-   `df.repartition(num_partitions, "col_a", ...)`: Splits the DataFrame into a **fixed number of partitions**. This is the primary tool for distributed scaling. It redistributes data across the cluster, which involves a network shuffle. Use it to control the units of work across your workers.
-   `df.into_batches(num_rows)`: Splits each existing partition into smaller **size-based batches** (by number of rows). This happens *within* each worker and does not trigger a full network shuffle. It's useful for managing memory for expensive operations (like UDFs) on a per-worker basis, not for initial data distribution.
-   `df.into_partitions(num_partitions)`: **Rarely needed.** This is a more primitive version of `repartition` that coalesces existing partitions into a smaller number. It's less flexible than `repartition` and doesn't support shuffling by key. Prefer `repartition` in almost all cases.

## Quick Recipes

### Recipe 1: Basic Conversion to Distributed

**Goal:** Take an existing script and run it on a connected Ray cluster.

**Steps:**
1.  Ensure your environment is configured to connect to Ray.
2.  Set the Daft runner to Ray.
3.  Add a `repartition()` call after your data loading step.

```python
import daft

# 1. Set the runner to use Ray
# This assumes Ray is running and accessible.
# For a local Ray cluster started with `ray start --head`, this may be "ray://127.0.0.1:10001"
# If no address is provided, Daft will start a new local Ray cluster.
daft.set_runner_ray()

# Load data as usual
df = daft.from_pydict({"id": list(range(100))})

# 2. Repartition the data to distribute it across the cluster
# A good starting point is the number of available CPU cores or workers.
NUM_PARTITIONS = 16 
df = df.repartition(NUM_PARTITIONS)

# ... rest of your script ...
# For example, a simple transformation
df = df.with_column("id_plus_one", df["id"] + 1)

df.collect()
```

### Recipe 2: Hash Partitioning for Joins/Groupbys

**Goal:** Optimize a join or groupby operation by co-locating related data.

**Steps:**
1.  Identify the key(s) used in the join or `groupby`.
2.  Use `repartition` with the key(s) before the operation.

```python
import daft

daft.set_runner_ray()

# Two dataframes to be joined on "user_id"
users = daft.from_pydict({"user_id": range(1000), "name": [f"user_{i}" for i in range(1000)]})
events = daft.from_pydict({"user_id": range(1000) * 2, "event": ["login", "logout"] * 1000})

NUM_PARTITIONS = 32

# Repartition BOTH dataframes on the join key
users = users.repartition(NUM_PARTITIONS, "user_id")
events = events.repartition(NUM_PARTITIONS, "user_id")

# The subsequent join will be much more efficient
joined_df = events.join(users, on="user_id")

# The same applies to groupbys
# df = df.repartition(NUM_PARTITIONS, "group_key").groupby("group_key").agg(...)

joined_df.collect()
```

## Example Prompts for ClaudeCODE

-   "Take this single-node Daft script and modify it to run on a distributed Ray cluster. Add comments explaining the changes."
-   "I have a Daft script that performs a slow join. Can you optimize it for distributed execution? The join key is `product_id`."
-   "Explain the difference between `repartition` and `into_batches` in Daft and show me an example of when to use each."

## Common Pitfalls & Checks

-   **Forgetting to Repartition:** After loading data, it might exist in a single partition. Subsequent operations will not be parallelized. Always add a `repartition` step after data loading for distributed execution.
-   **Partitions Too Large:** If partitions are too large, workers may run out of memory. Use `into_batches` to break down large partitions before memory-intensive UDFs.
-   **Partitions Too Small:** A very high number of partitions creates excessive overhead. Start with a number close to your total worker cores and adjust from there.
-   **Runner Not Set:** Ensure `daft.set_runner_ray()` is called or `DAFT_RUNNER=ray` is set in the environment. Without it, the script will run locally and `repartition` will have no effect.
-   **Shuffling Unnecessarily:** Repeatedly calling `repartition` without a clear purpose can cause expensive and unnecessary data shuffling over the network. Partition once after loading, and then only when required for an operation like a join on a different key.

## References

-   [Official Docs: Partitioning and Batching](https://docs.daft.ai/en/latest/optimization/partitioning.html)
-   [Official Docs: Running on Ray](https://docs.daft.ai/en/latest/distributed/ray.html)
