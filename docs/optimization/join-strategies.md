# Join Strategies

Daft's [df.join()][daft.DataFrame.join] supports seven join types and three execution strategies. The join type controls which rows appear in the output. The strategy controls how Daft physically executes the join - which side gets shuffled, broadcast, or sorted. Picking the right combination can mean the difference between a sub-second operation and an OOM crash.

## Join Types

| Type | SQL Equivalent | Output |
|------|---------------|--------|
| `"inner"` | `INNER JOIN` | Rows where both sides match on the join key |
| `"left"` | `LEFT OUTER JOIN` | All rows from the left, with nulls where the right has no match |
| `"right"` | `RIGHT OUTER JOIN` | All rows from the right, with nulls where the left has no match |
| `"outer"` | `FULL OUTER JOIN` | All rows from both sides, with nulls where either side has no match |
| `"semi"` | `WHERE EXISTS` | Rows from the left that have at least one match on the right |
| `"anti"` | `WHERE NOT EXISTS` | Rows from the left that have no match on the right |
| `"cross"` | `CROSS JOIN` | Cartesian product of both sides (no join key) |

Semi and anti joins are particularly useful for filtering. If you need to remove rows whose key appears in another table, an anti-join is the right tool:

```python
# Remove all rows from df whose "id" appears in ids_to_remove
df_filtered = df.join(ids_to_remove, on="id", how="anti")
```

## Column Name Conflicts

When both sides of a join share a non-key column name, Daft prepends `"right."` to the conflicting column from the right DataFrame. You can customize this with the `prefix` and `suffix` parameters:

```python
# Default: conflicting columns get "right." prefix
joined = df1.join(df2, on="key")
# Schema: key, value, right.value

# Custom suffix instead
joined = df1.join(df2, on="key", suffix="_other")
# Schema: key, value, value_other
```

## Execution Strategies

By default (`strategy=None`), Daft's query optimizer picks the strategy automatically. You can override this when you know something the optimizer doesn't.

### Hash Join (default)

Both sides are hash-partitioned on the join key and co-located. This is the general-purpose strategy and works for all join types.

```python
df.join(other, on="key", strategy="hash")
```

Both sides get shuffled, so this is the most memory-intensive strategy for large datasets. If you're hitting memory pressure on joins, see [Managing Memory Usage](memory.md) and consider whether broadcast or sort-merge might be a better fit.

### Broadcast Join

One side of the join is replicated to every worker. No shuffle needed for the other side, which makes this dramatically cheaper when one table is small enough to fit in memory on each worker.

```python
# Broadcast the small lookup table to all workers
df_large.join(df_small, on="key", strategy="broadcast")
```

Which side gets broadcast depends on the join type:

| Join Type | Broadcast Side |
|-----------|---------------|
| `"inner"` | The smaller table (auto-selected) |
| `"left"` | Right table |
| `"right"` | Left table |
| `"semi"`, `"anti"` | The filtering side (right table) |

Broadcast joins do not support outer joins.

### Sort-Merge Join

Both sides are sorted on the join key, then merged in a single linear pass. Useful when data is already sorted on the join key or when memory pressure makes the shuffle in a hash join too costly.

```python
df.join(other, on="key", strategy="sort_merge")
```

Sort-merge only supports inner joins.

## Choosing a Strategy

For most workloads, leaving `strategy=None` and letting the optimizer decide is the right call. Override when:

- **One table is much smaller than the other** (lookup tables, filter sets, dimension tables): use `strategy="broadcast"` to avoid a full shuffle. This is common in deduplication pipelines where you join a large dataset against a small set of IDs to remove.
- **Both tables are large and you're hitting memory limits**: consider whether you can restructure as a semi or anti join (which discard unneeded columns early), increase partitions via [df.repartition()][daft.DataFrame.repartition], or enable disk spilling with `DAFT_SHUFFLE_ALGORITHM=flight_shuffle`.
- **Data is pre-sorted on the join key**: `strategy="sort_merge"` can skip the partitioning step entirely for inner joins.

## Distributed Joins

When running on [Ray](../distributed/ray.md), joins that shuffle data are subject to the object store's memory limits. If your join columns don't fit in distributed memory:

1. **Try broadcast first** if one side is small enough. This avoids the shuffle entirely.
2. **Enable flight shuffle** for large-to-large joins: set `DAFT_SHUFFLE_ALGORITHM=flight_shuffle` and point it at a local volume for spilling:

```python
daft.set_execution_config(flight_shuffle_dirs=["/mnt/spill"])
```

3. **Increase partitions** to reduce per-partition memory pressure: insert a `df.repartition(n)` before the join.

For more on memory management, see [Managing Memory Usage](memory.md) and [Partitioning and Batching](partitioning.md).
