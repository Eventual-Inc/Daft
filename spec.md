
# Skip Unnecessary Shuffles — Pre-Partitioned / Pre-Sorted Input

## Goal

When input data is already arranged correctly, Daft unconditionally shuffles and sorts before
operations like asof joins, window functions, and groupbys. At TB scale this shuffle is often
>80% of total runtime.

The goal is to let input sources declare how their data is laid out so the planner can skip
work that is already done.

---

## Two Types of Hints

### Hash Partition Hint
> "Each task I emit already contains all rows for a given key combination."

- Non-overlapping key groups across tasks — each key value exists in exactly one task
- No ordering guarantee within a task
- Lets the planner skip **hash shuffles** before groupby, window, distinct

### Range Partition Hint
> "Each task I emit covers a non-overlapping range of values, and rows within each task are sorted."

- Non-overlapping ranges across tasks — file 1 has ts=[0–1000], file 2 has ts=[1001–2000], etc.
- Sorted within each task by the declared column(s)
- A combined guarantee: range partitioning alone does not imply sorted, and sorted alone does not
  imply non-overlapping ranges. Both must hold together.
- Lets the planner skip **range shuffles + local sorts** before asof joins

---

## Success Criteria

1. A custom `DataSource` can declare `ClusteringKeys.hash("producer", "date_hour")` and Daft
   skips the hash shuffle before downstream groupby / window / distinct operations.

2. A custom `DataSource` can declare `ClusteringKeys.range("ts_ns")` and Daft skips the sample,
   range shuffle, and local sort before asof joins.

3. Window functions consult `output_ordering` on `PipelineNodeConfig` and skip the local sort
   when the data is already in the right order.

4. In-memory DataFrames can be annotated via `df.assume_clustered_by(*cols)` as an escape hatch
   for cases where automatic inference isn't yet available.

5. File-backed sources (Iceberg, Parquet, Lance) automatically advertise their partition and sort
   metadata so users get the optimization without any manual declaration.

---

## What Is Already Done

### PR #7033 — Clustering Propagation Fix (prerequisite)

**Problem:** Clustering specs silently downgraded to `Unknown` when any projection
(`.with_column()`, `.select()`) appeared between a clustering producer and a consumer. Root cause
was name-based column matching failing on `BoundExpr` references.

**What it does:**
- Introduces `BoundClusteringSpec` — a new type parametrized over `BoundExpr` instead of
  `ExprRef`, used exclusively in the physical/distributed pipeline
- New `translate_through_projection()` method that rewrites clustering keys to reference output
  columns after a projection, or downgrades to `Unknown` only if columns are actually dropped
- New `clustering.rs` in `src/daft-distributed/src/pipeline_node/`

**Why it matters:** Without this, any projection between `assume_clustered_by` (or a declared
source) and the downstream join would silently kill the optimization. This is the foundation
everything else builds on. **Review this before #7031.**

---

### PR #7031 — Hash Partition Hints for Custom DataSources

**What it does:**
- Adds `ClusteringKeys` Python class in `daft/io/clustering.py` with `ClusteringKeys.hash(*cols)`
- Adds `get_clustering_keys() -> ClusteringKeys | None` optional hook on `DataSource` subclasses
- Threads declared keys through `PhysicalScanInfo.clustering_keys` → `ScanSourceNode` →
  `BoundClusteringSpec::Hash` on `PipelineNodeConfig`
- Fixes BoundExpr handling bugs in the repartition insertion logic in `translate.rs`
- Tests use `_has_shuffle(df)` via `df.explain(show_all=True)` to verify shuffle elision

**Key distinction:** `get_clustering_keys()` is only available on custom `DataSource` subclasses.
It is an execution-time property (how tasks emit rows in memory), not a storage property (how
files are laid out on disk). A source can be stored one way on disk but emit tasks clustered
differently depending on how `get_tasks()` is implemented.

**What it does NOT do:** Does not support range partitioning. Does not support sort order. Only
covers custom Python DataSources, not Iceberg/Parquet/Lance file-backed sources.

---

### Our PR — In-Memory Escape Hatch (`assume_clustered_by`)

**What it does:**
- Wires `InMemoryInfo.clustering_spec` through to `InMemorySourceNode::new()` — previously this
  field was always dropped and `Unknown` was hardcoded
- Adds `df.assume_clustered_by(*cols)` Python API that annotates an in-memory DataFrame's
  clustering metadata without changing execution
- Guards: only accepts column references (not arbitrary expressions), only works before any
  operations, only on in-memory sources, validates columns exist in schema

**Purpose:** Test harness for all downstream optimization work. Before Iceberg/Parquet metadata
propagation exists, tests can inject clustering metadata via this escape hatch and verify shuffle
elision fires correctly using the `_has_shuffle()` pattern from #7031.

**Relation to #7031:** `get_clustering_keys()` covers custom DataSource subclasses. `assume_clustered_by`
covers in-memory DataFrames (results of `.collect()`, `from_pydict()`, etc.) where there is no
DataSource to subclass. Different use cases, different entry points, same `PipelineNodeConfig.clustering_spec`
destination.

---

## Remaining Work

### 1. `output_ordering` on `PipelineNodeConfig` (prerequisite for all sort-related work)

**What:** Add `output_ordering: Option<Vec<(BoundExpr, SortDirection)>>` as a new field on
`PipelineNodeConfig`. This is the only place sort order can live in the physical layer.

**Propagation rules:**
- `SortNode` → sets `output_ordering`
- `FilterNode`, `ProjectNode` → propagate via a `translate_ordering` mirror of
  `translate_through_projection` from #7033
- `RepartitionHash`, `RepartitionRange` → clear `output_ordering` (shuffle destroys order)

**Files touched:**
- `src/daft-distributed/src/pipeline_node/mod.rs` — add field to `PipelineNodeConfig`
- `src/daft-distributed/src/pipeline_node/sort.rs` — set field
- `src/daft-distributed/src/pipeline_node/translate.rs` — propagate / clear through each node type

**This is pure plumbing with no behavior change.** Safe to land as a standalone PR.

---

### 2. `ClusteringKeys.range("ts_ns")` variant (extends #7031)

**What:** Extend `ClusteringKeys` with a `range` variant that asserts both non-overlapping
ranges across files and sorted order within each file.

**User-facing API:**
```python
class MySource(DataSource):
    def get_clustering_keys(self) -> ClusteringKeys | None:
        return ClusteringKeys.range("ts_ns")
```

**Implementation:**
- `daft/io/clustering.py` — add `ClusteringKeys.range(*cols)` factory method
- `src/daft-scan/src/clustering.rs` — add `Range(Vec<ExprRef>)` variant to `ClusteringKeys` enum
- `src/daft-distributed/src/pipeline_node/scan_source.rs` — handle `ClusteringKeys::Range` →
  construct `BoundClusteringSpec::Range` + set `output_ordering` on `PipelineNodeConfig`
  (requires work item 1 above)

**Note:** `ClusteringSpec::Range` already exists in the codebase with `by` and `descending`
fields. The work is exposing it through the Python API and threading it from `ClusteringKeys`.

---

### 3. Asof Join Shuffle Skip

**Current flow in `asof_join.rs`:**
```
execution_loop():
  left_materialized  = Vec<MaterializedOutput>  (one entry per file)
  right_materialized = Vec<MaterializedOutput>  (one entry per file)
  range_shuffle_and_join():
    1. sample + compute boundaries
    2. range_repartition_two_sides()   ← expensive shuffle
    3. compute_carryovers()
    4. pair by partition index → submit join tasks
```

**With range partition hint:**
```
execution_loop():
  left_materialized  = Vec<MaterializedOutput>  (one entry per file, already paired by index)
  right_materialized = Vec<MaterializedOutput>
  if needs_range_repartition(left, right, composite_key) == false:
    skip steps 1+2, use materialized outputs directly as already-paired partitions
    compute_carryovers()
    pair by file index → submit join tasks    ← same code path, smaller input
  else:
    current path unchanged
```

**Memory win:** Before: each join task buffers one full partition (N files). After: each join
task buffers one file. The `AsofJoinNode` local algorithm (hash-build + probe) is unchanged —
it just receives file-sized input instead of partition-sized input.

**Files touched:**
- `src/daft-distributed/src/pipeline_node/join/translate_asof_join.rs` — add
  `needs_range_repartition` check, dispatch to new code path
- `src/daft-distributed/src/pipeline_node/join/asof_join.rs` — add branch in
  `range_shuffle_and_join()` to skip sample + repartition when hint present

**Tests:** Use `_has_shuffle()` pattern from #7031:
```python
def _has_shuffle(df) -> bool:
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    physical = buf.getvalue().split("== Physical Plan ==")[-1]
    return "Shuffle" in physical
```

---

### 4. Window Local Sort Skip

**What:** Window functions already skip the hash shuffle when input is hash-partitioned
(existing `needs_hash_repartition` check in `translate.rs`). The missing piece is skipping the
local sort when the data is already in the right order.

**Change:** In `window.rs`, before inserting the local sort plan node, check
`child.config().output_ordering`. If it satisfies the window's `order_by`, skip the sort.

**Requires:** Work item 1 (`output_ordering` on `PipelineNodeConfig`).

**Files touched:**
- `src/daft-distributed/src/pipeline_node/window.rs`

---

### 5. Automatic Source Metadata Propagation (file-backed sources)

**What:** Iceberg, Parquet, and Lance all expose partitioning and sort order metadata. Today
`ScanSourceNode::new()` hardcodes `ClusteringSpec::Unknown` regardless. This work reads the
metadata and populates `ClusteringSpec` + `output_ordering` automatically so users get the
optimization without any manual declaration.

**Per source:**
- **Iceberg:** `Table.spec()` → hash/range clustering, `Table.sort_order()` → output_ordering
- **Parquet:** `sorting_columns` metadata from Parquet footer → `ClusteringSpec::Range` +
  `output_ordering`
- **Lance:** partition spec → `ClusteringSpec::Hash`

**Files touched:**
- `src/daft-distributed/src/pipeline_node/scan_source.rs` — read metadata instead of hardcoding
  `Unknown`
- Per-format scan task construction — surface partition spec and sort order fields

**Note:** GlobScan (`read_parquet("s3://bucket/*.parquet")`) stays `Unknown` — individual files
may be sorted but there is no global partition guarantee across files.

---

## Blast Radius Summary

| File | Change |
|---|---|
| `src/daft-distributed/src/pipeline_node/mod.rs` | Add `output_ordering` to `PipelineNodeConfig` |
| `src/daft-distributed/src/pipeline_node/sort.rs` | Set `output_ordering` |
| `src/daft-distributed/src/pipeline_node/translate.rs` | Propagate / clear `output_ordering` through each node |
| `src/daft-distributed/src/pipeline_node/window.rs` | Skip local sort when `output_ordering` satisfied |
| `src/daft-distributed/src/pipeline_node/join/translate_asof_join.rs` | Add range partition check |
| `src/daft-distributed/src/pipeline_node/join/asof_join.rs` | Skip sample + repartition when range hint present |
| `src/daft-distributed/src/pipeline_node/in_memory_source.rs` | Wire `InMemoryInfo.clustering_spec` through ✓ done |
| `src/daft-scan/src/clustering.rs` | Add `Range` variant to `ClusteringKeys` |
| `daft/io/clustering.py` | Add `ClusteringKeys.range()` factory |
| `src/daft-distributed/src/pipeline_node/scan_source.rs` | Read source metadata instead of hardcoding `Unknown` |
| `src/daft-logical-plan/src/builder/mod.rs` | `assume_clustered_by` ✓ done |
| `daft/dataframe/dataframe.py` | `df.assume_clustered_by()` ✓ done |

---

## Key Design Decisions

**Why two separate hints and not one?**
Hash and range partitioning serve different operations. Hash partition hints skip hash shuffles
(groupby, window, distinct). Range partition hints skip range shuffles + local sorts (asof join).
They are orthogonal properties and a source can declare both independently.

**Why is range partitioning a combined guarantee?**
Range partitioned (non-overlapping ranges) alone does not imply sorted within each file. Sorted
within each file alone does not imply non-overlapping ranges. Both must hold together for the
asof join optimization to be safe. `ClusteringSpec::Range` in the codebase already encodes both
via its `by` and `descending` fields.

**Why does `assume_clustered_by` exist alongside `get_clustering_keys()`?**
`get_clustering_keys()` is a hook on custom `DataSource` subclasses — it cannot be called on
arbitrary DataFrames. `assume_clustered_by` covers in-memory DataFrames (results of `.collect()`,
`from_pydict()`, etc.) where there is no DataSource to subclass. It also serves as the test
harness for all downstream optimization work before real source metadata propagation is built.

**Review order for PRs:**
1. #7033 first — it's the foundation, introduces `BoundClusteringSpec` used by everything else
2. #7031 second — builds on #7033's types for custom datasource hash partitioning
3. Our PR — in-memory escape hatch, independent of both but rebases cleanly on top
