# Join Ordering: Findings & Remediation Plan

## Correctness Issues

### 1. Column-alias projections block join reordering
**File**: `join_graph.rs:783-786`

`process_node` checks reorderability of projections with
`matches!(e.as_ref(), Expr::Column(_))`. This rejects `Alias(Column(_), _)` expressions
(simple renames like `col("a").alias("b")`), even though `process_linear_chain` already
handles renames correctly via `input_mapping()`. Result: any rename between joins causes
the entire subtree below it to become a leaf relation, preventing reordering.

Two tests are `#[ignore]`d because of this (`test_create_join_graph_multiple_renames`,
`test_create_join_graph_with_non_join_projections_and_filters`).

**Fix**: Change the reorderability check to `e.input_mapping().is_some()`, which returns
true for columns and column aliases but not for computed projections.

### 2. Computed projections between joins block reordering
**File**: `join_graph.rs:780-791`

Even with fix #1, computed projections (e.g. `double + double`) between joins still block
reordering. The second ignored test (`test_create_join_graph_with_non_join_projections_and_filters`)
requires a proper projection pushup pass that separates computed projections from column
renames before graph construction.

### 3. Joins nested under non-reorderable operators are never optimized
**File**: `mod.rs:34-64`

After `build_logical_plan` replaces a join subtree, the optimizer never recurses into the
children of the newly built plan. Joins nested under aggs or outer joins within the
reordered subtree are skipped. The TODO at line 39-52 acknowledges this.

### ~~Transitive edge inference~~
Originally flagged but verified to be correct. The recursive `add_bidirectional_edge`
calls compute the full transitive closure regardless of edge insertion order.

## Refactoring Opportunities

### 4. Separate graph representation from graph building
`JoinAdjList` mixes construction (equivalence sets, domain estimation) with querying
(`get_connections`). DP-ccp needs efficient connected-subgraph-complement enumeration,
which benefits from a clean graph abstraction with bitmask-based node sets.

### 5. Use bitmasks instead of Vec<usize> for relation sets
With max 7 relations today, a u8 bitmask suffices. DP-ccp fundamentally needs bitmask
enumeration. Introducing bitmasks now makes the DP-ccp implementation straightforward.

## Remediation Order
1. Fix alias projections blocking reordering (#1) -- correctness, un-ignores first test
2. Implement projection pushup (#2) -- correctness, un-ignores second test
3. Recurse into children after reordering (#3) -- missed optimization
4. Refactor graph to use bitmasks (#4, #5) -- refactor (pre-DP-ccp)
5. Implement DP-ccp behind JoinOrderer trait
