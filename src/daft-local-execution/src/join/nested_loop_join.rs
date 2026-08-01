use std::{collections::HashMap, sync::{Arc, LazyLock}};

use common_display::table_display::StrValue;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::{Operator, SchemaRef, UInt64Array}};
use daft_dsl::{
    Expr, ExprRef,
    expr::{Column, bound_expr::BoundExpr},
};
use daft_geo::{get_geometry_binary, wkb_to_mbr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::{RecordBatch, nested_loop_inner_join, nested_loop_inner_join_indexed};
use rayon::prelude::*;
use rstar::{AABB, RTree, RTreeObject};

// Thread pool used for parallel R-tree probing: num_cpus/2 (min 2) so that
// probe parallelism does not starve other concurrent partition workers.
static PROBE_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let threads = (cpus / 2).max(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("daft-rtree-probe-{i}"))
        .build()
        .expect("failed to build R-tree probe thread pool")
});
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::{
        join_operator::{
            BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
            ProbeResult,
        },
        packed_rtree::PackedRTree,
    },
    pipeline::NodeName,
};

// ── R-tree index ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct RTreeEntry {
    bbox: AABB<[f64; 2]>,
    row_idx: u32,
}

impl RTreeObject for RTreeEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.bbox
    }
}

/// Concat-free global R-tree over the build side.
///
/// The original scattered build batches are kept as-is — there is NO merged
/// copy of the build side, so peak build memory is 1× (the old
/// `RecordBatch::concat` path peaked at 2× during construction and kept the
/// merged duplicate for the join's lifetime). Tree entries carry GLOBAL row
/// indices; `offsets` maps a global index back to (table, local row).
///
/// The tree itself is a [`PackedRTree`]: rstar's `bulk_load` is single-
/// threaded (~20s for 11.3M entries — over 2/3 of a full join's wall time),
/// while the packed tree builds from one rayon par_sort.
struct RTreeIndex {
    tables: Vec<RecordBatch>,
    /// `offsets[t]` = global row index of the first row of `tables[t]`;
    /// `offsets[tables.len()]` = total row count.
    offsets: Vec<u64>,
    tree: PackedRTree,
}

/// Detect precomputed bbox Float64 columns by canonical name sets.
/// `rtree_*` is the canonical set produced by `df.with_spatial_bbox()` and the
/// one preserved through a spatial join by the optimizer; `min_*`/`bbox_*` are
/// accepted for backward compatibility with externally-materialized columns.
/// All four columns must be Float64 — otherwise the WKB slow path is used
/// (never a dropped group / dropped index).
fn detect_bbox_cols(schema: &daft_core::prelude::Schema) -> Option<[usize; 4]> {
    use daft_core::prelude::DataType;
    let try_find = |name: &str| {
        schema
            .get_index(name)
            .ok()
            .filter(|&i| schema.fields()[i].dtype == DataType::Float64)
    };
    let candidates = [
        ("rtree_min_x", "rtree_min_y", "rtree_max_x", "rtree_max_y"),
        ("min_x", "min_y", "max_x", "max_y"),
        ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y"),
    ];
    candidates.iter().find_map(|(mn_x, mn_y, mx_x, mx_y)| {
        Some([
            try_find(mn_x)?,
            try_find(mn_y)?,
            try_find(mx_x)?,
            try_find(mx_y)?,
        ])
    })
}

impl RTreeIndex {
    /// Build the index over the scattered build batches without concatenating
    /// them. Returns `None` when there are no rows at all, or when the total
    /// row count exceeds `u32::MAX` (entries store global indices as u32) —
    /// callers must check those preconditions if they need the tables back.
    ///
    /// Bbox fast-path: precomputed `rtree_*`-style Float64 columns are trusted
    /// over the WKB when present. Rows with null/invalid geometry (or
    /// non-finite bbox values) get no entry but still occupy their global row
    /// index, so later entries never shift.
    fn build(tables: Vec<RecordBatch>, build_geom_col: usize) -> Option<Self> {
        let total: usize = tables.iter().map(|t| t.len()).sum();
        if tables.is_empty() || total == 0 || total > u32::MAX as usize {
            return None;
        }

        let mut offsets: Vec<u64> = Vec::with_capacity(tables.len() + 1);
        offsets.push(0);
        for t in &tables {
            offsets.push(offsets.last().unwrap() + t.len() as u64);
        }

        let bbox_cols = detect_bbox_cols(&tables[0].schema);

        // Collect entries per table in parallel (WKB MBR extraction in
        // particular is worth spreading across cores), then flatten.
        let per_table: Vec<Vec<([f64; 4], u32)>> = tables
            .par_iter()
            .enumerate()
            .map(|(t_idx, table)| {
                let base = offsets[t_idx];
                let mut entries: Vec<([f64; 4], u32)> = Vec::new();
                if let Some([ix, iy, ax, ay]) = bbox_cols {
                    // Fast path: use precomputed bbox columns (dtype checked at
                    // detection). Use .get(i) (None for nulls) so null bbox
                    // fields are skipped rather than read as 0.0.
                    let (Ok(min_x_s), Ok(min_y_s), Ok(max_x_s), Ok(max_y_s)) = (
                        table.get_column(ix).f64(),
                        table.get_column(iy).f64(),
                        table.get_column(ax).f64(),
                        table.get_column(ay).f64(),
                    ) else {
                        return entries; // unreachable: detection requires Float64
                    };
                    for i in 0..table.len() {
                        let (Some(mn_x), Some(mn_y), Some(mx_x), Some(mx_y)) =
                            (min_x_s.get(i), min_y_s.get(i), max_x_s.get(i), max_y_s.get(i))
                        else {
                            continue;
                        };
                        if mn_x.is_finite()
                            && mn_y.is_finite()
                            && mx_x.is_finite()
                            && mx_y.is_finite()
                        {
                            entries.push(([mn_x, mn_y, mx_x, mx_y], (base + i as u64) as u32));
                        }
                    }
                } else {
                    // Slow path: parse WKB bytes to extract the MBR. A table
                    // whose geometry column can't be read as binary contributes
                    // no entries (its rows could never pass the exact predicate
                    // either), but its rows still occupy global indices.
                    let Ok(binary) = get_geometry_binary(table.get_column(build_geom_col)) else {
                        return entries;
                    };
                    for i in 0..table.len() {
                        let Some(wkb) = binary.get(i) else { continue };
                        let Some([mn_x, mn_y, mx_x, mx_y]) = wkb_to_mbr(wkb) else {
                            continue;
                        };
                        entries.push(([mn_x, mn_y, mx_x, mx_y], (base + i as u64) as u32));
                    }
                }
                entries
            })
            .collect();
        let mut entries: Vec<([f64; 4], u32)> =
            Vec::with_capacity(per_table.iter().map(Vec::len).sum());
        for t in per_table {
            entries.extend(t);
        }

        Some(Self {
            tables,
            offsets,
            tree: PackedRTree::build(entries),
        })
    }

    fn total_rows(&self) -> u64 {
        *self.offsets.last().unwrap()
    }

    /// Map a global row index back to `(table_idx, local_row)`.
    fn resolve(&self, global: u64) -> (usize, u64) {
        debug_assert!(global < self.total_rows());
        // First offset strictly greater than `global` bounds its table; empty
        // tables produce repeated offsets and are correctly skipped over.
        let t = self.offsets.partition_point(|&off| off <= global) - 1;
        (t, global - self.offsets[t])
    }

    /// Gather build rows by NON-DECREASING global indices, in the given order
    /// (duplicates allowed). One `take` per touched table + a concat of only
    /// the matched slices — never a copy of the whole build side.
    fn gather(&self, sorted_globals: &[u64]) -> DaftResult<RecordBatch> {
        let mut pieces: Vec<RecordBatch> = Vec::new();
        let mut i = 0;
        while i < sorted_globals.len() {
            debug_assert!(i == 0 || sorted_globals[i - 1] <= sorted_globals[i]);
            let (t, _) = self.resolve(sorted_globals[i]);
            let start_off = self.offsets[t];
            let end_off = self.offsets[t + 1];
            let mut locals: Vec<u64> = Vec::new();
            while i < sorted_globals.len() && sorted_globals[i] < end_off {
                locals.push(sorted_globals[i] - start_off);
                i += 1;
            }
            let idx = UInt64Array::from_vec("", locals);
            pieces.push(self.tables[t].take(&idx)?);
        }
        match pieces.len() {
            0 => Ok(RecordBatch::empty(Some(self.tables[0].schema.clone()))),
            1 => Ok(pieces.pop().unwrap()),
            _ => RecordBatch::concat(&pieces),
        }
    }
}

struct RTreeState {
    index: RTreeIndex,
    build_geom_col: usize,
    probe_geom_col: usize,
}

// ── Partitioned R-tree state (one R-tree per equality-key group) ──────────

/// Per-key state: each group owns its own `RecordBatch` (only that group's
/// rows), and an R-tree whose entries index into that per-group batch.
/// This avoids a global `RecordBatch::concat` whose peak = 2× all build rows.
struct PartitionedRTreeState {
    /// key → (group's RecordBatch, R-tree indexing into it)
    groups: HashMap<String, (RecordBatch, RTree<RTreeEntry>)>,
    probe_key_col: usize,
    probe_geom_col: usize,
}

// ── Spatial function names ────────────────────────────────────────────────

const SPATIAL_FNS: &[&str] = &[
    "st_intersects", "st_contains", "st_within", "st_covers",
    "st_covered_by", "st_touches", "st_overlaps",
    "st_crosses", "st_equals", "st_dwithin",
];

/// How to evaluate the accelerated spatial node directly on parsed geometries
/// — the same per-pair semantics the expression kernel implements.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SpatialPredKind {
    Relate(daft_geo::RelatePred),
    /// `st_dwithin` with its literal distance.
    DWithin(f64),
}

fn spatial_fn_kind(name: &str, dwithin_distance: f64) -> Option<SpatialPredKind> {
    use daft_geo::RelatePred::*;
    Some(match name {
        "st_intersects" => SpatialPredKind::Relate(Intersects),
        "st_contains" => SpatialPredKind::Relate(Contains),
        "st_within" => SpatialPredKind::Relate(Within),
        "st_covers" => SpatialPredKind::Relate(Covers),
        "st_covered_by" => SpatialPredKind::Relate(CoveredBy),
        "st_touches" => SpatialPredKind::Relate(Touches),
        "st_overlaps" => SpatialPredKind::Relate(Overlaps),
        "st_crosses" => SpatialPredKind::Relate(Crosses),
        "st_equals" => SpatialPredKind::Relate(Equals),
        "st_dwithin" => SpatialPredKind::DWithin(dwithin_distance),
        _ => return None,
    })
}

/// Everything the operator needs about the ONE accelerated spatial node,
/// extracted in a single walk so no two properties can come from different
/// nodes (see the `mixed_or_and_dwithin_...` regression test).
struct AccelSpatial {
    /// Build-side-local geometry column index.
    build_col: usize,
    /// Probe-side-local geometry column index.
    probe_col: usize,
    /// R-tree query-box padding required by the node (st_dwithin's distance).
    pad: f64,
    /// Per-pair evaluation semantics of the node.
    kind: SpatialPredKind,
    /// Whether the node's arg0 is the build-side column (argument order
    /// matters for asymmetric predicates like contains/within).
    build_is_arg0: bool,
    /// The full filter minus the accelerated node: what still needs
    /// expression evaluation after direct verification. `None` when the
    /// filter IS the spatial node. Still bound to the join output schema.
    remainder: Option<ExprRef>,
}

// ── Column-index extraction (no TreeNode dependency) ─────────────────────

/// Recursively walk `expr` looking for a spatial function call.
///
/// On success returns the [`AccelSpatial`] for the SPECIFIC node that was
/// selected — column indices, pad, evaluation kind, argument order, and the
/// remaining conjuncts all describe that one node and the tree around it.
/// This is the single source of truth, so no two properties can disagree.
fn extract_from_expr(
    expr: &ExprRef,
    build_side: JoinSide,
    output_schema_len: usize,
    build_n: usize,
) -> Option<AccelSpatial> {
    match expr.as_ref() {
        Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) => {
            if !SPATIAL_FNS.contains(&sf.name()) {
                return None;
            }
            let arg0 = sf.inputs.required(0).ok()?;
            let arg1 = sf.inputs.required(1).ok()?;
            let idx0 = match arg0.as_ref() {
                Expr::Column(Column::Bound(bc)) => bc.index,
                _ => return None,
            };
            let idx1 = match arg1.as_ref() {
                Expr::Column(Column::Bound(bc)) => bc.index,
                _ => return None,
            };

            // `st_dwithin` requires the R-tree query box to be padded by its
            // distance argument on all sides, or true matches whose bboxes
            // don't already intersect would be missed. The distance must be
            // a resolvable, non-negative, finite literal — anything else
            // means the required pad is unknown, so acceleration on this
            // node must be refused entirely (never default to 0.0, which
            // would silently under-pad and drop rows). Other spatial
            // predicates (st_intersects, st_contains, ...) are exact on the
            // bbox-intersection candidate set, so they need no padding.
            let pad = if sf.name() == "st_dwithin" {
                let d = sf.inputs.required(2).ok()?;
                let lit = d.as_literal()?;
                let val = lit.as_f64().or_else(|| lit.as_i64().map(|v| v as f64))?;
                if !val.is_finite() || val < 0.0 {
                    return None;
                }
                val
            } else {
                0.0
            };

            let kind = spatial_fn_kind(sf.name(), pad)?;

            let probe_n = output_schema_len - build_n;
            let (build_col, probe_col, build_is_arg0) = match build_side {
                JoinSide::Left => {
                    // output = [build(0..build_n) | probe(build_n..)]
                    if idx0 < build_n && idx1 >= build_n {
                        // arg0 = build geom, arg1 = probe geom
                        (idx0, idx1 - build_n, true)
                    } else if idx0 >= build_n && idx1 < build_n {
                        // arg0 = probe geom, arg1 = build geom
                        (idx1, idx0 - build_n, false)
                    } else {
                        return None;
                    }
                }
                JoinSide::Right => {
                    // output = [probe(0..probe_n) | build(probe_n..)]
                    if idx0 >= probe_n && idx1 < probe_n {
                        // arg0 = build geom, arg1 = probe geom
                        (idx0 - probe_n, idx1, true)
                    } else if idx0 < probe_n && idx1 >= probe_n {
                        // arg0 = probe geom, arg1 = build geom
                        (idx1 - probe_n, idx0, false)
                    } else {
                        return None;
                    }
                }
            };

            Some(AccelSpatial {
                build_col,
                probe_col,
                pad,
                kind,
                build_is_arg0,
                remainder: None,
            })
        }
        // Only an AND-conjunction preserves superset-soundness: every true pair
        // must satisfy the spatial conjunct, hence bbox-intersect. Under OR the
        // other branch can be true for pairs whose bboxes do NOT intersect, and
        // under NOT the truth of the wrapped predicate is inverted — in both
        // cases bbox candidate generation would silently drop true rows.
        //
        // The non-selected branch joins the remainder: it must still be
        // evaluated per candidate pair after direct verification. AND order is
        // preserved when recombining (left-to-right), and a `true AND rest`
        // filter is equivalent to `rest` — while any null/false spatial result
        // drops the pair in both formulations, so splitting never changes the
        // surviving set.
        Expr::BinaryOp { op: Operator::And, left, right } => {
            if let Some(acc) = extract_from_expr(left, build_side, output_schema_len, build_n) {
                let remainder = match acc.remainder {
                    Some(lrem) => Some(lrem.and(right.clone())),
                    None => Some(right.clone()),
                };
                Some(AccelSpatial { remainder, ..acc })
            } else if let Some(acc) =
                extract_from_expr(right, build_side, output_schema_len, build_n)
            {
                let remainder = match acc.remainder {
                    Some(rrem) => Some(left.clone().and(rrem)),
                    None => Some(left.clone()),
                };
                Some(AccelSpatial { remainder, ..acc })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_geom_col_indices(
    filter: &BoundExpr,
    build_side: JoinSide,
    output_schema_len: usize,
    build_n: usize,
) -> Option<AccelSpatial> {
    extract_from_expr(filter.inner(), build_side, output_schema_len, build_n)
}

// ── Index-direct candidate verification ──────────────────────────────────

/// Evaluate the accelerated spatial predicate directly on parsed geometries
/// for R-tree candidate `pairs` (probe_idx, global_build_idx), WITHOUT
/// materializing a candidate tile batch. Returns the surviving pairs.
///
/// `pairs` must be sorted by global build index: candidates are processed as
/// runs sharing a build row, so each build geometry (typically a large
/// polygon WKB) is parsed ONCE per morsel-run instead of once per pair.
/// Probe geometries are parsed once per referenced probe row. Runs are
/// verified in parallel on `PROBE_POOL`.
///
/// Null/unparseable geometries never match — the same outcome the expression
/// kernel's null mask produces for those pairs.
fn direct_verify(
    index: &RTreeIndex,
    build_geom_col: usize,
    probe_binary: &daft_core::prelude::BinaryArray,
    pairs: &[(u64, u64)],
    kind: SpatialPredKind,
    build_is_arg0: bool,
) -> Vec<(u64, u64)> {
    use daft_geo::geo::Geometry;

    if pairs.is_empty() {
        return vec![];
    }

    // Parse each referenced probe geometry once.
    let mut needed = vec![false; probe_binary.len()];
    for &(pi, _) in pairs {
        needed[pi as usize] = true;
    }
    let probe_geoms: Vec<Option<Geometry>> = PROBE_POOL.install(|| {
        (0..probe_binary.len())
            .into_par_iter()
            .map(|i| {
                if needed[i] {
                    probe_binary.get(i).and_then(|wkb| daft_geo::parse_wkb(wkb).ok())
                } else {
                    None
                }
            })
            .collect()
    });

    // Split the sorted pairs into runs sharing a global build row.
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut start = 0;
    for i in 1..=pairs.len() {
        if i == pairs.len() || pairs[i].1 != pairs[start].1 {
            runs.push((start, i));
            start = i;
        }
    }

    let eval = |a: &Geometry, b: &Geometry| -> bool {
        match kind {
            SpatialPredKind::Relate(pred) => daft_geo::relate_pred(a, b, pred),
            // Mirrors StDwithin::call exactly: finite distance <= d.
            SpatialPredKind::DWithin(d) => {
                let dist = daft_geo::geom_distance(a, b);
                dist.is_finite() && dist <= d
            }
        }
    };

    // Verify runs in parallel; each run parses its build geometry once.
    let survivors: Vec<Vec<(u64, u64)>> = PROBE_POOL.install(|| {
        runs.into_par_iter()
            .map(|(run_start, run_end)| {
                let global = pairs[run_start].1;
                let (t, local) = index.resolve(global);
                let Ok(build_binary) =
                    get_geometry_binary(index.tables[t].get_column(build_geom_col))
                else {
                    return vec![];
                };
                let Some(build_geom) = build_binary
                    .get(local as usize)
                    .and_then(|wkb| daft_geo::parse_wkb(wkb).ok())
                else {
                    return vec![];
                };
                pairs[run_start..run_end]
                    .iter()
                    .filter(|(pi, _)| {
                        let Some(probe_geom) = &probe_geoms[*pi as usize] else {
                            return false;
                        };
                        if build_is_arg0 {
                            eval(&build_geom, probe_geom)
                        } else {
                            eval(probe_geom, &build_geom)
                        }
                    })
                    .copied()
                    .collect()
            })
            .collect()
    });

    survivors.into_iter().flatten().collect()
}

/// Build per-partition R-trees without a global concat.
///
/// Phase 1: scan all source tables once to map key → [(table_idx, row_idx)].
///          No data is copied in this phase.
/// Phase 2: for each key, `RecordBatch::take` rows from their source tables
///          and concat only that group's (typically small) slices.
///          Build one R-tree per group indexing into its own RecordBatch.
///          This phase is parallelised with rayon across groups.
///
/// Bbox fast-path: if the build tables contain precomputed `min_x/min_y/max_x/max_y`
/// Float64 columns, their values are used directly instead of parsing WKB bytes.
/// This eliminates the dominant cost (wkb_to_mbr) when such columns exist.
///
/// Memory peak ≈ original_scattered_tables + max_one_group (during phase 2).
/// After this function returns the original `tables` are freed by the caller.
/// Steady-state = sum(per_group_RecordBatch) + R-tree entries (~32 B each).
fn build_partitioned_rtrees(
    tables: &[RecordBatch],
    build_key_col: usize,
    build_geom_col: usize,
    probe_key_col: usize,
    probe_geom_col: usize,
) -> Option<PartitionedRTreeState> {
    if tables.is_empty() {
        return None;
    }

    // Detect precomputed bbox columns in the first table's schema.
    // If present, use them directly instead of wkb_to_mbr (10-100× faster).
    // Dtype is validated at detection: non-Float64 name collisions fall back
    // to the WKB path instead of silently dropping the group below.
    let bbox_cols = detect_bbox_cols(&tables[0].schema);

    // Phase 1: group (table_idx, row_idx) pairs by key — no data copy.
    let mut key_to_locs: HashMap<String, Vec<(usize, u32)>> = HashMap::new();
    for (t_idx, table) in tables.iter().enumerate() {
        let key_series = table.get_column(build_key_col);
        for r_idx in 0..table.len() {
            let key = key_series.str_value(r_idx);
            key_to_locs.entry(key).or_default().push((t_idx, r_idx as u32));
        }
    }

    if key_to_locs.is_empty() {
        return None;
    }

    // Phase 2: per group — extract rows, concat, build R-tree.
    // Parallelised with rayon: each group is independent.
    let groups: HashMap<String, (RecordBatch, RTree<RTreeEntry>)> = key_to_locs
        .into_par_iter()
        .filter_map(|(key, locs)| {
            // Gather per-source-table index lists.
            let mut per_table: HashMap<usize, Vec<u64>> = HashMap::new();
            for (t_idx, r_idx) in &locs {
                per_table.entry(*t_idx).or_default().push(*r_idx as u64);
            }

            // Take rows from each source table and collect the slices.
            let mut pieces: Vec<RecordBatch> = Vec::with_capacity(per_table.len());
            for (t_idx, row_indices) in per_table {
                let idx_arr = UInt64Array::from_vec("", row_indices);
                if let Ok(taken) = tables[t_idx].take(&idx_arr) {
                    if !taken.is_empty() {
                        pieces.push(taken);
                    }
                }
            }

            if pieces.is_empty() {
                return None;
            }

            let group_rb = if pieces.len() == 1 {
                pieces.remove(0)
            } else {
                RecordBatch::concat(&pieces).ok()?
            };

            if group_rb.is_empty() {
                return None;
            }

            // Build R-tree entries — bbox fast-path when precomputed columns exist.
            let entries: Vec<RTreeEntry> = if let Some([ix, iy, ax, ay]) = bbox_cols {
                // Use .get(i) (returns Option<f64>, None for nulls) so that null bbox
                // fields are skipped rather than read as 0.0 (which `.values()[i]` would do).
                let min_x_s = group_rb.get_column(ix).f64().ok()?;
                let min_y_s = group_rb.get_column(iy).f64().ok()?;
                let max_x_s = group_rb.get_column(ax).f64().ok()?;
                let max_y_s = group_rb.get_column(ay).f64().ok()?;
                (0..group_rb.len())
                    .filter_map(|i| {
                        let mn_x = min_x_s.get(i)?;
                        let mn_y = min_y_s.get(i)?;
                        let mx_x = max_x_s.get(i)?;
                        let mx_y = max_y_s.get(i)?;
                        if mn_x.is_finite() && mn_y.is_finite()
                            && mx_x.is_finite() && mx_y.is_finite()
                        {
                            Some(RTreeEntry {
                                bbox: AABB::from_corners([mn_x, mn_y], [mx_x, mx_y]),
                                row_idx: i as u32,
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                // Slow path: parse WKB bytes to extract MBR.
                let geom_series = group_rb.get_column(build_geom_col);
                let binary = get_geometry_binary(geom_series).ok()?;
                (0..group_rb.len())
                    .filter_map(|i| {
                        let wkb = binary.get(i)?;
                        let [mn_x, mn_y, mx_x, mx_y] = wkb_to_mbr(wkb)?;
                        Some(RTreeEntry {
                            bbox: AABB::from_corners([mn_x, mn_y], [mx_x, mx_y]),
                            row_idx: i as u32,
                        })
                    })
                    .collect()
            };

            Some((key, (group_rb, RTree::bulk_load(entries))))
        })
        .collect();

    if groups.is_empty() {
        return None;
    }

    Some(PartitionedRTreeState { groups, probe_key_col, probe_geom_col })
}

// ── Operator state ────────────────────────────────────────────────────────

pub(crate) struct NestedLoopBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) struct NestedLoopProbeState {
    build_tables: Vec<RecordBatch>,
    rtree_state: Option<RTreeState>,
    partitioned_rtree_state: Option<PartitionedRTreeState>,
    stream_idx: usize,
}

// ── Operator ──────────────────────────────────────────────────────────────

pub struct NestedLoopJoinOperator {
    filter: BoundExpr,
    output_schema: SchemaRef,
    build_side: JoinSide,
    /// `Some((build_geom_col, probe_geom_col))` when R-tree is applicable.
    geom_cols: Option<(usize, usize)>,
    /// `Some((build_key_col, probe_key_col))` when equality partition key is present.
    partition_key: Option<(usize, usize)>,
    /// For `st_dwithin` predicates: the distance `d` by which to pad the probe
    /// query AABB on all sides before querying the R-tree.  `None` (or `0.0`)
    /// for topological predicates whose query box is the exact probe MBR.
    dwithin_distance: Option<f64>,
    /// Direct per-pair evaluation of the accelerated node:
    /// (kind, build_is_arg0). Same node as `geom_cols` — single walk.
    direct_pred: Option<(SpatialPredKind, bool)>,
    /// The filter minus the accelerated node (still bound to the output
    /// schema): evaluated on the direct-verified survivors. `None` when the
    /// filter IS the spatial node — no expression evaluation at all then.
    remainder: Option<BoundExpr>,
}

impl NestedLoopJoinOperator {
    /// `build_n_cols` = column count of the build-side physical plan schema.
    pub fn new(
        filter: BoundExpr,
        output_schema: SchemaRef,
        build_side: JoinSide,
        build_n_cols: usize,
        partition_key: Option<(usize, usize)>,
    ) -> Self {
        // Single walk: column selection, pad, evaluation kind, argument
        // order, and remainder all come from the same accelerated node, so
        // they can never disagree (see module-level doc on
        // `extract_from_expr`).
        let extracted = extract_geom_col_indices(
            &filter,
            build_side,
            output_schema.len(),
            build_n_cols,
        );
        let (geom_cols, dwithin_distance, direct_pred, remainder) = match extracted {
            Some(acc) => (
                Some((acc.build_col, acc.probe_col)),
                Some(acc.pad),
                Some((acc.kind, acc.build_is_arg0)),
                // Components are already Column::Bound against the output
                // schema (the filter itself is a BoundExpr over it).
                acc.remainder.map(BoundExpr::new_unchecked),
            ),
            None => (None, None, None, None),
        };
        Self {
            filter,
            output_schema,
            build_side,
            geom_cols,
            partition_key,
            dwithin_distance,
            direct_pred,
            remainder,
        }
    }
}

impl JoinOperator for NestedLoopJoinOperator {
    type BuildState = NestedLoopBuildState;
    type FinalizedBuildState = Vec<RecordBatch>;
    type ProbeState = NestedLoopProbeState;

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(NestedLoopBuildState { tables: Vec::new() })
    }

    fn build(
        &self,
        input: MicroPartition,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        if !input.is_empty() {
            state.tables.extend(input.record_batches().iter().cloned());
        }
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(state.tables).into()
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        // Prefer the partitioned R-tree path when we have both a geometry column
        // and an equality partition key.  This keeps memory proportional to the
        // build side (no extra 2× concat copy over the whole table at once) and
        // makes each probe morsel query only its matching key group's R-tree.
        if let (Some((build_geom_col, probe_geom_col)), Some((build_key_col, probe_key_col))) =
            (self.geom_cols, self.partition_key)
        {
            if let Some(ps) = build_partitioned_rtrees(
                &finalized_build_state,
                build_key_col,
                build_geom_col,
                probe_key_col,
                probe_geom_col,
            ) {
                return NestedLoopProbeState {
                    build_tables: vec![],
                    rtree_state: None,
                    partitioned_rtree_state: Some(ps),
                    stream_idx: 0,
                };
            }
        }
        // Fall back to single global R-tree when no partition key. The index
        // owns the original scattered batches — no concatenated copy is made.
        if let Some((build_col, probe_col)) = self.geom_cols {
            let total: usize = finalized_build_state.iter().map(|t| t.len()).sum();
            // Preconditions mirror RTreeIndex::build's `None` cases exactly:
            // build() consumes the tables, so it must only be called when it
            // is guaranteed to succeed (falling through with the tables lost
            // would silently produce an empty join).
            if total > 0 && total <= u32::MAX as usize {
                let index = RTreeIndex::build(finalized_build_state, build_col)
                    .expect("RTreeIndex::build preconditions were checked");
                return NestedLoopProbeState {
                    build_tables: vec![],
                    rtree_state: Some(RTreeState {
                        index,
                        build_geom_col: build_col,
                        probe_geom_col: probe_col,
                    }),
                    partitioned_rtree_state: None,
                    stream_idx: 0,
                };
            }
        }
        NestedLoopProbeState {
            build_tables: finalized_build_state,
            rtree_state: None,
            partitioned_rtree_state: None,
            stream_idx: 0,
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        let build_is_empty = if state.partitioned_rtree_state.is_some() {
            false // partitioned state is never considered empty; unmatched keys just produce no output
        } else {
            // An RTreeState is only constructed over a non-empty build side.
            state
                .rtree_state
                .as_ref()
                .map_or(state.build_tables.is_empty(), |_| false)
        };

        if input.is_empty() || build_is_empty {
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        if state.stream_idx >= input.record_batches().len() {
            state.stream_idx = 0;
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let output_schema = self.output_schema.clone();
        let filter = self.filter.clone();
        let build_side = self.build_side;
        let pad = self.dwithin_distance.unwrap_or(0.0);
        let direct_pred = self.direct_pred;
        let remainder = self.remainder.clone();

        spawner
            .spawn(
                async move {
                    let probe_tables = input.record_batches();
                    let probe_tbl = &probe_tables[state.stream_idx];

                    let output_mp = if let Some(ref ps) = state.partitioned_rtree_state {
                        // ── Partition-key R-tree path ─────────────────────────────
                        // Each group has its own RecordBatch; probe rows are bucketed
                        // by key and joined against only their matching group.
                        let key_series = probe_tbl.get_column(ps.probe_key_col);
                        let probe_geom_series = probe_tbl.get_column(ps.probe_geom_col);

                        // Collect candidate (probe_idx, build_idx) pairs per key.
                        // build_idx is relative to that key's group RecordBatch.
                        // Each probe row is independent, so we probe the R-tree in
                        // parallel with rayon and merge the per-row results serially.
                        let per_key: HashMap<String, (Vec<u64>, Vec<u64>)> =
                            if let Ok(binary) = get_geometry_binary(probe_geom_series) {
                                let per_row: Vec<Vec<(String, u64, u64)>> =
                                    PROBE_POOL.install(|| (0..probe_tbl.len())
                                        .into_par_iter()
                                        .map(|i| -> Vec<(String, u64, u64)> {
                                            let key = key_series.str_value(i);
                                            let Some((_, group_tree)) =
                                                ps.groups.get(&key)
                                            else {
                                                return vec![];
                                            };
                                            let Some(wkb) = binary.get(i) else {
                                                return vec![];
                                            };
                                            let Some([min_x, min_y, max_x, max_y]) =
                                                wkb_to_mbr(wkb)
                                            else {
                                                return vec![];
                                            };
                                            let q = AABB::from_corners(
                                                [min_x - pad, min_y - pad],
                                                [max_x + pad, max_y + pad],
                                            );
                                            group_tree
                                                .locate_in_envelope_intersecting(q)
                                                .map(|entry| {
                                                    (key.clone(), i as u64, entry.row_idx as u64)
                                                })
                                                .collect()
                                        })
                                        .collect());
                                let mut map: HashMap<String, (Vec<u64>, Vec<u64>)> =
                                    HashMap::new();
                                for row_cands in per_row {
                                    for (key, pi, bi) in row_cands {
                                        let e = map.entry(key).or_default();
                                        e.0.push(pi);
                                        e.1.push(bi);
                                    }
                                }
                                map
                            } else {
                                HashMap::new()
                            };

                        // One indexed join call per unique key in this probe morsel.
                        let mut result_batches: Vec<RecordBatch> = Vec::new();
                        for (key, (cp, cb)) in per_key {
                            if cp.is_empty() { continue; }
                            let (group_rb, _) = ps.groups.get(&key).unwrap();
                            let rb = nested_loop_inner_join_indexed(
                                probe_tbl, group_rb, &filter, build_side, &cp, &cb,
                            )?;
                            if !rb.is_empty() {
                                result_batches.push(rb);
                            }
                        }

                        if result_batches.is_empty() {
                            MicroPartition::empty(Some(output_schema))
                        } else {
                            MicroPartition::new_loaded(
                                output_schema,
                                Arc::new(result_batches),
                                None,
                            )
                        }
                    } else if let Some(ref rs) = state.rtree_state {
                        // ── R-tree accelerated path (concat-free) ─────────────────
                        let index = &rs.index;

                        let probe_series = probe_tbl.get_column(rs.probe_geom_col);
                        if let Ok(binary) = get_geometry_binary(probe_series) {
                            // Probe each row in parallel: RTree is Sync so concurrent
                            // locate_in_envelope_intersecting calls are safe.  Each rayon
                            // task returns its (probe_idx, global_build_idx) pairs.
                            let per_row: Vec<Vec<(u64, u64)>> = PROBE_POOL.install(|| (0..probe_tbl.len())
                                .into_par_iter()
                                .map(|i| -> Vec<(u64, u64)> {
                                    let Some(wkb) = binary.get(i) else {
                                        return vec![];
                                    };
                                    let Some([min_x, min_y, max_x, max_y]) = wkb_to_mbr(wkb)
                                    else {
                                        return vec![];
                                    };
                                    let q = [
                                        min_x - pad,
                                        min_y - pad,
                                        max_x + pad,
                                        max_y + pad,
                                    ];
                                    let mut row_pairs = Vec::new();
                                    index.tree.for_each_intersecting(&q, |row| {
                                        row_pairs.push((i as u64, row as u64));
                                    });
                                    row_pairs
                                })
                                .collect());
                            let total: usize = per_row.iter().map(|v| v.len()).sum();
                            let mut pairs: Vec<(u64, u64)> = Vec::with_capacity(total);
                            for row_pairs in per_row {
                                pairs.extend(row_pairs);
                            }
                            // Sort by global build index: direct_verify parses
                            // each build geometry once per run, and gather()
                            // groups per-table runs with one take per table.
                            pairs.sort_unstable_by_key(|&(_, bi)| bi);

                            // Direct verification: evaluate the accelerated
                            // spatial node on parsed geometries, killing false
                            // candidates BEFORE any row gathering. What's left
                            // to expression-evaluate is only the remainder —
                            // nothing at all for a pure spatial filter.
                            let mask_pred: Option<&BoundExpr> =
                                if let Some((kind, build_is_arg0)) = direct_pred {
                                    pairs = direct_verify(
                                        index,
                                        rs.build_geom_col,
                                        binary,
                                        &pairs,
                                        kind,
                                        build_is_arg0,
                                    );
                                    remainder.as_ref()
                                } else {
                                    // No direct evaluation available: the full
                                    // filter runs on the gathered candidates.
                                    Some(&filter)
                                };

                            if pairs.is_empty() {
                                MicroPartition::empty(Some(output_schema))
                            } else {
                                // Gather each side ONCE in pair order, assemble the
                                // output-schema batch, then mask with what's left —
                                // no candidate tile + second gather of passing rows.
                                let build_globals: Vec<u64> =
                                    pairs.iter().map(|&(_, bi)| bi).collect();
                                let probe_idxs: Vec<u64> =
                                    pairs.iter().map(|&(pi, _)| pi).collect();
                                let build_rows = index.gather(&build_globals)?;
                                let probe_rows =
                                    probe_tbl.take(&UInt64Array::from_vec("", probe_idxs))?;

                                let (left_tbl, right_tbl) = match build_side {
                                    JoinSide::Left => (build_rows, probe_rows),
                                    JoinSide::Right => (probe_rows, build_rows),
                                };
                                let mut columns: Vec<daft_core::series::Series> = (0
                                    ..left_tbl.num_columns())
                                    .map(|i| left_tbl.get_column(i).clone())
                                    .collect();
                                columns.extend(
                                    (0..right_tbl.num_columns())
                                        .map(|i| right_tbl.get_column(i).clone()),
                                );
                                let n_pairs = pairs.len();
                                let cand_batch = RecordBatch::new_with_size(
                                    output_schema.clone(),
                                    columns,
                                    n_pairs,
                                )?;

                                let out = match mask_pred {
                                    Some(pred) => {
                                        let mask = cand_batch.eval_expression(pred)?;
                                        cand_batch.mask_filter(&mask)?
                                    }
                                    None => cand_batch,
                                };
                                if out.is_empty() {
                                    MicroPartition::empty(Some(output_schema))
                                } else {
                                    MicroPartition::new_loaded(
                                        output_schema,
                                        Arc::new(vec![out]),
                                        None,
                                    )
                                }
                            }
                        } else {
                            // Probe column wasn't Binary — fall back to the naive
                            // per-table loop over the index's scattered batches.
                            let mut result_batches = Vec::new();
                            for build_tbl in &index.tables {
                                let out = nested_loop_inner_join(
                                    probe_tbl,
                                    build_tbl,
                                    &filter,
                                    build_side,
                                )?;
                                if !out.is_empty() {
                                    result_batches.push(out);
                                }
                            }
                            if result_batches.is_empty() {
                                MicroPartition::empty(Some(output_schema))
                            } else {
                                MicroPartition::new_loaded(
                                    output_schema,
                                    Arc::new(result_batches),
                                    None,
                                )
                            }
                        }
                    } else {
                        // ── Fallback: original per-table loop ─────────────────────
                        let mut result_batches = Vec::new();
                        for build_tbl in &state.build_tables {
                            let out = nested_loop_inner_join(
                                probe_tbl,
                                build_tbl,
                                &filter,
                                build_side,
                            )?;
                            if !out.is_empty() {
                                result_batches.push(out);
                            }
                        }
                        if result_batches.is_empty() {
                            MicroPartition::empty(Some(output_schema))
                        } else {
                            MicroPartition::new_loaded(
                                output_schema,
                                Arc::new(result_batches),
                                None,
                            )
                        }
                    };

                    state.stream_idx += 1;
                    let result = if state.stream_idx >= probe_tables.len() {
                        state.stream_idx = 0;
                        ProbeOutput::NeedMoreInput(Some(output_mp))
                    } else {
                        ProbeOutput::HasMoreOutput {
                            input,
                            output: output_mp,
                        }
                    };
                    Ok((state, result))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        _states: Vec<Self::ProbeState>,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        Ok(None).into()
    }

    fn needs_probe_finalization(&self) -> bool {
        false
    }

    fn name(&self) -> NodeName {
        "Nested Loop Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::NestedLoopJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        let accel = if self.geom_cols.is_some() { " [R-tree]" } else { "" };
        vec![
            format!("Nested Loop Join{accel}"),
            format!("Filter = {}", self.filter.inner()),
            format!("Build Side = {:?}", self.build_side),
        ]
    }
}

#[cfg(test)]
mod rtree_index_tests {
    use daft_core::prelude::*;
    use daft_geo::geo::{Geometry, Point};
    use daft_geo::utils::geom_to_wkb;

    use super::*;

    /// Batch of (id, geom) where geom is WKB-encoded points; `None` = null geom.
    fn batch(ids: &[i64], pts: &[Option<(f64, f64)>]) -> RecordBatch {
        assert_eq!(ids.len(), pts.len());
        let id = Int64Array::from_vec("id", ids.to_vec()).into_series();
        let wkb: Vec<Option<Vec<u8>>> = pts
            .iter()
            .map(|o| {
                o.map(|(x, y)| geom_to_wkb(&Geometry::Point(Point::new(x, y))).unwrap())
            })
            .collect();
        let geom = BinaryArray::from_iter("geom", wkb.into_iter()).into_series();
        RecordBatch::from_nonempty_columns(vec![id, geom]).unwrap()
    }

    fn query(index: &RTreeIndex, min: [f64; 2], max: [f64; 2]) -> Vec<u64> {
        let q = [min[0], min[1], max[0], max[1]];
        let mut out: Vec<u64> = index.tree.search(&q).into_iter().map(u64::from).collect();
        out.sort_unstable();
        out
    }

    /// Entries must carry GLOBAL row indices spanning all build tables — no
    /// concatenated copy of the build side exists anywhere.
    #[test]
    fn rtree_index_spans_tables_with_global_indices() {
        let t0 = batch(&[10, 11, 12], &[Some((0.0, 0.0)), Some((1.0, 1.0)), Some((2.0, 2.0))]);
        let t1 = batch(&[13, 14], &[Some((10.0, 10.0)), Some((11.0, 11.0))]);
        let index = RTreeIndex::build(vec![t0, t1], 1).unwrap();

        assert_eq!(index.total_rows(), 5);
        assert_eq!(query(&index, [9.5, 9.5], [10.5, 10.5]), vec![3]);
        assert_eq!(query(&index, [0.5, 0.5], [11.5, 11.5]), vec![1, 2, 3, 4]);

        assert_eq!(index.resolve(0), (0, 0));
        assert_eq!(index.resolve(2), (0, 2));
        assert_eq!(index.resolve(3), (1, 0));
        assert_eq!(index.resolve(4), (1, 1));
    }

    /// Null geometries produce no R-tree entry but still occupy a global row
    /// index — entries after them must not shift.
    #[test]
    fn rtree_index_null_geoms_keep_global_indices_stable() {
        let t0 = batch(&[10, 11, 12], &[Some((0.0, 0.0)), None, Some((2.0, 2.0))]);
        let t1 = batch(&[13], &[Some((10.0, 10.0))]);
        let index = RTreeIndex::build(vec![t0, t1], 1).unwrap();

        assert_eq!(index.total_rows(), 4);
        assert_eq!(query(&index, [-0.5, -0.5], [10.5, 10.5]), vec![0, 2, 3]);
        assert_eq!(index.resolve(3), (1, 0));
    }

    /// `gather` must return rows in the given order, crossing table
    /// boundaries, so probe/build sides stay pair-aligned.
    #[test]
    fn rtree_index_gather_crosses_table_boundaries() {
        let t0 = batch(&[10, 11, 12], &[Some((0.0, 0.0)), Some((1.0, 1.0)), Some((2.0, 2.0))]);
        let t1 = batch(&[13, 14], &[Some((10.0, 10.0)), Some((11.0, 11.0))]);
        let index = RTreeIndex::build(vec![t0, t1], 1).unwrap();

        let gathered = index.gather(&[0, 2, 3, 4]).unwrap();
        assert_eq!(gathered.len(), 4);
        let ids = gathered.get_column(0).i64().unwrap();
        let got: Vec<i64> = (0..4).map(|i| ids.get(i).unwrap()).collect();
        assert_eq!(got, vec![10, 12, 13, 14]);

        // Duplicate globals (one build row matching many probe rows) must be
        // duplicated in the output, preserving order.
        let gathered = index.gather(&[3, 3, 4]).unwrap();
        let ids = gathered.get_column(0).i64().unwrap();
        let got: Vec<i64> = (0..3).map(|i| ids.get(i).unwrap()).collect();
        assert_eq!(got, vec![13, 13, 14]);
    }

    /// Precomputed bbox columns must be trusted over the WKB when present —
    /// same contract the Python `wrong bbox columns` test pins e2e.
    #[test]
    fn rtree_index_uses_precomputed_bbox_columns() {
        let t = batch(&[10], &[Some((0.0, 0.0))]);
        // Append rtree_* columns whose box is deliberately NOT at the geom.
        let cols = vec![
            t.get_column(0).clone(),
            t.get_column(1).clone(),
            Float64Array::from_vec("rtree_min_x", vec![100.0]).into_series(),
            Float64Array::from_vec("rtree_min_y", vec![100.0]).into_series(),
            Float64Array::from_vec("rtree_max_x", vec![101.0]).into_series(),
            Float64Array::from_vec("rtree_max_y", vec![101.0]).into_series(),
        ];
        let t = RecordBatch::from_nonempty_columns(cols).unwrap();
        let index = RTreeIndex::build(vec![t], 1).unwrap();

        assert_eq!(query(&index, [99.5, 99.5], [101.5, 101.5]), vec![0]);
        assert!(query(&index, [-0.5, -0.5], [0.5, 0.5]).is_empty());
    }

    /// No rows anywhere → no index (caller falls back to the naive path).
    #[test]
    fn rtree_index_empty_build_returns_none() {
        assert!(RTreeIndex::build(vec![], 1).is_none());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("geom", DataType::Binary),
        ]);
        let empty = RecordBatch::empty(Some(Arc::new(schema)));
        assert!(RTreeIndex::build(vec![empty], 1).is_none());
    }
}

#[cfg(test)]
mod direct_verify_tests {
    use daft_core::prelude::*;
    use daft_geo::RelatePred;
    use daft_geo::geo::{Coord, Geometry, LineString, Point, Polygon};
    use daft_geo::utils::geom_to_wkb;

    use super::*;

    fn point_wkb(x: f64, y: f64) -> Vec<u8> {
        geom_to_wkb(&Geometry::Point(Point::new(x, y))).unwrap()
    }

    fn square_wkb(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<u8> {
        let ring = LineString(vec![
            Coord { x: x0, y: y0 },
            Coord { x: x1, y: y0 },
            Coord { x: x1, y: y1 },
            Coord { x: x0, y: y1 },
            Coord { x: x0, y: y0 },
        ]);
        geom_to_wkb(&Geometry::Polygon(Polygon::new(ring, vec![]))).unwrap()
    }

    fn geom_batch(ids: &[i64], wkbs: &[Option<Vec<u8>>]) -> RecordBatch {
        let id = Int64Array::from_vec("id", ids.to_vec()).into_series();
        let geom =
            BinaryArray::from_iter("geom", wkbs.iter().map(|o| o.as_deref())).into_series();
        RecordBatch::from_nonempty_columns(vec![id, geom]).unwrap()
    }

    fn probe_binary(batch: &RecordBatch) -> &BinaryArray {
        get_geometry_binary(batch.get_column(1)).unwrap()
    }

    /// st_contains is interior-only: boundary and outside points must not
    /// survive; the inside point must.
    #[test]
    fn direct_verify_contains_excludes_boundary() {
        let build = RTreeIndex::build(
            vec![geom_batch(&[100], &[Some(square_wkb(0.0, 0.0, 2.0, 2.0))])],
            1,
        )
        .unwrap();
        let probe = geom_batch(
            &[0, 1, 2],
            &[
                Some(point_wkb(1.0, 1.0)), // inside
                Some(point_wkb(0.0, 1.0)), // boundary
                Some(point_wkb(5.0, 5.0)), // outside
            ],
        );
        let pairs = vec![(0, 0), (1, 0), (2, 0)];
        let survivors = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::Relate(RelatePred::Contains),
            true,
        );
        assert_eq!(survivors, vec![(0, 0)]);
    }

    /// Argument order must be preserved: `contains(probe_pt, build_poly)`
    /// (build_is_arg0 = false) asks whether a POINT contains a POLYGON —
    /// always false — while `within(probe_pt, build_poly)` is true inside.
    #[test]
    fn direct_verify_respects_arg_order() {
        let build = RTreeIndex::build(
            vec![geom_batch(&[100], &[Some(square_wkb(0.0, 0.0, 2.0, 2.0))])],
            1,
        )
        .unwrap();
        let probe = geom_batch(&[0], &[Some(point_wkb(1.0, 1.0))]);
        let pairs = vec![(0, 0)];

        let contains_flipped = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::Relate(RelatePred::Contains),
            false, // probe point is arg0: point.contains(polygon)
        );
        assert!(contains_flipped.is_empty());

        let within = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::Relate(RelatePred::Within),
            false, // probe point is arg0: point.within(polygon)
        );
        assert_eq!(within, vec![(0, 0)]);
    }

    /// st_dwithin semantics must mirror the expression kernel exactly:
    /// finite distance <= d, boundary inclusive.
    #[test]
    fn direct_verify_dwithin_boundary_inclusive() {
        let build =
            RTreeIndex::build(vec![geom_batch(&[100], &[Some(point_wkb(0.0, 0.0))])], 1)
                .unwrap();
        let probe = geom_batch(
            &[0, 1],
            &[
                Some(point_wkb(3.0, 4.0)), // distance exactly 5.0
                Some(point_wkb(6.0, 8.0)), // distance 10.0
            ],
        );
        let pairs = vec![(0, 0), (1, 0)];
        let survivors = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::DWithin(5.0),
            true,
        );
        assert_eq!(survivors, vec![(0, 0)]);
    }

    /// Null or unparseable geometries on either side never match — the same
    /// outcome the expression kernel's null mask produces.
    #[test]
    fn direct_verify_null_and_invalid_geoms_drop() {
        let build = RTreeIndex::build(
            vec![geom_batch(
                &[100, 101],
                &[Some(square_wkb(0.0, 0.0, 2.0, 2.0)), Some(b"garbage".to_vec())],
            )],
            1,
        )
        .unwrap();
        let probe = geom_batch(&[0, 1], &[Some(point_wkb(1.0, 1.0)), None]);
        // Pair every probe row with every build row.
        let pairs = vec![(0, 0), (1, 0), (0, 1), (1, 1)];
        let survivors = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::Relate(RelatePred::Contains),
            true,
        );
        assert_eq!(survivors, vec![(0, 0)]);
    }

    /// Runs spanning multiple build tables must resolve to the right rows.
    #[test]
    fn direct_verify_multi_table_runs() {
        let build = RTreeIndex::build(
            vec![
                geom_batch(
                    &[100, 101],
                    &[
                        Some(square_wkb(0.0, 0.0, 2.0, 2.0)),
                        Some(square_wkb(10.0, 10.0, 12.0, 12.0)),
                    ],
                ),
                geom_batch(&[102], &[Some(square_wkb(20.0, 20.0, 22.0, 22.0))]),
            ],
            1,
        )
        .unwrap();
        let probe = geom_batch(
            &[0, 1, 2],
            &[
                Some(point_wkb(1.0, 1.0)),   // in build global 0
                Some(point_wkb(11.0, 11.0)), // in build global 1
                Some(point_wkb(21.0, 21.0)), // in build global 2 (table 1)
            ],
        );
        // All probe rows against all build rows, sorted by build global.
        let pairs = vec![
            (0, 0), (1, 0), (2, 0),
            (0, 1), (1, 1), (2, 1),
            (0, 2), (1, 2), (2, 2),
        ];
        let survivors = direct_verify(
            &build,
            1,
            probe_binary(&probe),
            &pairs,
            SpatialPredKind::Relate(RelatePred::Contains),
            true,
        );
        assert_eq!(survivors, vec![(0, 0), (1, 1), (2, 2)]);
    }
}

#[cfg(test)]
mod acceleration_tests {
    use daft_core::prelude::{DataType, Field, Schema};
    use daft_dsl::{expr::bound_expr::BoundExpr, unresolved_col};

    use super::*;

    fn geom_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Geometry),
            Field::new("b", DataType::Geometry),
        ])
    }

    /// 4-geometry-column schema used by tests that need the accelerated node
    /// and a decoy node to straddle build/probe differently (see
    /// `mixed_or_and_dwithin_uses_the_accelerated_nodes_distance`).
    fn geom_schema4() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Geometry),
            Field::new("b", DataType::Geometry),
            Field::new("c", DataType::Geometry),
            Field::new("d", DataType::Geometry),
        ])
    }

    fn extract_full(expr: daft_dsl::ExprRef) -> Option<AccelSpatial> {
        let bound = BoundExpr::try_new(expr, &geom_schema()).unwrap();
        extract_geom_col_indices(&bound, JoinSide::Left, 2, 1)
    }

    fn extract(expr: daft_dsl::ExprRef) -> Option<(usize, usize, f64)> {
        extract_full(expr).map(|a| (a.build_col, a.probe_col, a.pad))
    }

    /// Bind an expression to the test schema and render it, for comparing
    /// remainder structure without relying on Expr equality.
    fn bound_display(expr: daft_dsl::ExprRef) -> String {
        format!(
            "{}",
            BoundExpr::try_new(expr, &geom_schema()).unwrap().inner()
        )
    }

    #[test]
    fn contains_extraction_reports_kind_and_arg_order() {
        let e = daft_geo::st_contains::st_contains(unresolved_col("a"), unresolved_col("b"));
        let acc = extract_full(e).expect("st_contains should be accelerated");
        assert_eq!(
            acc.kind,
            SpatialPredKind::Relate(daft_geo::RelatePred::Contains)
        );
        assert!(acc.build_is_arg0, "arg0 (a) is the build-side column");
        assert!(acc.remainder.is_none(), "bare spatial node has no remainder");

        // Flipped arguments: arg0 is the probe-side column.
        let e = daft_geo::st_contains::st_contains(unresolved_col("b"), unresolved_col("a"));
        let acc = extract_full(e).expect("flipped st_contains should be accelerated");
        assert!(!acc.build_is_arg0);
    }

    #[test]
    fn dwithin_extraction_carries_distance_kind() {
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(5.0),
        );
        let acc = extract_full(e).expect("st_dwithin should be accelerated");
        assert_eq!(acc.kind, SpatialPredKind::DWithin(5.0));
        assert_eq!(acc.pad, 5.0);
    }

    /// The remainder must be the full filter minus the accelerated node —
    /// exactly what still needs expression evaluation after direct
    /// verification.
    #[test]
    fn and_composed_extraction_returns_remainder() {
        let spatial =
            daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        let rest = unresolved_col("a").not_null();
        let acc = extract_full(spatial.and(rest)).expect("should be accelerated");
        let rem = acc.remainder.expect("non-spatial conjunct must remain");
        assert_eq!(
            format!("{rem}"),
            bound_display(unresolved_col("a").not_null())
        );
    }

    #[test]
    fn nested_and_remainder_combines_both_conjuncts() {
        let spatial =
            daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        let e = unresolved_col("a")
            .not_null()
            .and(spatial)
            .and(unresolved_col("b").not_null());
        let acc = extract_full(e).expect("should be accelerated");
        let rem = acc.remainder.expect("both non-spatial conjuncts must remain");
        assert_eq!(
            format!("{rem}"),
            bound_display(
                unresolved_col("a")
                    .not_null()
                    .and(unresolved_col("b").not_null())
            )
        );
    }

    #[test]
    fn st_intersects_is_accelerated() {
        let e = daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        assert!(extract(e).is_some());
    }

    #[test]
    fn st_disjoint_is_never_accelerated() {
        let e = daft_geo::st_disjoint::st_disjoint(unresolved_col("a"), unresolved_col("b"));
        assert!(extract(e).is_none());
    }

    #[test]
    fn negated_intersects_is_never_accelerated() {
        let e = daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"))
            .not();
        assert!(extract(e).is_none());
    }

    #[test]
    fn or_composed_intersects_is_never_accelerated() {
        let spatial =
            daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        let e = spatial.or(unresolved_col("a").is_null());
        assert!(extract(e).is_none());
    }

    #[test]
    fn and_composed_intersects_is_still_accelerated() {
        let spatial =
            daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        let e = spatial.and(unresolved_col("a").not_null());
        assert!(extract(e).is_some());
    }

    /// Regression test for the divergent-walk soundness bug: column selection
    /// and pad computation must both come from the SAME accelerated node.
    ///
    /// Schema: `[a, b, c, d]`, all Geometry, bound against a 4-column
    /// pseudo "output schema" with `build_n = 3` (columns 0..3 = a, b, c are
    /// build-local; column 3 = d is probe-local, at probe-local index 0).
    ///
    /// Filter: `(st_dwithin(a, b, 5) | <non-spatial>) & st_dwithin(c, d, 100)`.
    ///
    /// - The left `Or` branch is never descended into — only `And` recurses —
    ///   so `st_dwithin(a, b, 5)` is structurally unreachable and can never be
    ///   the accelerated node (it also happens to fail the build/probe
    ///   straddle check on its own, since a and b are both build-local here;
    ///   either reason alone is sufficient, so this test doesn't depend on
    ///   which one applies).
    /// - The right `st_dwithin(c, d, 100)` node has c build-local (idx 2 < 3)
    ///   and d probe-local (idx 3 >= 3), so it straddles and IS selected as
    ///   the accelerated node.
    ///
    /// Before the fix, column selection and pad computation were two
    /// independent walks that could pick different nodes: columns from the
    /// right node (2, 0) but pad from the left node (5.0). The fix must
    /// report pad 100.0 — the distance belonging to the very node whose
    /// columns were selected.
    #[test]
    fn mixed_or_and_dwithin_uses_the_accelerated_nodes_distance() {
        let spatial_left = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(5.0),
        );
        let non_spatial = unresolved_col("a").is_null();
        let left_or = spatial_left.or(non_spatial);
        let spatial_right = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("c"),
            unresolved_col("d"),
            daft_dsl::lit(100.0),
        );
        let filter_expr = left_or.and(spatial_right);

        let bound = BoundExpr::try_new(filter_expr, &geom_schema4()).unwrap();
        let result = extract_geom_col_indices(&bound, JoinSide::Left, 4, 3);

        let acc = result.expect("st_dwithin(c,d,100) should be accelerated");
        assert_eq!((acc.build_col, acc.probe_col), (2, 0));
        assert_eq!(
            acc.pad, 100.0,
            "pad must come from the accelerated node (c,d,100), not the unreachable (a,b,5) node"
        );
    }

    #[test]
    fn dwithin_with_non_literal_distance_is_not_accelerated() {
        // Distance argument is a column reference, not a resolvable literal.
        // The required pad is unknown, so acceleration must be refused
        // entirely rather than silently defaulting to a pad of 0.0 (which
        // would under-pad the R-tree query box and drop true matches).
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            unresolved_col("a"),
        );
        assert!(extract(e).is_none());
    }

    #[test]
    fn st_intersects_pads_zero() {
        let e = daft_geo::st_intersects::st_intersects(unresolved_col("a"), unresolved_col("b"));
        let (_, _, pad) = extract(e).expect("st_intersects should be accelerated");
        assert_eq!(pad, 0.0);
    }

    #[test]
    fn dwithin_with_negative_distance_is_not_accelerated() {
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(-1.0),
        );
        assert!(extract(e).is_none());
    }

    /// The guard is a strict `< 0.0`, so an exact-zero distance is the
    /// boundary value that must still be accelerated, with pad reported as
    /// 0.0. Pins against a future `<= 0.0` typo that would silently start
    /// refusing valid zero-distance `st_dwithin` queries.
    #[test]
    fn dwithin_zero_distance_is_accelerated_with_zero_pad() {
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(0.0),
        );
        let (_, _, pad) = extract(e).expect("st_dwithin(a, b, 0.0) should be accelerated");
        assert_eq!(pad, 0.0);
    }

    #[test]
    fn dwithin_nan_distance_is_not_accelerated() {
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(f64::NAN),
        );
        assert!(extract(e).is_none());
    }

    #[test]
    fn dwithin_infinite_distance_is_not_accelerated() {
        let e = daft_geo::st_dwithin::st_dwithin(
            unresolved_col("a"),
            unresolved_col("b"),
            daft_dsl::lit(f64::INFINITY),
        );
        assert!(extract(e).is_none());
    }
}
