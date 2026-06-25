use std::{collections::HashMap, sync::{Arc, LazyLock}};

use common_display::table_display::StrValue;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::{SchemaRef, UInt64Array}};
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
    let threads = (cpus / 2).max(2);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("daft-rtree-probe-{i}"))
        .build()
        .expect("failed to build R-tree probe thread pool")
});
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
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

struct RTreeState {
    merged_build: RecordBatch,
    rtree: RTree<RTreeEntry>,
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
    "st_covered_by", "st_disjoint", "st_touches", "st_overlaps",
    "st_crosses", "st_equals", "st_dwithin",
];

// ── Column-index extraction (no TreeNode dependency) ─────────────────────

/// Recursively walk `expr` looking for a spatial function call.
/// Returns `Some((build_local_col, probe_local_col))` on success.
fn extract_from_expr(
    expr: &ExprRef,
    build_side: JoinSide,
    output_schema_len: usize,
    build_n: usize,
) -> Option<(usize, usize)> {
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
            let probe_n = output_schema_len - build_n;
            match build_side {
                JoinSide::Left => {
                    // output = [build(0..build_n) | probe(build_n..)]
                    if idx0 < build_n && idx1 >= build_n {
                        // arg0 = build geom, arg1 = probe geom
                        Some((idx0, idx1 - build_n))
                    } else if idx0 >= build_n && idx1 < build_n {
                        // arg0 = probe geom, arg1 = build geom
                        Some((idx1, idx0 - build_n))
                    } else {
                        None
                    }
                }
                JoinSide::Right => {
                    // output = [probe(0..probe_n) | build(probe_n..)]
                    if idx0 >= probe_n && idx1 < probe_n {
                        // arg0 = build geom, arg1 = probe geom
                        Some((idx0 - probe_n, idx1))
                    } else if idx0 < probe_n && idx1 >= probe_n {
                        // arg0 = probe geom, arg1 = build geom
                        Some((idx1 - probe_n, idx0))
                    } else {
                        None
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_from_expr(left, build_side, output_schema_len, build_n)
                .or_else(|| extract_from_expr(right, build_side, output_schema_len, build_n))
        }
        Expr::Not(inner) => extract_from_expr(inner, build_side, output_schema_len, build_n),
        _ => None,
    }
}

fn extract_geom_col_indices(
    filter: &BoundExpr,
    build_side: JoinSide,
    output_schema_len: usize,
    build_n: usize,
) -> Option<(usize, usize)> {
    extract_from_expr(filter.inner(), build_side, output_schema_len, build_n)
}

/// Walk `expr` looking for an `st_dwithin` call and return its third argument
/// (distance) as `f64`. Handles `BinaryOp` / `Not` wrappers like `extract_from_expr`.
fn extract_dwithin_distance(expr: &ExprRef) -> Option<f64> {
    match expr.as_ref() {
        Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf))
            if sf.name() == "st_dwithin" =>
        {
            let d = sf.inputs.required(2).ok()?;
            d.as_literal().and_then(|l| {
                l.as_f64().or_else(|| l.as_i64().map(|v| v as f64))
            })
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_dwithin_distance(left).or_else(|| extract_dwithin_distance(right))
        }
        Expr::Not(inner) => extract_dwithin_distance(inner),
        _ => None,
    }
}

// ── R-tree construction helpers ───────────────────────────────────────────

/// Build a single R-tree covering all rows in `tables` (original path).
///
/// Bbox fast-path: if the merged build batch contains precomputed
/// `min_x/min_y/max_x/max_y` (or `bbox_min_x/…`) Float64 columns, their values
/// are used directly instead of parsing WKB via `wkb_to_mbr`.  This mirrors the
/// same detection done in `build_partitioned_rtrees`.
fn build_rtree(
    tables: &[RecordBatch],
    build_col: usize,
) -> Option<(RecordBatch, RTree<RTreeEntry>)> {
    if tables.is_empty() {
        return None;
    }
    let merged = RecordBatch::concat(tables).ok()?;
    if merged.is_empty() {
        return None;
    }

    // Detect precomputed bbox columns — same candidate sets as the partitioned path.
    let bbox_cols: Option<[usize; 4]> = {
        let schema = &merged.schema;
        let try_find = |name: &str| schema.get_index(name).ok();
        let candidates = [
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
    };

    let entries: Vec<RTreeEntry> = if let Some([ix, iy, ax, ay]) = bbox_cols {
        // Fast path: use precomputed bbox columns.
        // Use .get(i) (returns Option<f64>, None for nulls) so that null bbox
        // fields are skipped rather than read as 0.0 (which `.values()[i]` would do).
        let min_x_s = merged.get_column(ix).f64().ok()?;
        let min_y_s = merged.get_column(iy).f64().ok()?;
        let max_x_s = merged.get_column(ax).f64().ok()?;
        let max_y_s = merged.get_column(ay).f64().ok()?;
        (0..merged.len())
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
        let series = merged.get_column(build_col);
        let binary = get_geometry_binary(series).ok()?;
        (0..merged.len())
            .filter_map(|i| {
                let wkb = binary.get(i)?;
                let [min_x, min_y, max_x, max_y] = wkb_to_mbr(wkb)?;
                Some(RTreeEntry {
                    bbox: AABB::from_corners([min_x, min_y], [max_x, max_y]),
                    row_idx: i as u32,
                })
            })
            .collect()
    };

    Some((merged, RTree::bulk_load(entries)))
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
    let bbox_cols: Option<[usize; 4]> = {
        let schema = &tables[0].schema;
        let try_find = |name: &str| schema.get_index(name).ok();
        let candidates = [
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
    };

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
        let geom_cols = extract_geom_col_indices(
            &filter,
            build_side,
            output_schema.len(),
            build_n_cols,
        );
        let dwithin_distance = extract_dwithin_distance(filter.inner());
        Self { filter, output_schema, build_side, geom_cols, partition_key, dwithin_distance }
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
        // Fall back to single global R-tree when no partition key.
        if let Some((build_col, probe_col)) = self.geom_cols {
            if let Some((merged, rtree)) = build_rtree(&finalized_build_state, build_col) {
                return NestedLoopProbeState {
                    build_tables: vec![],
                    rtree_state: Some(RTreeState {
                        merged_build: merged,
                        rtree,
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
            state
                .rtree_state
                .as_ref()
                .map_or(state.build_tables.is_empty(), |rs| rs.merged_build.is_empty())
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
                                                .locate_in_envelope_intersecting(&q)
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
                        // ── R-tree accelerated path ───────────────────────────────
                        let merged_build = &rs.merged_build;
                        let rtree = &rs.rtree;

                        let probe_series = probe_tbl.get_column(rs.probe_geom_col);
                        let (cand_probe, cand_build, use_rtree) = if let Ok(binary) =
                            get_geometry_binary(probe_series)
                        {
                            // Probe each row in parallel: RTree is Sync so concurrent
                            // locate_in_envelope_intersecting calls are safe.  Each rayon
                            // task returns its (probe_idx, build_idx) pairs; we flatten
                            // them into the final index vectors afterwards.
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
                                    let q = AABB::from_corners(
                                        [min_x - pad, min_y - pad],
                                        [max_x + pad, max_y + pad],
                                    );
                                    rtree
                                        .locate_in_envelope_intersecting(&q)
                                        .map(|entry| (i as u64, entry.row_idx as u64))
                                        .collect()
                                })
                                .collect());
                            let total: usize = per_row.iter().map(|v| v.len()).sum();
                            let mut cp: Vec<u64> = Vec::with_capacity(total);
                            let mut cb: Vec<u64> = Vec::with_capacity(total);
                            for pairs in per_row {
                                for (pi, bi) in pairs {
                                    cp.push(pi);
                                    cb.push(bi);
                                }
                            }
                            (cp, cb, true)
                        } else {
                            (vec![], vec![], false)
                        };

                        if use_rtree {
                            let result = nested_loop_inner_join_indexed(
                                probe_tbl,
                                merged_build,
                                &filter,
                                build_side,
                                &cand_probe,
                                &cand_build,
                            )?;
                            if result.is_empty() {
                                MicroPartition::empty(Some(output_schema))
                            } else {
                                MicroPartition::new_loaded(
                                    output_schema,
                                    Arc::new(vec![result]),
                                    None,
                                )
                            }
                        } else {
                            // Probe column wasn't Binary — fall back to naive join.
                            let out = nested_loop_inner_join(
                                probe_tbl,
                                merged_build,
                                &filter,
                                build_side,
                            )?;
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
