use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_dsl::{
    Expr, ExprRef,
    expr::{Column, bound_expr::BoundExpr},
};
use daft_geo::wkb_to_mbr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{RecordBatch, nested_loop_inner_join, nested_loop_inner_join_indexed};
use rstar::{AABB, RTree, RTreeObject};
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

// ── Spatial function names ────────────────────────────────────────────────

const SPATIAL_FNS: &[&str] = &[
    "st_intersects", "st_contains", "st_within", "st_covers",
    "st_covered_by", "st_disjoint", "st_touches", "st_overlaps",
    "st_crosses", "st_equals",
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

// ── R-tree construction ───────────────────────────────────────────────────

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
    let series = merged.get_column(build_col);
    let binary = series.binary().ok()?;

    let entries: Vec<RTreeEntry> = (0..merged.len())
        .filter_map(|i| {
            let wkb = binary.get(i)?;
            let [min_x, min_y, max_x, max_y] = wkb_to_mbr(wkb)?;
            Some(RTreeEntry {
                bbox: AABB::from_corners([min_x, min_y], [max_x, max_y]),
                row_idx: i as u32,
            })
        })
        .collect();

    Some((merged, RTree::bulk_load(entries)))
}

// ── Operator state ────────────────────────────────────────────────────────

pub(crate) struct NestedLoopBuildState {
    tables: Vec<RecordBatch>,
}

pub(crate) struct NestedLoopProbeState {
    build_tables: Vec<RecordBatch>,
    rtree_state: Option<RTreeState>,
    stream_idx: usize,
}

// ── Operator ──────────────────────────────────────────────────────────────

pub struct NestedLoopJoinOperator {
    filter: BoundExpr,
    output_schema: SchemaRef,
    build_side: JoinSide,
    /// `Some((build_geom_col, probe_geom_col))` when R-tree is applicable.
    geom_cols: Option<(usize, usize)>,
}

impl NestedLoopJoinOperator {
    /// `build_n_cols` = column count of the build-side physical plan schema.
    pub fn new(
        filter: BoundExpr,
        output_schema: SchemaRef,
        build_side: JoinSide,
        build_n_cols: usize,
    ) -> Self {
        let geom_cols = extract_geom_col_indices(
            &filter,
            build_side,
            output_schema.len(),
            build_n_cols,
        );
        Self { filter, output_schema, build_side, geom_cols }
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
        if let Some((build_col, probe_col)) = self.geom_cols {
            if let Some((merged, rtree)) = build_rtree(&finalized_build_state, build_col) {
                return NestedLoopProbeState {
                    build_tables: vec![],
                    rtree_state: Some(RTreeState {
                        merged_build: merged,
                        rtree,
                        probe_geom_col: probe_col,
                    }),
                    stream_idx: 0,
                };
            }
        }
        NestedLoopProbeState {
            build_tables: finalized_build_state,
            rtree_state: None,
            stream_idx: 0,
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        let build_is_empty = state
            .rtree_state
            .as_ref()
            .map_or(state.build_tables.is_empty(), |rs| rs.merged_build.is_empty());

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

        spawner
            .spawn(
                async move {
                    let probe_tables = input.record_batches();
                    let probe_tbl = &probe_tables[state.stream_idx];

                    let output_mp = if let Some(ref rs) = state.rtree_state {
                        // ── R-tree accelerated path ───────────────────────────────
                        let merged_build = &rs.merged_build;
                        let rtree = &rs.rtree;

                        let probe_series = probe_tbl.get_column(rs.probe_geom_col);
                        let (cand_probe, cand_build, use_rtree) = if let Ok(binary) =
                            probe_series.binary()
                        {
                            // Conservative capacity: assume ~few candidates per probe row.
                            let hint = probe_tbl.len() * 4;
                            let mut cp: Vec<u64> = Vec::with_capacity(hint);
                            let mut cb: Vec<u64> = Vec::with_capacity(hint);
                            for i in 0..probe_tbl.len() {
                                let Some(wkb) = binary.get(i) else {
                                    continue;
                                };
                                let Some([min_x, min_y, max_x, max_y]) = wkb_to_mbr(wkb)
                                else {
                                    continue;
                                };
                                let q = AABB::from_corners(
                                    [min_x, min_y],
                                    [max_x, max_y],
                                );
                                for entry in rtree.locate_in_envelope_intersecting(&q) {
                                    cp.push(i as u64);
                                    cb.push(entry.row_idx as u64);
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
