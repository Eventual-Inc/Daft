use core::panic;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::Meter;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use daft_core::join::JoinSide;
use daft_dsl::{
    Expr,
    clustering_is_covered_by,
    expr::{
        Column, ResolvedColumn,
        agg::extract_agg_expr,
        bound_expr::{BoundAggExpr, BoundExpr, BoundVLLMExpr, BoundWindowExpr},
    },
    join::strip_join_side_cols,
    left_col, resolved_col, right_col, unresolved_col,
};
use daft_logical_plan::{
    JoinOptions, JoinType, LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    ops,
    partitioning::{HashRepartitionConfig, RepartitionSpec},
};
use daft_scan::{ScanState, scan_task_iters};
use daft_schema::schema::Schema;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, concat::ConcatNode, distinct::DistinctNode,
        explode::ExplodeNode, filter::FilterNode, glob_scan_source::GlobScanSourceNode,
        in_memory_source::InMemorySourceNode, into_batches::IntoBatchesNode, join::BroadcastSpatialJoinNode,
        into_partitions::IntoPartitionsNode, limit::LimitNode,
        monotonically_increasing_id::MonotonicallyIncreasingIdNode, pivot::PivotNode,
        project::ProjectNode, random_shuffle::RandomShuffleNode, sample::SampleNode,
        scan_source::ScanSourceNode, sink::SinkNode, sort::SortNode,
        stage_checkpoint_keys::StageCheckpointKeysNode, top_n::TopNNode, udf::UDFNode,
        unpivot::UnpivotNode, vllm::VLLMNode, window::WindowNode,
    },
    plan::PlanConfig,
};

pub(crate) struct TranslationOutput {
    pub root: DistributedPipelineNode,
    pub hints: Vec<String>,
}

pub(crate) fn logical_plan_to_pipeline_node(
    plan_config: PlanConfig,
    plan: LogicalPlanRef,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    meter: &Meter,
) -> DaftResult<TranslationOutput> {
    let mut translator =
        LogicalPlanToPipelineNodeTranslator::new(plan_config, psets, meter.clone());
    let _ = plan.visit(&mut translator)?;
    Ok(TranslationOutput {
        root: translator.curr_node.pop().unwrap(),
        hints: translator.hints,
    })
}

/// Spatial function names whose presence in a Filter predicate above a Join triggers
/// the NLJ R-tree rewrite instead of hash-join + post-filter.
// Single source of truth: `daft_dsl::join` (the operator's acceleration list
// in daft-local-execution intentionally differs — no st_disjoint).
const SPATIAL_PREDICATES: &[&str] = daft_dsl::join::SPATIAL_JOIN_PREDICATES;

/// Check whether `expr` contains at least one spatial predicate call.
fn is_spatial_predicate(expr: &daft_dsl::ExprRef) -> bool {
    let mut found = false;
    let _ = expr.apply(|e: &daft_dsl::ExprRef| {
        if let Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) = e.as_ref() {
            if SPATIAL_PREDICATES.contains(&sf.func.name()) {
                found = true;
                return Ok(common_treenode::TreeNodeRecursion::Stop);
            }
        }
        Ok(common_treenode::TreeNodeRecursion::Continue)
    });
    found
}

/// Re-bind `expr` to `schema` by column name.
///
/// `BoundExpr::try_new` only works for fully-unresolved expressions. This helper
/// first converts every `Bound` column back to an unresolved reference (preserving
/// the field name) and then calls `BoundExpr::try_new` so indices are correct for
/// the new schema.  Mirrors the identical function in `daft-local-plan/src/translate.rs`.
fn rebind_predicate(
    expr: daft_dsl::ExprRef,
    schema: &daft_schema::schema::Schema,
) -> DaftResult<BoundExpr> {
    let unbound = expr
        .transform(|e| {
            if let Expr::Column(Column::Bound(bc)) = e.as_ref() {
                Ok(Transformed::yes(daft_dsl::unresolved_col(bc.field.name.as_ref())))
            } else {
                Ok(Transformed::no(e))
            }
        })?
        .data;
    BoundExpr::try_new(unbound, schema)
}

pub(crate) struct LogicalPlanToPipelineNodeTranslator {
    pub plan_config: PlanConfig,
    pub meter: Meter,
    pipeline_node_id_counter: NodeID,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    curr_node: Vec<DistributedPipelineNode>,
    /// Pointer values of `Filter` logical-plan nodes that were fully translated in
    /// `f_down` (spatial NLJ rewrite).  Their `f_up` call must be a no-op.
    spatial_filter_ptrs: HashSet<*const LogicalPlan>,
    pub(crate) hints: Vec<String>,
}

impl LogicalPlanToPipelineNodeTranslator {
    pub(crate) fn new(
        plan_config: PlanConfig,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        meter: Meter,
    ) -> Self {
        Self {
            plan_config,
            meter,
            pipeline_node_id_counter: 0,
            psets,
            curr_node: Vec::new(),
            spatial_filter_ptrs: HashSet::new(),
            hints: Vec::new(),
        }
    }

    /// Translate `plan` using a fresh portion of the `curr_node` stack.
    ///
    /// Saves the current stack, runs the visitor on `plan`, extracts the single
    /// result node, then restores the saved stack.  Used by the spatial NLJ
    /// rewrite in `f_down` to translate join children before the main traversal
    /// would visit them.
    fn translate_subtree(
        &mut self,
        plan: &LogicalPlanRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let saved = std::mem::take(&mut self.curr_node);
        let _ = plan.visit(self)?;
        let result = self
            .curr_node
            .pop()
            .expect("translate_subtree: visitor produced no node");
        self.curr_node = saved;
        Ok(result)
    }

    /// Build a `SpatialHashJoinNode` for `join`, hash-partitioning both sides by
    /// `left_eq_keys`/`right_eq_keys` and running a local R-tree nested-loop join
    /// filtered by `combined_predicate`.
    ///
    /// `combined_predicate` is the FULL semantics the local NLJ must enforce — equi
    /// conjuncts AND the spatial predicate — with `ResolvedColumn::JoinSide` markers
    /// already stripped (via `strip_join_side_cols`) so it can be rebound against
    /// `join.output_schema`. Hash-partitioning only co-locates equal keys onto the same
    /// task; it does NOT prevent different keys from sharing a partition, so the local
    /// NLJ filter is the only place join semantics are actually enforced.
    ///
    /// Mirrors `build_spatial_nested_loop_join` in `daft-local-plan/src/translate.rs`
    /// (the native runner's equivalent rewrite), adapted to translate the join's
    /// children into distributed pipeline nodes and hash-repartition them instead of
    /// running everything within one local task.
    ///
    /// Shared by both spatial-join rewrite shapes in `f_down`:
    ///   - `Filter(spatial_pred) -> [Project?] -> Join(equi_keys)`
    ///   - bare `Join(equi_keys AND spatial residual)`
    ///
    /// Returns `Ok(None)` if the equi-key dtypes mismatch across sides — hashing raw
    /// values of different dtypes can partition equal logical values into different
    /// tasks, and the local filter can't recover cross-partition rows, so the caller
    /// must fall back to the normal (dtype-normalizing) join translation.
    fn build_spatial_hash_join_node(
        &mut self,
        join: &ops::Join,
        combined_predicate: daft_dsl::ExprRef,
        left_eq_keys: &[daft_dsl::ExprRef],
        right_eq_keys: &[daft_dsl::ExprRef],
    ) -> DaftResult<Option<DistributedPipelineNode>> {
        let dtypes_match = left_eq_keys.iter().zip(right_eq_keys.iter()).all(|(l, r)| {
            match (
                l.to_field(&join.left.schema()),
                r.to_field(&join.right.schema()),
            ) {
                (Ok(lf), Ok(rf)) => lf.dtype == rf.dtype,
                _ => false,
            }
        });
        if !dtypes_match {
            return Ok(None);
        }

        // Translate join children into distributed pipeline nodes.
        let left_node = self.translate_subtree(&join.left)?;
        let right_node = self.translate_subtree(&join.right)?;

        // Bind equi-join keys to the respective child schemas.
        let left_on = BoundExpr::bind_all(left_eq_keys, &left_node.config().schema)?;
        let right_on = BoundExpr::bind_all(right_eq_keys, &right_node.config().schema)?;

        let join_schema = join.output_schema.clone();
        let spatial_filter = rebind_predicate(combined_predicate, &join_schema)?;

        // Determine the build side: arg0 of the spatial function is the "container"
        // geometry (polygon / building footprint) and should be the build side for the
        // R-tree.
        let left_schema_len = join.left.schema().len();
        let build_on_left: bool = (|| {
            // `spatial_filter` has already been rebound to `join_schema`, so its
            // Bound-column indices are positions in the concatenated [left | right]
            // join output — valid for both call sites (Filter predicate combined with
            // equi keys, and the JoinSide-stripped bare-Join residual combined with
            // equi keys).
            if let Some(idx) = daft_dsl::join::spatial_join_arg0_bound_index(spatial_filter.inner()) {
                idx < left_schema_len
            } else {
                // Fallback: build on the side with fewer estimated rows.
                let ls = join.left.materialized_stats().approx_stats.num_rows;
                let rs = join.right.materialized_stats().approx_stats.num_rows;
                ls <= rs
            }
        })();

        let build_side = if build_on_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };

        let left_stats = join.left.materialized_stats().approx_stats.clone();
        let right_stats = join.right.materialized_stats().approx_stats.clone();

        let spatial_node = self.gen_spatial_hash_join_nodes(
            left_node,
            right_node,
            left_on,
            right_on,
            join.join_type,
            build_side,
            spatial_filter,
            // Output schema: always use the raw join output schema. The NLJ produces
            // probe ∪ build columns in that order.
            join_schema,
            &left_stats,
            &right_stats,
        )?;
        Ok(Some(spatial_node))
    }

    /// Plan a PURE spatial join (inner join, spatial ON, no equality
    /// conjuncts): broadcast the small side when one fits under the
    /// threshold; otherwise synthesize a grid partition key (explode by
    /// geohash covering cells + reference-cell dedup) so giant×giant joins
    /// run through the hash-partitioned spatial path; otherwise error with
    /// the available options.
    fn build_pure_spatial_join_node(
        &mut self,
        join: &ops::Join,
        combined_predicate: daft_dsl::ExprRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let left_bytes = join.left.materialized_stats().approx_stats.size_bytes;
        let right_bytes = join.right.materialized_stats().approx_stats.size_bytes;
        let threshold = self
            .plan_config
            .config
            .broadcast_join_size_bytes_threshold;
        if left_bytes.min(right_bytes) <= threshold {
            return self.build_broadcast_spatial_join_node(join, combined_predicate);
        }
        if let Some(node) = self.build_grid_spatial_join_node(join)? {
            return Ok(node);
        }
        Err(common_error::DaftError::not_implemented(format!(
            "A spatial join without a usable equality partition key on the distributed runner \
             requires one side small enough to broadcast (left ~{left_bytes} bytes, right \
             ~{right_bytes} bytes, broadcast_join_size_bytes_threshold = {threshold}), and this \
             predicate is not supported by the grid rewrite (st_dwithin, st_disjoint, \
             OR/NOT-composed predicates, and non-column geometry arguments are not supported). Options: raise the threshold via \
             daft.set_execution_config(broadcast_join_size_bytes_threshold=...), add an equality \
             partition key (e.g. a geohash cell) to the ON clause, or bucket the larger side and \
             join per bucket."
        )))
    }

    /// Synthesize a grid-partitioned plan for a giant×giant pure spatial join
    /// (the PBSM / reference-cell method):
    ///
    /// 1. Each side is augmented with its geometry's geohash COVERING cells
    ///    (`st_geohash_cells` of the bbox — strict: it errors rather than
    ///    silently dropping oversized coverings) and its bbox min corner,
    ///    then exploded to one row per covering cell.
    /// 2. The sides are equi-joined on the cell (riding the existing
    ///    hash-partitioned `SpatialHashJoinNode` path) with the original
    ///    spatial predicate AND a reference-cell conjunct:
    ///    `st_geohash(st_point(max(l_min_x, r_min_x), max(l_min_y, r_min_y))) == cell`.
    ///    The reference point is the min corner of the two bboxes'
    ///    intersection, which lies in BOTH bboxes, so its cell is in both
    ///    covers — every true pair meets in exactly that one cell and is
    ///    emitted exactly once. No post-join dedup needed.
    /// 3. A final projection restores the original join output schema.
    ///
    /// Returns `Ok(None)` when the ON shape isn't supported (predicate whose
    /// truth doesn't imply bbox intersection — st_dwithin, st_disjoint — or
    /// geometry arguments that aren't plain columns).
    fn build_grid_spatial_join_node(
        &mut self,
        join: &ops::Join,
    ) -> DaftResult<Option<DistributedPipelineNode>> {
        /// Truth implies the geometries' bounding boxes intersect — the
        /// soundness requirement for covering-cell co-partitioning.
        const GRID_SUPPORTED_PREDICATES: &[&str] = &[
            "st_contains", "st_intersects", "st_within", "st_covers",
            "st_covered_by", "st_touches", "st_overlaps", "st_crosses", "st_equals",
        ];
        /// Adaptive precision ladder. The container (arg0) side picks, PER
        /// ROW, the finest precision in [MIN, MAX] whose bbox covering stays
        /// within TARGET cells — building-scale rows stay at gh6 (~1.2 km ×
        /// 0.6 km cells), region-scale rows coarsen instead of erroring. The
        /// other side emits its covering at EVERY ladder precision (for a
        /// point: the geohash prefixes of its gh6 cell) so it meets build
        /// rows at whatever precision each chose.
        const GRID_MIN_PRECISION: u8 = 4;
        const GRID_MAX_PRECISION: u8 = 6;
        const GRID_TARGET_CELLS: u16 = 64;
        const CELL_L: &str = "__grid_cell_l";
        const CELL_R: &str = "__grid_cell_r";
        const MINX_L: &str = "__grid_min_x_l";
        const MINY_L: &str = "__grid_min_y_l";
        const MINX_R: &str = "__grid_min_x_r";
        const MINY_R: &str = "__grid_min_y_r";

        let Some(on_expr) = join.on.inner() else {
            return Ok(None);
        };

        // Find the spatial call, its per-side geometry COLUMN names, and
        // which join side arg0 (the container geometry) lives on.
        fn find_geoms(expr: &daft_dsl::ExprRef) -> Option<(String, String, JoinSide)> {
            match expr.as_ref() {
                Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf))
                    if GRID_SUPPORTED_PREDICATES.contains(&sf.name()) =>
                {
                    let side_col = |e: &daft_dsl::ExprRef| match e.as_ref() {
                        Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(f, side))) => {
                            Some((f.name.to_string(), *side))
                        }
                        _ => None,
                    };
                    let (n0, s0) = side_col(sf.inputs.required(0).ok()?)?;
                    let (n1, s1) = side_col(sf.inputs.required(1).ok()?)?;
                    match (s0, s1) {
                        (JoinSide::Left, JoinSide::Right) => Some((n0, n1, JoinSide::Left)),
                        (JoinSide::Right, JoinSide::Left) => Some((n1, n0, JoinSide::Right)),
                        _ => None,
                    }
                }
                Expr::BinaryOp {
                    op: daft_core::prelude::Operator::And,
                    left,
                    right,
                } => find_geoms(left).or_else(|| find_geoms(right)),
                _ => None,
            }
        }
        let Some((left_geom, right_geom, arg0_side)) = find_geoms(on_expr) else {
            return Ok(None);
        };

        // Augment a side: covering cells + bbox min corner, then explode.
        // `ignore_empty_and_null = true` drops rows with null/invalid
        // geometry — they can never satisfy the spatial predicate.
        // The container (arg0) side uses the ADAPTIVE covering; the other
        // side the LADDER covering.
        let augment = |plan: LogicalPlanRef,
                       geom: &str,
                       adaptive: bool,
                       cell: &str,
                       minx: &str,
                       miny: &str|
         -> DaftResult<LogicalPlanRef> {
            let bbox = daft_geo::st_bbox::st_bbox(unresolved_col(geom));
            let cells = if adaptive {
                daft_geo::st_geohash_cells_adaptive(
                    unresolved_col(geom),
                    GRID_MIN_PRECISION,
                    GRID_MAX_PRECISION,
                    GRID_TARGET_CELLS,
                )
            } else {
                daft_geo::st_geohash_cells_ladder(
                    unresolved_col(geom),
                    GRID_MIN_PRECISION,
                    GRID_MAX_PRECISION,
                )
            };
            let b = LogicalPlanBuilder::from(plan)
                .with_columns(vec![
                    cells.alias(cell),
                    daft_dsl::functions::struct_::get(bbox.clone(), "min_x").alias(minx),
                    daft_dsl::functions::struct_::get(bbox, "min_y").alias(miny),
                ])?
                .explode(vec![unresolved_col(cell)], true, None)?;
            Ok(b.plan)
        };
        let left_aug = augment(
            join.left.clone(),
            &left_geom,
            arg0_side == JoinSide::Left,
            CELL_L,
            MINX_L,
            MINY_L,
        )?;
        let right_aug = augment(
            join.right.clone(),
            &right_geom,
            arg0_side == JoinSide::Right,
            CELL_R,
            MINX_R,
            MINY_R,
        )?;

        // New ON: original spatial predicate AND cell equality AND the
        // reference-cell dedup conjunct.
        use daft_schema::{dtype::DataType, field::Field};
        let lcell = || left_col(Field::new(CELL_L, DataType::Utf8));
        let rcell = right_col(Field::new(CELL_R, DataType::Utf8));
        let lminx = left_col(Field::new(MINX_L, DataType::Float64));
        let lminy = left_col(Field::new(MINY_L, DataType::Float64));
        let rminx = right_col(Field::new(MINX_R, DataType::Float64));
        let rminy = right_col(Field::new(MINY_R, DataType::Float64));
        let gx = lminx
            .clone()
            .gt_eq(rminx.clone())
            .if_else(lminx, rminx);
        let gy = lminy
            .clone()
            .gt_eq(rminy.clone())
            .if_else(lminy, rminy);
        // Reference-cell dedup at the PAIR's precision: the adaptive side's
        // cell length IS the precision the pair met at, and geohash cells are
        // prefixes of their finer refinements — so compare the reference
        // point's finest-precision geohash TRUNCATED to that length. (The
        // equality conjunct makes lcell == rcell for matched pairs, so
        // comparing against the left cell is fully general.)
        let ref_cell_fine: daft_dsl::ExprRef = daft_dsl::functions::BuiltinScalarFn::new(
            daft_geo::StGeohash {
                precision: GRID_MAX_PRECISION,
            },
            vec![daft_geo::st_point::st_point(gx, gy)],
        )
        .into();
        let ref_cell_at_pair_precision = daft_functions_utf8::left(
            ref_cell_fine,
            daft_functions_utf8::utf8_length_bytes(lcell()),
        );
        let new_on = on_expr
            .clone()
            .and(lcell().eq(rcell))
            .and(ref_cell_at_pair_precision.eq(lcell()));

        let joined = LogicalPlanBuilder::from(left_aug).join(
            right_aug,
            Some(new_on),
            vec![],
            JoinType::Inner,
            None,
            JoinOptions::default(),
        )?;
        // Restore the ORIGINAL join output schema (drop helper columns).
        let out_cols: Vec<daft_dsl::ExprRef> = join
            .output_schema
            .fields()
            .iter()
            .map(|f| unresolved_col(f.name.as_ref()))
            .collect();
        let final_plan = joined.select(out_cols)?.plan;

        // Freshly built nodes have no materialized stats; the spatial hash
        // path downstream reads approx stats, so enrich before translating.
        use daft_logical_plan::optimization::{EnrichWithStats, OptimizerRule};
        let enriched = EnrichWithStats::new(Some(self.plan_config.config.clone()))
            .try_optimize(final_plan)?
            .data;

        Ok(Some(self.translate_subtree(&enriched)?))
    }

    /// Build a `BroadcastSpatialJoinNode` for a PURE spatial join — an inner
    /// join whose ON predicate is spatial with NO equality conjuncts, so there
    /// is no key to hash-partition by. The smaller side (by approx stats) is
    /// broadcast to every task and becomes the local NLJ's build side; every
    /// task therefore sees the complete build side and no matches can be lost
    /// across partitions.
    ///
    /// Errors when neither side fits under
    /// `broadcast_join_size_bytes_threshold` — a pure spatial join with two
    /// large sides needs either a partition key (e.g. a geohash cell) or
    /// bucketed execution, and silently falling back to a cross-join would
    /// OOM at exactly the scales where it matters.
    fn build_broadcast_spatial_join_node(
        &mut self,
        join: &ops::Join,
        combined_predicate: daft_dsl::ExprRef,
    ) -> DaftResult<DistributedPipelineNode> {
        let left_node = self.translate_subtree(&join.left)?;
        let right_node = self.translate_subtree(&join.right)?;

        let join_schema = join.output_schema.clone();
        let spatial_filter = rebind_predicate(combined_predicate, &join_schema)?;

        // Broadcast-side selection: PREFER the spatial function's arg0 side
        // (the container geometry — e.g. polygons in st_contains(poly, pt)):
        // it is the natural R-tree build side and typically the one carrying
        // precomputed `rtree_*` bbox columns. Fall back to the other side
        // when the preferred one doesn't fit under the threshold, and error
        // when neither does.
        let left_bytes = join.left.materialized_stats().approx_stats.size_bytes;
        let right_bytes = join.right.materialized_stats().approx_stats.size_bytes;
        let threshold = self
            .plan_config
            .config
            .broadcast_join_size_bytes_threshold;

        let left_schema_len = join.left.schema().len();
        let preferred = (|| {
            // Bound indices in `spatial_filter` are positions in [left | right].
            if let Some(idx) = daft_dsl::join::spatial_join_arg0_bound_index(spatial_filter.inner()) {
                if idx < left_schema_len {
                    JoinSide::Left
                } else {
                    JoinSide::Right
                }
            } else if left_bytes <= right_bytes {
                JoinSide::Left
            } else {
                JoinSide::Right
            }
        })();
        let (preferred_bytes, other_bytes, other) = match preferred {
            JoinSide::Left => (left_bytes, right_bytes, JoinSide::Right),
            JoinSide::Right => (right_bytes, left_bytes, JoinSide::Left),
        };
        let build_side = if preferred_bytes <= threshold {
            preferred
        } else if other_bytes <= threshold {
            other
        } else {
            return Err(common_error::DaftError::not_implemented(format!(
                "A spatial join without a usable equality partition key on the distributed runner \
                 requires one side small enough to broadcast: left ~{left_bytes} bytes, right \
                 ~{right_bytes} bytes, broadcast_join_size_bytes_threshold = {threshold}. Options: \
                 raise the threshold via \
                 daft.set_execution_config(broadcast_join_size_bytes_threshold=...), add an \
                 equality partition key (e.g. a geohash cell) to the ON clause, or bucket the \
                 larger side and join per bucket."
            )));
        };
        let (broadcaster, receiver) = match build_side {
            JoinSide::Left => (left_node, right_node),
            JoinSide::Right => (right_node, left_node),
        };

        let node_id = self.get_next_pipeline_node_id();
        Ok(DistributedPipelineNode::new(
            Arc::new(BroadcastSpatialJoinNode::new(
                node_id,
                &self.plan_config,
                spatial_filter,
                build_side,
                broadcaster,
                receiver,
                join_schema,
                &self.meter,
            )),
            &self.meter,
        ))
    }

    pub fn get_next_pipeline_node_id(&mut self) -> NodeID {
        self.pipeline_node_id_counter += 1;
        self.pipeline_node_id_counter
    }

    /// Record a user-facing hint. Hints are surfaced as a Python `UserWarning` when the plan
    /// runs, and embedded inline in the rendered plan when the plan is displayed via
    /// `repr_ascii` — display itself is side-effect free so that warnings can't interleave
    /// with the plan's stdout output.
    pub(crate) fn record_hint(&mut self, msg: String) {
        self.hints.push(msg);
    }

    pub(crate) fn can_skip_hash_repartition(
        input_node: &DistributedPipelineNode,
        partition_columns: &[BoundExpr],
    ) -> DaftResult<bool> {
        let input_clustering_spec = &input_node.config().clustering_spec;
        // If there is only one partition, we can skip the shuffle
        if input_clustering_spec.num_partitions() == 1 {
            return Ok(true);
        }

        // The clustering keys are already bound (to the input node's schema). We can skip the
        // shuffle if partitioning by the operator's columns keeps that clustering intact — the
        // partition columns exactly match the input clustering or are a superset of it (each
        // operator group is then contained within a single input partition).
        // `clustering_is_covered_by` handles both cases.
        let is_compatible = if input_clustering_spec.is_hash() {
            clustering_is_covered_by(input_clustering_spec.partition_by(), partition_columns)
        } else {
            false
        };

        Ok(is_compatible)
    }
}

impl TreeNodeVisitor for LogicalPlanToPipelineNodeTranslator {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        // ── Spatial join rewrite (shape 1) ──────────────────────────────────────
        // Detect Filter(spatial_pred) → [Project?] → Join(equi_keys, Inner) and
        // translate the whole pattern into a SpatialHashJoinNode (hash-shuffle by
        // equi key, NLJ R-tree locally).  We handle it in f_down so that we can
        // translate the join children ourselves and skip the automatic subtree
        // traversal (return Jump).
        //
        // This is the `.where(spatial_pred)` shape: the equality conjuncts live on
        // the Join's own `on` and the spatial predicate lives in a separate Filter
        // immediately above it.
        if let LogicalPlan::Filter(filter) = node.as_ref() {
            if is_spatial_predicate(&filter.predicate) {
                // Look through an optional intermediate Project (optimizer-inserted)
                // to find the inner Join.
                let inner_join: Option<&ops::Join> = match filter.input.as_ref() {
                    LogicalPlan::Join(j) if j.join_type == JoinType::Inner => Some(j),
                    // Only look through PLAIN column projections (see the
                    // native translator's identical guard).
                    LogicalPlan::Project(p)
                        if p.projection.iter().all(|e| {
                            matches!(e.as_ref(), Expr::Column(_))
                        }) =>
                    {
                        match p.input.as_ref() {
                            LogicalPlan::Join(j) if j.join_type == JoinType::Inner => Some(j),
                            _ => None,
                        }
                    }
                    _ => None,
                };

                if let Some(join) = inner_join {
                    let (remaining, left_eq_keys, right_eq_keys, _null_equals_nulls) =
                        join.on.split_eq_preds();

                    // Only rewrite when the join has equi-keys and no remaining
                    // non-equi predicates (those would need separate handling).
                    let merges_columns = join.output_schema.len()
                        != join.left.schema().len() + join.right.schema().len();
                    if remaining.is_empty() && !left_eq_keys.is_empty() && !merges_columns {
                        // The local NLJ filter is the only place join semantics are
                        // enforced: hash-partitioning co-locates equal keys but does
                        // NOT prevent different keys from sharing a partition. Carry
                        // the full ON predicate (equality conjuncts) alongside the
                        // WHERE spatial predicate.
                        let mut combined_predicate = filter.predicate.clone();
                        if let Some(on_expr) = join.on.inner() {
                            combined_predicate =
                                combined_predicate.and(strip_join_side_cols(on_expr.clone())?);
                        }

                        if let Some(spatial_node) = self.build_spatial_hash_join_node(
                            join,
                            combined_predicate,
                            &left_eq_keys,
                            &right_eq_keys,
                        )? {
                            // If there is an intermediate Project between the Filter
                            // and the Join (inserted by the optimizer to deduplicate
                            // join-key columns), wrap the SHJ with a ProjectNode so
                            // that the SHJ output matches filter.input.schema() =
                            // p.output_schema. Without this, downstream nodes
                            // (Distinct, outer Project) bind their column indices
                            // against p.output_schema but receive join.output_schema
                            // at runtime — causing type mismatches (e.g. my_id: Int64
                            // receiving a String array).
                            let spatial_node = if let LogicalPlan::Project(p) =
                                filter.input.as_ref()
                            {
                                let projection = BoundExpr::bind_all(
                                    &p.projection,
                                    p.input.schema().as_ref(),
                                )?;
                                DistributedPipelineNode::new(
                                    Arc::new(ProjectNode::new(
                                        self.get_next_pipeline_node_id(),
                                        &self.plan_config,
                                        projection,
                                        p.projected_schema.clone(),
                                        spatial_node,
                                    )),
                                    &self.meter,
                                )
                            } else {
                                spatial_node
                            };

                            self.curr_node.push(spatial_node);
                            // Mark this Filter node so that f_up is a no-op for it.
                            let ptr = Arc::as_ptr(node) as *const LogicalPlan;
                            self.spatial_filter_ptrs.insert(ptr);
                            // Skip the entire subtree — we translated the children above.
                            return Ok(TreeNodeRecursion::Jump);
                        }
                    }
                }
            }
            return Ok(TreeNodeRecursion::Continue);
        }

        // ── Spatial join rewrite (shape 2) ──────────────────────────────────────
        // `df.join(other, on=(a == b) & st_intersects(...))` — the natural, combined
        // `on=` predicate — has NO separate Filter node: `split_eq_preds` splits the
        // Join's own `on` into equi-keys plus a spatial residual, both living on the
        // bare Join. Route the same way as shape 1: the residual becomes part of the
        // NLJ filter (combined with the equi keys, exactly like `combined_predicate`
        // above), and only fires for `JoinType::Inner` with at least one equi-key. If
        // the residual is a non-spatial predicate, leave it alone — that is a
        // genuinely different (unimplemented) non-equality-join feature, not this
        // rewrite's concern.
        if let LogicalPlan::Join(join) = node.as_ref() {
            if join.join_type == JoinType::Inner {
                let (remaining, left_eq_keys, right_eq_keys, _null_equals_nulls) =
                    join.on.split_eq_preds();

                if remaining.inner().is_some_and(is_spatial_predicate) {
                    // No separate WHERE filter here — the full ON predicate (equi
                    // conjuncts AND spatial residual) already IS the complete
                    // semantics the local NLJ must enforce.
                    let full_on = join
                        .on
                        .inner()
                        .expect("non-empty residual implies a non-empty on expr")
                        .clone();
                    let combined_predicate = strip_join_side_cols(full_on)?;

                    let spatial_node = if left_eq_keys.is_empty() {
                        // PURE spatial join — no key to hash-partition by.
                        // Broadcast when a side is small; synthesize a grid
                        // key otherwise; error only when neither applies.
                        Some(self.build_pure_spatial_join_node(join, combined_predicate)?)
                    } else {
                        match self.build_spatial_hash_join_node(
                            join,
                            combined_predicate.clone(),
                            &left_eq_keys,
                            &right_eq_keys,
                        )? {
                            Some(node) => Some(node),
                            // The hash-partitioned path declined (equi-key
                            // dtype mismatch across sides: hashing raw values
                            // could split equal logical keys into different
                            // partitions). Broadcasting needs no partitioning
                            // at all, and the NLJ filter carries the FULL ON
                            // predicate including the equality conjuncts — so
                            // it is a sound fallback, and better than the
                            // generic non-equality-join error.
                            None => Some(
                                self.build_broadcast_spatial_join_node(join, combined_predicate)?,
                            ),
                        }
                    };

                    if let Some(spatial_node) = spatial_node {
                        self.curr_node.push(spatial_node);
                        // Mark this Join node so that f_up is a no-op for it.
                        let ptr = Arc::as_ptr(node) as *const LogicalPlan;
                        self.spatial_filter_ptrs.insert(ptr);
                        // Skip the entire subtree — we translated the children above.
                        return Ok(TreeNodeRecursion::Jump);
                    }
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &LogicalPlanRef) -> DaftResult<TreeNodeRecursion> {
        // If this node was fully translated in f_down (spatial NLJ rewrite), the
        // SpatialHashJoinNode is already on the stack.  Skip the normal translation.
        {
            let ptr = Arc::as_ptr(node) as *const LogicalPlan;
            if self.spatial_filter_ptrs.remove(&ptr) {
                return Ok(TreeNodeRecursion::Continue);
            }
        }

        let output = match node.as_ref() {
            LogicalPlan::Source(source) => {
                match source.source_info.as_ref() {
                    SourceInfo::InMemory(info) => DistributedPipelineNode::new(
                        Arc::new(InMemorySourceNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            info.clone(),
                            self.psets.clone(),
                        )),
                        &self.meter,
                    ),
                    SourceInfo::Physical(info) => {
                        let scan_tasks = match &info.scan_state {
                            ScanState::Operator(_) => unreachable!(
                                "ScanOperator should not be present in the optimized logical plan for pipeline node translation"
                            ),
                            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                        };

                        // Perform scan task splitting and merging.
                        let scan_tasks = if self.plan_config.config.enable_scan_task_split_and_merge
                        {
                            scan_task_iters::split_and_merge_pass(
                                scan_tasks,
                                &info.pushdowns,
                                &self.plan_config.config,
                            )?
                        } else {
                            scan_tasks
                        };
                        DistributedPipelineNode::new(
                            Arc::new(ScanSourceNode::new(
                                self.get_next_pipeline_node_id(),
                                &self.plan_config,
                                info.pushdowns.clone(),
                                scan_tasks,
                                source.output_schema.clone(),
                                info.clustering_keys.clone(),
                            )?),
                            &self.meter,
                        )
                    }
                    SourceInfo::GlobScan(info) => DistributedPipelineNode::new(
                        Arc::new(GlobScanSourceNode::try_new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            info.glob_paths.clone(),
                            info.pushdowns.clone(),
                            source.output_schema.clone(),
                            info.io_config.clone().map(|c| *c),
                        )?),
                        &self.meter,
                    ),
                    SourceInfo::PlaceHolder(_) => unreachable!(
                        "PlaceHolder should not be present in the logical plan for pipeline node translation"
                    ),
                }
            }
            LogicalPlan::UDFProject(udf) if udf.is_actor_pool_udf() => {
                #[cfg(feature = "python")]
                {
                    let udf_expr = BoundExpr::try_new(udf.expr.clone(), &udf.input.schema())?;
                    let passthrough_columns =
                        BoundExpr::bind_all(&udf.passthrough_columns, &udf.input.schema())?;
                    DistributedPipelineNode::new(
                        Arc::new(crate::pipeline_node::actor_udf::ActorUDF::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            udf_expr,
                            passthrough_columns,
                            udf.udf_properties.clone(),
                            udf.projected_schema.clone(),
                            self.curr_node.pop().unwrap(),
                        )?),
                        &self.meter,
                    )
                }
                #[cfg(not(feature = "python"))]
                {
                    panic!("ActorUDF is not supported without Python feature")
                }
            }
            LogicalPlan::UDFProject(udf) => {
                let expr = BoundExpr::try_new(udf.expr.clone(), &udf.input.schema())?;
                let passthrough_columns =
                    BoundExpr::bind_all(&udf.passthrough_columns, &udf.input.schema())?;

                DistributedPipelineNode::new(
                    Arc::new(UDFNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        expr,
                        udf.udf_properties.clone(),
                        passthrough_columns,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Filter(filter) => {
                let predicate =
                    BoundExpr::try_new(filter.predicate.clone(), &filter.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(FilterNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        predicate,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::StageCheckpointKeys(stage) => DistributedPipelineNode::new(
                Arc::new(StageCheckpointKeysNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    stage.checkpoint_config.clone(),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::IntoBatches(into_batches) => DistributedPipelineNode::new(
                Arc::new(IntoBatchesNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    into_batches.batch_size,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Limit(limit) => DistributedPipelineNode::new(
                Arc::new(LimitNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    limit.limit as usize,
                    limit.offset.map(|x| x as usize),
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Project(project) => {
                let projection = BoundExpr::bind_all(&project.projection, &project.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(ProjectNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        projection,
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Explode(explode) => {
                let to_explode = BoundExpr::bind_all(&explode.to_explode, &explode.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(ExplodeNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        to_explode,
                        explode.ignore_empty_and_null,
                        explode.index_column.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Unpivot(unpivot) => {
                let ids = BoundExpr::bind_all(&unpivot.ids, &unpivot.input.schema())?;
                let values = BoundExpr::bind_all(&unpivot.values, &unpivot.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(UnpivotNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        ids,
                        values,
                        unpivot.variable_name.clone(),
                        unpivot.value_name.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Sample(sample) => DistributedPipelineNode::new(
                Arc::new(SampleNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    sample.fraction,
                    sample.size,
                    sample.with_replacement,
                    sample.seed,
                    node.schema(),
                    self.curr_node.pop().unwrap(),
                )),
                &self.meter,
            ),
            LogicalPlan::Sink(sink) => {
                let sink_info = sink.sink_info.bind(&sink.input.schema())?;
                DistributedPipelineNode::new(
                    Arc::new(SinkNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sink_info.into(),
                        sink.schema.clone(),
                        sink.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                DistributedPipelineNode::new(
                    Arc::new(MonotonicallyIncreasingIdNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        monotonically_increasing_id.column_name.clone(),
                        node.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Concat(_) => DistributedPipelineNode::new(
                Arc::new(ConcatNode::new(
                    self.get_next_pipeline_node_id(),
                    &self.plan_config,
                    node.schema(),
                    self.curr_node.pop().unwrap(), // Other
                    self.curr_node.pop().unwrap(), // Child
                )),
                &self.meter,
            ),
            LogicalPlan::Repartition(repartition) => match &repartition.repartition_spec {
                RepartitionSpec::Hash(_)
                | RepartitionSpec::Random(_)
                | RepartitionSpec::Range(_) => {
                    let child = self.curr_node.pop().unwrap();
                    let input_size_bytes = repartition
                        .input
                        .materialized_stats()
                        .approx_stats
                        .size_bytes;
                    self.gen_repartition_node(
                        repartition.repartition_spec.clone(),
                        node.schema(),
                        child,
                        input_size_bytes,
                    )?
                }
            },
            LogicalPlan::IntoPartitions(into_partitions) => {
                let backend = self.select_backend();
                DistributedPipelineNode::new(
                    Arc::new(IntoPartitionsNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        into_partitions.num_partitions,
                        node.schema(),
                        backend,
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Aggregate(aggregate) => {
                let input_schema = aggregate.input.schema();
                let group_by = BoundExpr::bind_all(&aggregate.groupby, &input_schema)?;
                let aggregations = aggregate
                    .aggregations
                    .iter()
                    .map(|expr| {
                        let agg_expr = extract_agg_expr(expr)?;
                        BoundAggExpr::try_new(agg_expr, &aggregate.input.schema())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let input_node = self.curr_node.pop().unwrap();
                let input_size_bytes = aggregate.input.materialized_stats().approx_stats.size_bytes;
                self.gen_agg_nodes(
                    input_node,
                    group_by.clone(),
                    aggregations,
                    aggregate.output_schema.clone(),
                    group_by,
                    input_size_bytes,
                )?
            }
            LogicalPlan::Distinct(distinct) => {
                let columns = distinct.columns.clone().unwrap_or_else(|| {
                    distinct
                        .input
                        .schema()
                        .field_names()
                        .map(resolved_col)
                        .collect::<Vec<_>>()
                });
                let columns = BoundExpr::bind_all(&columns, &distinct.input.schema())?;
                let input_node = self.curr_node.pop().unwrap();

                // Check if we can elide the repartition
                if Self::can_skip_hash_repartition(&input_node, &columns)? {
                    DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns,
                            distinct.input.schema(),
                            input_node,
                        )),
                        &self.meter,
                    )
                } else {
                    let input_size_bytes =
                        distinct.input.materialized_stats().approx_stats.size_bytes;
                    // Need full 2-stage distinct with shuffle
                    // First stage: Initial local distinct to reduce the dataset
                    let initial_distinct = DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns.clone(),
                            distinct.input.schema(),
                            input_node,
                        )),
                        &self.meter,
                    );

                    // Second stage: Repartition to distribute the dataset
                    let repartition = self.gen_repartition_node(
                        RepartitionSpec::Hash(HashRepartitionConfig::new(
                            None,
                            columns.clone().into_iter().map(|e| e.into()).collect(),
                        )),
                        distinct.input.schema(),
                        initial_distinct,
                        input_size_bytes,
                    )?;

                    // Last stage: Redo the distinct to get the final result
                    DistributedPipelineNode::new(
                        Arc::new(DistinctNode::new(
                            self.get_next_pipeline_node_id(),
                            &self.plan_config,
                            columns,
                            distinct.input.schema(),
                            repartition,
                        )),
                        &self.meter,
                    )
                }
            }
            LogicalPlan::Window(window) => {
                let partition_by =
                    BoundExpr::bind_all(&window.window_spec.partition_by, &window.input.schema())?;
                let order_by =
                    BoundExpr::bind_all(&window.window_spec.order_by, &window.input.schema())?;
                let window_functions =
                    BoundWindowExpr::bind_all(&window.window_functions, &window.input.schema())?;

                // First stage: Shuffle by the partition_by columns to colocate rows
                let input_node = self.curr_node.pop().unwrap();
                let input_size_bytes = window.input.materialized_stats().approx_stats.size_bytes;
                let repartition = if partition_by.is_empty() {
                    self.gen_gather_node(input_node, input_size_bytes)
                } else if Self::can_skip_hash_repartition(&input_node, &partition_by)? {
                    input_node
                } else {
                    self.gen_repartition_node(
                        RepartitionSpec::Hash(HashRepartitionConfig::new(
                            None,
                            partition_by.clone().into_iter().map(|e| e.into()).collect(),
                        )),
                        window.input.schema(),
                        input_node,
                        input_size_bytes,
                    )?
                };

                // Final stage: The actual window op
                DistributedPipelineNode::new(
                    Arc::new(WindowNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        partition_by,
                        order_by,
                        window.window_spec.descending.clone(),
                        window.window_spec.nulls_first.clone(),
                        window.window_spec.frame.clone(),
                        window.window_spec.min_periods,
                        window_functions,
                        window.aliases.clone(),
                        window.schema.clone(),
                        repartition,
                    )?),
                    &self.meter,
                )
            }
            LogicalPlan::Join(join) => {
                // Visitor appends in in-order
                // TODO: Just use regular recursion?
                let right_node = self.curr_node.pop().unwrap();
                let left_node = self.curr_node.pop().unwrap();

                self.translate_join(join, left_node, right_node)?
            }
            LogicalPlan::AsofJoin(asof_join) => {
                let right_node = self.curr_node.pop().unwrap();
                let left_node = self.curr_node.pop().unwrap();
                self.translate_asof_join(asof_join, left_node, right_node)?
            }
            LogicalPlan::Sort(sort) => {
                let sort_by = BoundExpr::bind_all(&sort.sort_by, &sort.input.schema())?;

                DistributedPipelineNode::new(
                    Arc::new(SortNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by,
                        sort.descending.clone(),
                        sort.nulls_first.clone(),
                        sort.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::TopN(top_n) => {
                let sort_by = BoundExpr::bind_all(&top_n.sort_by, &top_n.input.schema())?;

                // First stage: Perform a local topN
                let local_topn = DistributedPipelineNode::new(
                    Arc::new(TopNNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by.clone(),
                        top_n.descending.clone(),
                        top_n.nulls_first.clone(),
                        top_n.limit + top_n.offset.unwrap_or(0),
                        Some(0),
                        top_n.input.schema(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                );

                // Second stage: Gather all data to a single node
                let input_size_bytes = top_n.input.materialized_stats().approx_stats.size_bytes;
                let gather = self.gen_gather_node(local_topn, input_size_bytes);

                // Final stage: Do another topN to get the final result
                DistributedPipelineNode::new(
                    Arc::new(TopNNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        sort_by,
                        top_n.descending.clone(),
                        top_n.nulls_first.clone(),
                        top_n.limit,
                        top_n.offset,
                        top_n.input.schema(),
                        gather,
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Pivot(pivot) => {
                let input_schema = pivot.input.schema();
                let group_by = BoundExpr::bind_all(&pivot.group_by, &input_schema)?;
                let pivot_column = BoundExpr::try_new(pivot.pivot_column.clone(), &input_schema)?;
                let value_column = BoundExpr::try_new(pivot.value_column.clone(), &input_schema)?;
                let aggregation = BoundAggExpr::try_new(pivot.aggregation.clone(), &input_schema)?;

                let input_node = self.curr_node.pop().unwrap();

                let group_by_with_pivot = {
                    let mut gb = group_by.clone();
                    gb.push(pivot_column.clone());
                    gb
                };
                // Generate the output schema for the aggregation
                let output_fields = group_by_with_pivot
                    .iter()
                    .map(|expr| expr.inner().to_field(&pivot.input.schema()))
                    .chain(std::iter::once(
                        aggregation.inner().to_field(&pivot.input.schema()),
                    ))
                    .collect::<DaftResult<Vec<_>>>()?;
                let output_schema = Arc::new(Schema::new(output_fields));

                // First stage: Local aggregation with group_by + pivot_column
                let input_size_bytes = pivot.input.materialized_stats().approx_stats.size_bytes;
                let agg = self.gen_agg_nodes(
                    input_node,
                    group_by_with_pivot,
                    vec![aggregation.clone()],
                    output_schema,
                    group_by.clone(),
                    input_size_bytes,
                )?;

                // Final stage: Pivot transformation
                DistributedPipelineNode::new(
                    Arc::new(PivotNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        group_by,
                        pivot_column,
                        value_column,
                        aggregation,
                        pivot.names.clone(),
                        pivot.output_schema.clone(),
                        agg,
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::VLLMProject(vllm_project) => {
                let input_schema = vllm_project.input.schema();
                let expr = BoundVLLMExpr::try_new(vllm_project.expr.clone(), &input_schema)?;

                DistributedPipelineNode::new(
                    Arc::new(VLLMNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        expr,
                        vllm_project.output_column_name.clone(),
                        vllm_project.output_schema.clone(),
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::Shuffle(shuffle) => {
                let backend = self.select_backend();
                DistributedPipelineNode::new(
                    Arc::new(RandomShuffleNode::new(
                        self.get_next_pipeline_node_id(),
                        &self.plan_config,
                        shuffle.seed,
                        shuffle.input.schema(),
                        backend,
                        self.curr_node.pop().unwrap(),
                    )),
                    &self.meter,
                )
            }
            LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Intersect(_)
            | LogicalPlan::Shard(_)
            | LogicalPlan::Offset(_) => {
                panic!(
                    "Logical plan operator {} should be handled by the optimizer",
                    node.name()
                )
            }
        };
        self.curr_node.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}
