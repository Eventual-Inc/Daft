use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_core::join::JoinSide;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{JoinType, partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::stats::BasicJoinStats;
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::RuntimeStatsRef,
};

/// A distributed join node that:
///   1. Hash-repartitions both sides by the equi-join key (same as `HashJoinNode`)
///   2. Within each local task, runs a `NestedLoopJoin` with R-tree acceleration
///      instead of a hash join — avoiding the cross-product materialization that
///      causes OOM for spatial joins with dense geohash cells.
///
/// The build side is the polygon side (buildings); the probe side is the point side.
pub(crate) struct SpatialHashJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Equi-join keys (for the distributed hash shuffle)
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    join_type: JoinType,

    // Which logical side (left/right of the original join) is the build side.
    // Convention: phys_left = probe, phys_right = build in the local NLJ.
    build_side: JoinSide,

    // The spatial predicate (e.g. st_contains), bound to the join output schema.
    spatial_filter: BoundExpr,

    left: DistributedPipelineNode,
    right: DistributedPipelineNode,
}

impl SpatialHashJoinNode {
    const NODE_NAME: &'static str = "SpatialHashJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        join_type: JoinType,
        build_side: JoinSide,
        spatial_filter: BoundExpr,
        num_partitions: usize,
        left: DistributedPipelineNode,
        right: DistributedPipelineNode,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::HashJoin, // reuse HashJoin metrics bucket
            NodeCategory::BlockingSink,
        );
        let partition_cols = left_on
            .iter()
            .chain(right_on.iter())
            .map(BoundExpr::inner)
            .cloned()
            .collect::<Vec<_>>();
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            Arc::new(HashClusteringConfig::new(num_partitions, partition_cols).into()),
        );
        Self {
            config,
            context,
            left_on,
            right_on,
            join_type,
            build_side,
            spatial_filter,
            left,
            right,
        }
    }
}

impl PipelineNodeImpl for SpatialHashJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(BasicJoinStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["SpatialHashJoin".to_string()];
        res.push(format!("Type: {}", self.join_type));
        res.push(format!("Build side: {}", self.build_side));
        res.push(format!(
            "Left equi key: {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right equi key: {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Spatial filter: {}", self.spatial_filter));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let left_input = self.left.clone().produce_tasks(plan_context);
        let right_input = self.right.clone().produce_tasks(plan_context);

        TaskBuilderStream::new(
            left_input
                .zip(right_input)
                .map(move |(left_task, right_task)| {
                    SwordfishTaskBuilder::combine_with(
                        &left_task,
                        &right_task,
                        self.as_ref(),
                        |left_plan, right_plan| {
                            // Convention (from daft-local-plan/src/translate.rs):
                            //   phys_left  = probe side
                            //   phys_right = build side
                            let (phys_left, phys_right) = match self.build_side {
                                // Original left is build → swap: probe=right, build=left
                                JoinSide::Left => (right_plan, left_plan),
                                // Original right is build → keep: probe=left, build=right
                                JoinSide::Right => (left_plan, right_plan),
                            };

                            LocalPhysicalPlan::nested_loop_join(
                                phys_left,
                                phys_right,
                                self.spatial_filter.clone(),
                                self.build_side,
                                None, // no per-key partition; each hash partition already
                                      // contains only rows with compatible partition keys
                                self.config.schema.clone(),
                                StatsState::NotMaterialized,
                                LocalNodeContext::new(Some(self.node_id() as usize)),
                            )
                        },
                    )
                })
                .boxed(),
        )
    }
}
