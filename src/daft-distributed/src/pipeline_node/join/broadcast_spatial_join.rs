use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_core::join::JoinSide;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};

use super::broadcast_join::BroadcastJoinStats;
use crate::{
    pipeline_node::{
        ClusteringStrategy, DistributedPipelineNode, MaterializedOutput, NodeID,
        PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

/// A distributed join node for PURE spatial `on=` predicates (no equi keys):
///
/// 1. Materializes and broadcasts the SMALL side (chosen by the planner under
///    `broadcast_join_size_bytes_threshold`) to every receiver task — no
///    shuffle of either side.
/// 2. Within each local task, runs the R-tree-accelerated `NestedLoopJoin`
///    with the broadcast side as the build side.
///
/// Because every receiver task sees the complete build side, no partition key
/// is needed and no matches can be lost across partition boundaries.
pub(crate) struct BroadcastSpatialJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    /// The full ON predicate (pure spatial), bound to the join output schema.
    spatial_filter: BoundExpr,
    /// Which LOGICAL side of the join is broadcast (and therefore the NLJ
    /// build side).
    build_side: JoinSide,

    broadcaster: DistributedPipelineNode,
    broadcaster_schema: SchemaRef,
    receiver: DistributedPipelineNode,
    runtime_stats: Arc<BroadcastJoinStats>,
}

impl BroadcastSpatialJoinNode {
    const NODE_NAME: &'static str = "BroadcastSpatialJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        spatial_filter: BoundExpr,
        build_side: JoinSide,
        broadcaster: DistributedPipelineNode,
        receiver: DistributedPipelineNode,
        output_schema: SchemaRef,
        meter: &Meter,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::BroadcastJoin, // reuse BroadcastJoin metrics bucket
            NodeCategory::BlockingSink,
        );
        // The broadcaster is replicated to every task, so the output keeps the
        // receiver's clustering.
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            ClusteringStrategy::Passthrough { child: &receiver },
        );
        let broadcaster_schema = broadcaster.config().schema.clone();
        let runtime_stats = Arc::new(BroadcastJoinStats::new(meter, &context));

        Self {
            config,
            context,
            spatial_filter,
            build_side,
            broadcaster,
            broadcaster_schema,
            receiver,
            runtime_stats,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        broadcaster_input: TaskBuilderStream,
        mut receiver_input: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_broadcast_data = broadcaster_input
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_collect::<Vec<_>>()
            .await?;

        let build_rows = materialized_broadcast_data
            .iter()
            .map(|output| output.num_rows())
            .sum::<usize>();
        let build_bytes = materialized_broadcast_data
            .iter()
            .map(|output| output.size_bytes())
            .sum::<usize>();
        self.runtime_stats
            .set_build_rows_inserted(build_rows as u64);
        self.runtime_stats
            .set_build_bytes_inserted(build_bytes as u64);

        let (materialized_broadcast_data_plan, broadcast_psets) =
            MaterializedOutput::into_in_memory_scan_with_psets(
                materialized_broadcast_data,
                self.broadcaster_schema.clone(),
                self.node_id(),
            );
        while let Some(builder) = receiver_input.next().await {
            let new_builder = builder
                .map_plan(self.as_ref(), |input_plan| {
                    // Local NLJ convention (daft-local-plan/src/translate.rs):
                    //   arg0 = probe plan, arg1 = build plan, and `build_side`
                    //   names the LOGICAL side that builds. The broadcast side
                    //   is always the build side; the receiver task streams as
                    //   the probe.
                    LocalPhysicalPlan::nested_loop_join(
                        input_plan,
                        materialized_broadcast_data_plan.clone(),
                        self.spatial_filter.clone(),
                        self.build_side,
                        None, // no equality partition key — pure spatial join
                        self.config.schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.node_id() as usize)),
                    )
                })
                .with_psets(self.node_id(), broadcast_psets.clone());

            if result_tx.send(new_builder).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl PipelineNodeImpl for BroadcastSpatialJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.broadcaster.clone(), self.receiver.clone()]
    }

    fn make_runtime_stats(&self, _meter: &Meter) -> RuntimeStatsRef {
        self.runtime_stats.clone()
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![
            "BroadcastSpatialJoin".to_string(),
            format!("Spatial filter: {}", self.spatial_filter),
            format!("Broadcast (build) side: {}", self.build_side),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let broadcaster_input = self.broadcaster.clone().produce_tasks(plan_context);
        let receiver_input = self.receiver.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            broadcaster_input,
            receiver_input,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
