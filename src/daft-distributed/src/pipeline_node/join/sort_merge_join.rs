use std::{future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{JoinType, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        sort::range_repartition_two_sides,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::stats::RuntimeStatsRef,
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_vec,
    },
};

pub(crate) struct SortMergeJoinNode {
    // Node properties
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    join_type: JoinType,
    num_partitions: usize,

    // Child nodes
    left: DistributedPipelineNode,
    right: DistributedPipelineNode,
}

impl SortMergeJoinNode {
    const NODE_NAME: &'static str = "SortMergeJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        join_type: JoinType,
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
            NodeType::SortMergeJoin,
            NodeCategory::StreamingSink,
        );
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            left.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left,
            right,
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["SortMergeJoin".to_string()];
        res.push(format!("Type: {}", self.join_type));
        res.push(format!(
            "Left: Join key = {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right: Join key = {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Num partitions: {}", self.num_partitions));
        res
    }

    /// Creates and submits a sort-merge join task for a pair of partition groups.
    async fn create_and_submit_join_task(
        self: &Arc<Self>,
        left_partition_group: Vec<MaterializedOutput>,
        right_partition_group: Vec<MaterializedOutput>,
        result_tx: &Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let left_shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            self.left.node_id(),
            self.left.config().schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.left.node_id() as usize)),
        );

        let left_psets = left_partition_group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        let right_shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            self.right.node_id(),
            self.right.config().schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.right.node_id() as usize)),
        );

        let right_psets = right_partition_group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        // Build the join plan
        let plan = LocalPhysicalPlan::sort_merge_join(
            left_shuffle_read_plan,
            right_shuffle_read_plan,
            self.left_on.clone(),
            self.right_on.clone(),
            self.join_type,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
        );

        // Create the task
        let builder = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
            .with_psets(self.left.node_id(), left_psets)
            .with_psets(self.right.node_id(), right_psets);

        result_tx.send(builder).await.ok();
        Ok(())
    }

    /// Handles the multi-partition join case.
    /// This involves sampling, computing partition boundaries, range repartitioning both sides,
    /// and emitting join tasks for each partition pair.
    async fn range_shuffle_and_join(
        self: Arc<Self>,
        left_materialized: Vec<MaterializedOutput>,
        right_materialized: Vec<MaterializedOutput>,
        task_id_counter: &TaskIDCounter,
        result_tx: &Sender<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let num_partitions = self.num_partitions;

        let (left_partitioned, right_partitioned) = range_repartition_two_sides(
            left_materialized,
            right_materialized,
            self.left_on.clone(),
            self.right_on.clone(),
            self.left.config().schema.clone(),
            self.right.config().schema.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )
        .await?;

        let left_transposed =
            transpose_materialized_outputs_from_vec(left_partitioned, num_partitions);
        let right_transposed =
            transpose_materialized_outputs_from_vec(right_partitioned, num_partitions);

        for (left_group, right_group) in left_transposed.into_iter().zip(right_transposed) {
            self.create_and_submit_join_task(left_group, right_group, result_tx)
                .await?;
        }
        Ok(())
    }

    async fn execution_loop(
        self: Arc<Self>,
        left_inputs: TaskBuilderStream,
        right_inputs: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Materialize both inputs
        let left_materialized = left_inputs
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_filter(|output| future::ready(output.num_rows() > 0))
            .try_collect::<Vec<_>>()
            .await?;

        let right_materialized = right_inputs
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_filter(|output| future::ready(output.num_rows() > 0))
            .try_collect::<Vec<_>>()
            .await?;

        // Handle empty inputs
        if left_materialized.is_empty() || right_materialized.is_empty() {
            return Ok(());
        }

        // Special case: if both sides have only 1 partition, just do a direct join
        if left_materialized.len() == 1 && right_materialized.len() == 1 {
            self.create_and_submit_join_task(left_materialized, right_materialized, &result_tx)
                .await
        } else {
            // Multi-partition join case: sample, repartition, and join
            self.range_shuffle_and_join(
                left_materialized,
                right_materialized,
                &task_id_counter,
                &result_tx,
                &scheduler_handle,
            )
            .await
        }
    }
}

impl PipelineNodeImpl for SortMergeJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        self.multiline_display()
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(super::stats::BasicJoinStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let left_input = self.left.clone().produce_tasks(plan_context);
        let right_input = self.right.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);
        plan_context.spawn(self.execution_loop(
            left_input,
            right_input,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}
