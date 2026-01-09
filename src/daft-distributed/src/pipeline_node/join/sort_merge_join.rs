use std::{future, sync::Arc};

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{JoinType, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        sort::{
            create_range_repartition_tasks, create_sample_tasks,
            get_partition_boundaries_from_samples,
        },
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
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
    const NODE_NAME: NodeName = "SortMergeJoin";

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
            Self::NODE_NAME,
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
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
        let (left_in_memory_source_plan, left_psets) =
            MaterializedOutput::into_in_memory_scan_with_psets(
                left_partition_group,
                self.left.config().schema.clone(),
                self.left.node_id(),
            );

        let (right_in_memory_source_plan, right_psets) =
            MaterializedOutput::into_in_memory_scan_with_psets(
                right_partition_group,
                self.right.config().schema.clone(),
                self.right.node_id(),
            );

        // Merge left and right psets
        let mut psets = left_psets;
        psets.extend(right_psets);

        // Build the join plan
        let plan = LocalPhysicalPlan::sort_merge_join(
            left_in_memory_source_plan,
            right_in_memory_source_plan,
            self.left_on.clone(),
            self.right_on.clone(),
            self.join_type,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        // Create the task
        let builder = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);

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
        let descending = vec![false; self.left_on.len()];
        let nulls_first = vec![false; self.left_on.len()];

        // Sample left side
        let left_sample_tasks = create_sample_tasks(
            left_materialized.clone(),
            self.left.config().schema.clone(),
            self.left_on.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        // Sample right side
        let right_sample_tasks = create_sample_tasks(
            right_materialized.clone(),
            self.right.config().schema.clone(),
            self.right_on.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        // Collect all samples
        let sampled_outputs = try_join_all(
            left_sample_tasks
                .into_iter()
                .chain(right_sample_tasks.into_iter()),
        )
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

        // Compute partition boundaries from combined samples
        let boundaries = get_partition_boundaries_from_samples(
            sampled_outputs,
            &self.left_on,
            descending.clone(),
            nulls_first,
            num_partitions,
        )
        .await?;

        // Range repartition left side
        let left_schema = self.left.config().schema.clone();
        let left_partition_tasks = create_range_repartition_tasks(
            left_materialized,
            left_schema,
            self.left_on.clone(),
            descending.clone(),
            boundaries.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        // Range repartition right side
        let right_schema = self.right.config().schema.clone();
        let right_partition_tasks = create_range_repartition_tasks(
            right_materialized,
            right_schema,
            self.right_on.clone(),
            descending,
            boundaries,
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        // Wait for both sides to be partitioned
        let (left_partitioned_outputs, right_partitioned_outputs) = futures::try_join!(
            try_join_all(left_partition_tasks),
            try_join_all(right_partition_tasks)
        )?;

        let left_partitioned_outputs = left_partitioned_outputs
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let right_partitioned_outputs = right_partitioned_outputs
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Transpose outputs to group by partition index
        let left_transposed_outputs =
            transpose_materialized_outputs_from_vec(left_partitioned_outputs, num_partitions);
        let right_transposed_outputs =
            transpose_materialized_outputs_from_vec(right_partitioned_outputs, num_partitions);

        // Emit sort-merge join tasks for each partition pair
        for (left_partition_group, right_partition_group) in left_transposed_outputs
            .into_iter()
            .zip(right_transposed_outputs)
        {
            self.create_and_submit_join_task(
                left_partition_group,
                right_partition_group,
                result_tx,
            )
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
