use std::{cmp::Ordering, future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use common_partitioning::PartitionRef;
use daft_core::{array::ops::build_multi_array_compare, prelude::UInt64Array};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream, TaskOutput,
        shuffles::partition_groups::{
            ray_partition_groups_and_sentinels_from_outputs, ray_partition_groups_from_outputs,
        },
        sort::{
            create_range_repartition_tasks, create_range_repartition_tasks_with_sentinels,
            create_sample_tasks, get_partition_boundaries_from_samples,
        },
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

pub(crate) struct AsofJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    num_partitions: usize,

    left: DistributedPipelineNode,
    right: DistributedPipelineNode,
}

impl AsofJoinNode {
    const NODE_NAME: &'static str = "AsofJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
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
            NodeType::AsofJoin,
            NodeCategory::BlockingSink,
        );
        let partition_cols = left_by
            .iter()
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
            left_by,
            right_by,
            left_on,
            right_on,
            num_partitions,
            left,
            right,
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["AsofJoin".to_string()];
        res.push(format!(
            "Left by: [{}]",
            self.left_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right by: [{}]",
            self.right_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Left on: {}", self.left_on));
        res.push(format!("Right on: {}", self.right_on));
        res.push(format!("Num partitions: {}", self.num_partitions));
        res
    }

    /// Creates and submits an asof join task for a partition pair, embedding the sentinel
    /// row (if any) directly into the plan payload.
    async fn create_and_submit_join_task(
        self: &Arc<Self>,
        partition_idx: u32,
        left_partition_group: Vec<PartitionRef>,
        right_partition_group: Vec<PartitionRef>,
        right_sentinel: Option<RecordBatch>,
        result_tx: &Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let left_shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            self.left.node_id(),
            self.left.config().schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.left.node_id() as usize)),
        );

        let right_shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            self.right.node_id(),
            self.right.config().schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.right.node_id() as usize)),
        );

        let plan = LocalPhysicalPlan::asof_join(
            left_shuffle_read_plan,
            right_shuffle_read_plan,
            self.left_by.clone(),
            self.right_by.clone(),
            self.left_on.clone(),
            self.right_on.clone(),
            right_sentinel,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
        );

        let builder = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
            .extend_fingerprint(partition_idx)
            .with_psets(self.left.node_id(), left_partition_group)
            .with_psets(self.right.node_id(), right_partition_group);

        result_tx.send(builder).await.ok();
        Ok(())
    }

    /// Handles the multi-partition join case.
    /// Samples both sides with the composite (by, on) key, computes range boundaries,
    /// repartitions left normally and right with sentinel tracking, merges sentinel
    /// candidates per partition, forward-fills, then dispatches join tasks.
    async fn range_shuffle_and_join(
        self: Arc<Self>,
        left_materialized: Vec<MaterializedOutput>,
        right_materialized: Vec<MaterializedOutput>,
        task_id_counter: &TaskIDCounter,
        result_tx: &Sender<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let num_partitions = self.num_partitions;

        // When num_partitions == 1, skip range shuffle entirely.
        // get_partition_boundaries_from_samples returns empty boundaries for n=1
        // (no splits needed), which would error with "No boundaries found".
        if num_partitions == 1 {
            let left_refs = left_materialized
                .into_iter()
                .flat_map(|mo| mo.into_inner().0)
                .collect::<Vec<_>>();
            let right_refs = right_materialized
                .into_iter()
                .flat_map(|mo| mo.into_inner().0)
                .collect::<Vec<_>>();
            return self
                .create_and_submit_join_task(0, left_refs, right_refs, None, result_tx)
                .await;
        }

        let left_composite_key: Vec<BoundExpr> = self
            .left_by
            .iter()
            .chain(std::iter::once(&self.left_on))
            .cloned()
            .collect();
        let right_composite_key: Vec<BoundExpr> = self
            .right_by
            .iter()
            .chain(std::iter::once(&self.right_on))
            .cloned()
            .collect();

        let descending = vec![false; left_composite_key.len()];
        let nulls_first = vec![false; left_composite_key.len()];

        let left_sample_tasks = create_sample_tasks(
            left_materialized.clone(),
            self.left.config().schema.clone(),
            left_composite_key.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
            Some(0),
        )?;

        let left_boundary_key_names = left_composite_key
            .iter()
            .map(|expr| {
                expr.inner()
                    .to_field(&self.left.config().schema)
                    .map(|f| f.name)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let right_sample_by_aliased = right_composite_key
            .iter()
            .zip(left_boundary_key_names.into_iter())
            .map(|(expr, key_name)| BoundExpr::new_unchecked(expr.inner().alias(key_name)))
            .collect::<Vec<_>>();

        let right_sample_tasks = create_sample_tasks(
            right_materialized.clone(),
            self.right.config().schema.clone(),
            right_sample_by_aliased,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
            Some(1),
        )?;

        let combined_sampled_outputs = try_join_all(
            left_sample_tasks
                .into_iter()
                .chain(right_sample_tasks.into_iter()),
        )
        .await?
        .into_iter()
        .flatten()
        .map(TaskOutput::into_materialized)
        .collect::<DaftResult<Vec<_>>>()?;

        let left_partition_boundaries = get_partition_boundaries_from_samples(
            combined_sampled_outputs,
            &left_composite_key,
            descending.clone(),
            nulls_first,
            num_partitions,
        )
        .await?;

        // Range repartition left side (no sentinel tracking needed)
        let left_partition_tasks = create_range_repartition_tasks(
            left_materialized,
            self.left.config().schema.clone(),
            left_composite_key,
            descending.clone(),
            left_partition_boundaries.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
            Some(0),
        )?;

        let right_boundary_names = right_composite_key
            .iter()
            .map(|expr| {
                expr.inner()
                    .to_field(&self.right.config().schema)
                    .map(|f| f.name)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let right_partition_boundaries = RecordBatch::from_nonempty_columns(
            left_partition_boundaries
                .columns()
                .iter()
                .zip(right_boundary_names)
                .map(|(col, name)| col.as_materialized_series().rename(&name))
                .collect::<Vec<_>>(),
        )?;

        // Range repartition right side with sentinel tracking
        let right_partition_tasks = create_range_repartition_tasks_with_sentinels(
            right_materialized,
            self.right.config().schema.clone(),
            right_composite_key.clone(),
            descending,
            right_partition_boundaries,
            num_partitions,
            right_composite_key.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
            Some(1),
        )?;

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

        let left_partition_groups =
            ray_partition_groups_from_outputs(left_partitioned_outputs, num_partitions)?;
        let (right_partition_groups, sentinel_candidates_per_partition) =
            ray_partition_groups_and_sentinels_from_outputs(
                right_partitioned_outputs,
                num_partitions,
            )?;

        // Merge sentinels: for each partition j, pick the single globally max (by..., on)
        // row across all worker candidates, then forward-fill Nones from the previous partition.
        let mut global_sentinels: Vec<Option<RecordBatch>> = Vec::with_capacity(num_partitions);
        for candidates in sentinel_candidates_per_partition {
            let valid: Vec<RecordBatch> = candidates.into_iter().flatten().collect();
            let global_sentinel = if valid.is_empty() {
                None
            } else {
                let combined = RecordBatch::concat(&valid)?;
                record_batch_max_composite(&combined, &right_composite_key)?
            };
            global_sentinels.push(global_sentinel);
        }

        // Forward-fill: partition j with no sentinel inherits the sentinel of partition j-1.
        // Partition 0 always gets None (no previous partition).
        for j in 1..num_partitions {
            if global_sentinels[j].is_none() {
                let prev = global_sentinels[j - 1].clone();
                global_sentinels[j] = prev;
            }
        }

        // Dispatch one join task per partition, embedding global_sentinels[i-1] in the plan
        for (i, (left_partition_group, right_partition_group)) in left_partition_groups
            .into_iter()
            .zip(right_partition_groups)
            .enumerate()
        {
            let sentinel = if i == 0 {
                None
            } else {
                global_sentinels[i - 1].clone()
            };
            self.create_and_submit_join_task(
                i as u32,
                left_partition_group,
                right_partition_group,
                sentinel,
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

        if left_materialized.is_empty() {
            return Ok(());
        }

        if right_materialized.is_empty() {
            let all_left: Vec<PartitionRef> = left_materialized
                .iter()
                .flat_map(|o| o.partitions().iter().cloned())
                .collect();
            return self
                .create_and_submit_join_task(0, all_left, vec![], None, &result_tx)
                .await;
        }

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

impl PipelineNodeImpl for AsofJoinNode {
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

/// Find the single row with the lexicographically maximum composite key `(by..., on)` in
/// `batch`. Returns `None` if `batch` is empty.
///
/// `composite_keys` is `[by_keys..., on_key]` sorted ascending — the max row is the one
/// that sorts last under `(by ASC..., on ASC)`.
fn record_batch_max_composite(
    batch: &RecordBatch,
    composite_keys: &[BoundExpr],
) -> DaftResult<Option<RecordBatch>> {
    if batch.is_empty() {
        return Ok(None);
    }

    let cols: Vec<_> = composite_keys
        .iter()
        .map(|k| batch.eval_expression(k))
        .collect::<DaftResult<_>>()?;
    let descending = vec![false; composite_keys.len()];
    let nulls_first = vec![false; composite_keys.len()];
    let cmp = build_multi_array_compare(&cols, &descending, &nulls_first)?;
    let max_idx = (1..batch.len()).fold(0usize, |best, i| {
        if cmp(i, best) == Ordering::Greater {
            i
        } else {
            best
        }
    });
    let idx = UInt64Array::from_vec("idx", vec![max_idx as u64]);
    Ok(Some(batch.take(&idx)?))
}
