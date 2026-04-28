use std::{collections::HashMap, future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};

use super::stats::BasicJoinStats;
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        sort::{
            create_range_repartition_tasks, create_sample_tasks,
            get_partition_boundaries_from_samples,
        },
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskBuilder},
        worker::WorkerId,
    },
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

const PER_WORKER_CARRYOVER_PHASE: &str = "per_worker_carryover";
const PER_PARTITION_CARRYOVER_PHASE: &str = "per_partition_carryover";

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

    async fn create_and_submit_join_task(
        self: &Arc<Self>,
        partition_idx: u32,
        left_partition_group: Vec<MaterializedOutput>,
        right_partition_group: Vec<MaterializedOutput>,
        carryover_mo: Option<MaterializedOutput>,
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

        let mut right_psets = right_partition_group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        if let Some(carryover) = carryover_mo {
            right_psets.extend(carryover.into_inner().0);
        }

        let plan = LocalPhysicalPlan::asof_join(
            left_shuffle_read_plan,
            right_shuffle_read_plan,
            self.left_by.clone(),
            self.right_by.clone(),
            self.left_on.clone(),
            self.right_on.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
        );

        let builder = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
            .extend_fingerprint(partition_idx)
            .with_psets(self.left.node_id(), left_psets)
            .with_psets(self.right.node_id(), right_psets);

        result_tx.send(builder).await.ok();
        Ok(())
    }

    /// Samples both sides, computes range boundaries, repartitions, then dispatches join tasks.
    async fn range_shuffle_and_join(
        self: Arc<Self>,
        left_materialized: Vec<MaterializedOutput>,
        right_materialized: Vec<MaterializedOutput>,
        task_id_counter: &TaskIDCounter,
        result_tx: &Sender<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let num_partitions = self.num_partitions;

        if num_partitions == 1 {
            return self
                .create_and_submit_join_task(
                    0,
                    left_materialized,
                    right_materialized,
                    None,
                    result_tx,
                )
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

        let left_sample_tasks = create_sample_tasks(
            left_materialized.clone(),
            self.left.config().schema.clone(),
            left_composite_key.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
            Some(0),
        )?;

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
        .collect::<Vec<_>>();

        let left_partition_boundaries = get_partition_boundaries_from_samples(
            combined_sampled_outputs,
            &left_composite_key,
            descending.clone(),
            nulls_first,
            num_partitions,
        )
        .await?;

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

        let right_partition_tasks = create_range_repartition_tasks(
            right_materialized,
            self.right.config().schema.clone(),
            right_composite_key.clone(),
            descending,
            right_partition_boundaries,
            num_partitions,
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
        let right_partitioned_outputs: Vec<MaterializedOutput> =
            right_partitioned_outputs.into_iter().flatten().collect();

        let carryover_tasks = create_per_worker_carryover_tasks(
            right_partitioned_outputs.clone(),
            self.right.config().schema.clone(),
            right_composite_key.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        let per_worker_carryover_mos: Vec<Vec<MaterializedOutput>> =
            try_join_all(carryover_tasks.into_iter().map(try_join_all))
                .await?
                .into_iter()
                .map(|bucket| bucket.into_iter().flatten().collect())
                .collect();

        let per_partition_carryover_tasks = create_per_partition_carryover_tasks(
            per_worker_carryover_mos,
            self.right.config().schema.clone(),
            right_composite_key.clone(),
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        let mut global_carryovers: Vec<Option<MaterializedOutput>> =
            try_join_all(per_partition_carryover_tasks.into_iter().map(|t| async {
                match t {
                    Some(task) => task.await.map(|mo| mo.filter(|m| m.num_rows() > 0)),
                    None => Ok(None),
                }
            }))
            .await?;

        // Forward-propagate: if bucket i produced no carryover, inherit from bucket i-1.
        for i in 1..global_carryovers.len() {
            if global_carryovers[i].is_none() {
                let prev = global_carryovers[i - 1].clone();
                global_carryovers[i] = prev;
            }
        }

        let left_partition_groups =
            crate::utils::transpose::transpose_materialized_outputs_from_vec(
                left_partitioned_outputs,
                num_partitions,
            );

        let right_partition_groups =
            crate::utils::transpose::transpose_materialized_outputs_from_vec(
                right_partitioned_outputs,
                num_partitions,
            );

        for (i, (left_group, right_group)) in left_partition_groups
            .into_iter()
            .zip(right_partition_groups)
            .enumerate()
        {
            let carryover = if i == 0 {
                None
            } else {
                global_carryovers[i - 1].clone()
            };
            self.create_and_submit_join_task(
                i as u32,
                left_group,
                right_group,
                carryover,
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
            return self
                .create_and_submit_join_task(0, left_materialized, vec![], None, &result_tx)
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

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(BasicJoinStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
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

/// Builds and submits a single top_n(limit=1, descending=true) task over `inputs`.
#[allow(clippy::too_many_arguments)]
fn submit_top1_task(
    inputs: Vec<MaterializedOutput>,
    input_schema: SchemaRef,
    composite_keys: &[BoundExpr],
    phase: &str,
    strategy: Option<SchedulingStrategy>,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<SubmittedTask> {
    let node_id = pipeline_node.node_id();
    let descending = vec![true; composite_keys.len()];
    let nulls_first = vec![false; composite_keys.len()];

    let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
        inputs,
        input_schema,
        node_id,
        phase,
    );

    let plan = LocalPhysicalPlan::top_n(
        in_memory_scan,
        composite_keys.to_vec(),
        descending,
        nulls_first,
        1,
        None,
        StatsState::NotMaterialized,
        LocalNodeContext::new(Some(node_id as usize)).with_phase(phase),
    );

    SwordfishTaskBuilder::new(plan, pipeline_node, node_id)
        .with_psets(node_id, psets)
        .with_strategy(strategy)
        .build(pipeline_node.context().query_idx, task_id_counter)
        .submit(scheduler_handle)
}

/// Creates top_n(limit=1, descending=true) tasks per (bucket, worker) pair to compute
/// carryover (max) rows from the right-side range-repartitioned outputs.
///
/// Returns `Vec<Vec<SubmittedTask>>` where outer index = bucket_idx, inner = per-worker tasks.
fn create_per_worker_carryover_tasks(
    materialized_outputs: Vec<MaterializedOutput>,
    input_schema: SchemaRef,
    composite_keys: Vec<BoundExpr>,
    num_partitions: usize,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<Vec<Vec<SubmittedTask>>> {
    let mut worker_groups: HashMap<WorkerId, Vec<Vec<MaterializedOutput>>> = HashMap::new();
    for mo in materialized_outputs {
        let worker_id = mo.worker_id().clone();
        worker_groups
            .entry(worker_id)
            .or_default()
            .push(mo.split_into_materialized_outputs());
    }

    let mut tasks_by_bucket = Vec::with_capacity(num_partitions);
    for bucket_idx in 0..num_partitions {
        let mut bucket_tasks = vec![];
        for (worker_id, split_mos) in &worker_groups {
            let mos_for_bucket: Vec<MaterializedOutput> = split_mos
                .iter()
                .map(|splits| splits[bucket_idx].clone())
                .collect();

            let submitted = submit_top1_task(
                mos_for_bucket,
                input_schema.clone(),
                &composite_keys,
                PER_WORKER_CARRYOVER_PHASE,
                Some(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker_id.clone(),
                    soft: false,
                }),
                pipeline_node,
                task_id_counter,
                scheduler_handle,
            )?;

            bucket_tasks.push(submitted);
        }
        tasks_by_bucket.push(bucket_tasks);
    }

    Ok(tasks_by_bucket)
}

/// Creates a single top_n(limit=1, descending=true) task per partition over the per-worker
/// top-1 outputs produced by `create_per_worker_carryover_tasks`, yielding the global max per partition.
///
/// Returns `Vec<Option<SubmittedTask>>` where `None` means the partition had no data.
fn create_per_partition_carryover_tasks(
    per_worker_carryover_mos: Vec<Vec<MaterializedOutput>>,
    input_schema: SchemaRef,
    composite_keys: Vec<BoundExpr>,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<Vec<Option<SubmittedTask>>> {
    per_worker_carryover_mos
        .into_iter()
        .map(|worker_mos| {
            let non_empty: Vec<MaterializedOutput> = worker_mos
                .into_iter()
                .filter(|mo| mo.num_rows() > 0)
                .collect();

            if non_empty.is_empty() {
                return Ok(None);
            }

            submit_top1_task(
                non_empty,
                input_schema.clone(),
                &composite_keys,
                PER_PARTITION_CARRYOVER_PHASE,
                None,
                pipeline_node,
                task_id_counter,
                scheduler_handle,
            )
            .map(Some)
        })
        .collect()
}
