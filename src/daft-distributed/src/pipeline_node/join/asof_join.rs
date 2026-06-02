use std::{collections::HashMap, future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{AsofJoinStrategy, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};

use super::stats::BasicJoinStats;
use crate::{
    pipeline_node::{
        ClusteringStrategy, DistributedPipelineNode, MaterializedOutput, NodeID,
        PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        clustering::BoundClusteringSpec, sort::range_repartition_two_sides,
        translate::LogicalPlanToPipelineNodeTranslator,
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

const PARTIAL_CARRYOVER_BACKWARD_PHASE: &str = "partial_carryover_backward";
const FINAL_CARRYOVER_BACKWARD_PHASE: &str = "final_carryover_backward";
const PARTIAL_CARRYOVER_FORWARD_PHASE: &str = "partial_carryover_forward";
const FINAL_CARRYOVER_FORWARD_PHASE: &str = "final_carryover_forward";

pub(crate) struct AsofJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    strategy: AsofJoinStrategy,
    num_partitions: usize,
    needs_range_repartition: bool,

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
        strategy: AsofJoinStrategy,
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
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            ClusteringStrategy::Explicit(BoundClusteringSpec::hash(
                num_partitions,
                left_by.clone(),
            )),
        );
        let left_composite: Vec<BoundExpr> = left_by
            .iter()
            .chain(std::iter::once(&left_on))
            .cloned()
            .collect();
        let right_composite: Vec<BoundExpr> = right_by
            .iter()
            .chain(std::iter::once(&right_on))
            .cloned()
            .collect();
        let needs_range_repartition =
            LogicalPlanToPipelineNodeTranslator::needs_range_repartition(&left, &left_composite)
                || LogicalPlanToPipelineNodeTranslator::needs_range_repartition(
                    &right,
                    &right_composite,
                );
        Self {
            config,
            context,
            left_by,
            right_by,
            left_on,
            right_on,
            strategy,
            num_partitions,
            needs_range_repartition,
            left,
            right,
        }
    }

    async fn create_and_submit_join_task(
        self: &Arc<Self>,
        left_partition_group: Vec<MaterializedOutput>,
        right_partition_group: Vec<MaterializedOutput>,
        carryovers: (Option<MaterializedOutput>, Option<MaterializedOutput>),
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

        let (backward_carryover, forward_carryover) = carryovers;
        if let Some(carryover) = backward_carryover {
            right_psets.extend(carryover.into_inner().0);
        }
        if let Some(carryover) = forward_carryover {
            right_psets.extend(carryover.into_inner().0);
        }

        let plan = LocalPhysicalPlan::asof_join(
            left_shuffle_read_plan,
            right_shuffle_read_plan,
            self.left_by.clone(),
            self.right_by.clone(),
            self.left_on.clone(),
            self.right_on.clone(),
            self.strategy,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(self.node_id() as usize)),
        );

        let builder = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
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
                    left_materialized,
                    right_materialized,
                    (None, None),
                    result_tx,
                )
                .await;
        }

        let left_composite_key = self.left_composite_key();
        let right_composite_key = self.right_composite_key();

        let (left_partitioned_outputs, right_partitioned_outputs) = range_repartition_two_sides(
            left_materialized,
            right_materialized,
            left_composite_key,
            right_composite_key,
            self.left.config().schema.clone(),
            self.right.config().schema.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )
        .await?;

        // backward_carryovers[i] = max of bucket i (forward-propagated): used as carryover for bucket i+1.
        // forward_carryovers[i]  = min of bucket i (backward-propagated): used as carryover for bucket i-1.
        let (backward_carryovers, forward_carryovers) = match self.strategy {
            AsofJoinStrategy::Backward => {
                let backward_carryovers = self
                    .compute_carryovers(
                        right_partitioned_outputs.clone(),
                        true,
                        task_id_counter,
                        scheduler_handle,
                    )
                    .await?;
                (
                    backward_carryovers,
                    vec![None::<MaterializedOutput>; num_partitions],
                )
            }
            AsofJoinStrategy::Forward => {
                let forward_carryovers = self
                    .compute_carryovers(
                        right_partitioned_outputs.clone(),
                        false,
                        task_id_counter,
                        scheduler_handle,
                    )
                    .await?;
                (
                    vec![None::<MaterializedOutput>; num_partitions],
                    forward_carryovers,
                )
            }
            AsofJoinStrategy::Nearest => tokio::try_join!(
                self.compute_carryovers(
                    right_partitioned_outputs.clone(),
                    true,
                    task_id_counter,
                    scheduler_handle,
                ),
                self.compute_carryovers(
                    right_partitioned_outputs.clone(),
                    false,
                    task_id_counter,
                    scheduler_handle,
                ),
            )?,
        };

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
            let carryovers = (
                if i == 0 {
                    None
                } else {
                    backward_carryovers[i - 1].clone()
                },
                if i == num_partitions - 1 {
                    None
                } else {
                    forward_carryovers[i + 1].clone()
                },
            );
            self.create_and_submit_join_task(left_group, right_group, carryovers, result_tx)
                .await?;
        }

        Ok(())
    }

    /// Runs one top_n carryover pass over the right partitioned outputs.
    ///
    /// `is_strategy_backward=true` runs a backward pass: picks the per-partition max and fills gaps left-to-right.
    /// `is_strategy_backward=false` runs a forward pass: picks the per-partition min and fills gaps right-to-left.
    async fn compute_carryovers(
        &self,
        right_partitioned_outputs: Vec<MaterializedOutput>,
        is_strategy_backward: bool,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<Vec<Option<MaterializedOutput>>> {
        let descending = is_strategy_backward;
        let propagate_forward = is_strategy_backward;

        let partial_carryover_tasks = self.create_partial_carryover_tasks(
            right_partitioned_outputs,
            descending,
            task_id_counter,
            scheduler_handle,
        )?;

        let partial_carryovers: Vec<Vec<MaterializedOutput>> =
            try_join_all(partial_carryover_tasks.into_iter().map(try_join_all))
                .await?
                .into_iter()
                .map(|bucket| bucket.into_iter().flatten().collect())
                .collect();

        let final_carryover_tasks = self.create_final_carryover_tasks(
            partial_carryovers,
            descending,
            task_id_counter,
            scheduler_handle,
        )?;

        let mut final_carryovers: Vec<Option<MaterializedOutput>> =
            try_join_all(final_carryover_tasks.into_iter().map(|t| async {
                match t {
                    Some(task) => task.await.map(|mo| mo.filter(|m| m.num_rows() > 0)),
                    None => Ok(None),
                }
            }))
            .await?;

        let n = final_carryovers.len();
        if propagate_forward {
            for i in 1..n {
                if final_carryovers[i].is_none() {
                    let prev = final_carryovers[i - 1].clone();
                    final_carryovers[i] = prev;
                }
            }
        } else {
            for i in (0..n.saturating_sub(1)).rev() {
                if final_carryovers[i].is_none() {
                    let next = final_carryovers[i + 1].clone();
                    final_carryovers[i] = next;
                }
            }
        }

        Ok(final_carryovers)
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
                .create_and_submit_join_task(left_materialized, vec![], (None, None), &result_tx)
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

    fn left_composite_key(&self) -> Vec<BoundExpr> {
        self.left_by
            .iter()
            .chain(std::iter::once(&self.left_on))
            .cloned()
            .collect()
    }

    fn right_composite_key(&self) -> Vec<BoundExpr> {
        self.right_by
            .iter()
            .chain(std::iter::once(&self.right_on))
            .cloned()
            .collect()
    }

    /// Builds and submits a single top_n(limit=1) task over `inputs`,
    /// using the right-side schema and composite key derived from `self`.
    /// `descending=true` picks the max row (backward carryover); `false` picks the min (forward carryover).
    fn submit_top1_carryover_task(
        &self,
        inputs: Vec<MaterializedOutput>,
        descending: bool,
        phase: &str,
        strategy: Option<SchedulingStrategy>,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<SubmittedTask> {
        let composite_keys = self.right_composite_key();
        let n_keys = composite_keys.len();
        let node_id = self.node_id();

        let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
            inputs,
            self.right.config().schema.clone(),
            node_id,
            phase,
        );

        let plan = LocalPhysicalPlan::top_n(
            in_memory_scan,
            composite_keys,
            vec![descending; n_keys],
            vec![false; n_keys],
            1,
            None,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)).with_phase(phase),
        );

        SwordfishTaskBuilder::new(plan, self, node_id)
            // Forward (min) and backward (max) carryover tasks have identical top_n plans but
            // compute different results, so give them distinct fingerprints to avoid
            // sharing a worker pipeline and producing incorrect outputs.
            .extend_fingerprint(u32::from(descending))
            .with_psets(node_id, psets)
            .with_strategy(strategy)
            .build(self.context().query_idx, task_id_counter)
            .submit(scheduler_handle)
    }

    /// For each (partition, worker) pair, computes top_n(1) over that partition's data local to that worker.
    /// Tasks are pinned via worker affinity so no data movement occurs.
    /// `descending=true` picks the max (backward carryover); `false` picks the min (forward carryover).
    ///
    /// Returns `Vec<Vec<SubmittedTask>>` where outer index = partition_idx, inner = one task per worker.
    fn create_partial_carryover_tasks(
        &self,
        materialized_outputs: Vec<MaterializedOutput>,
        descending: bool,
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

        let mut tasks_by_bucket = Vec::with_capacity(self.num_partitions);
        for bucket_idx in 0..self.num_partitions {
            let mut bucket_tasks = vec![];
            for (worker_id, split_mos) in &worker_groups {
                let mos_for_bucket: Vec<MaterializedOutput> = split_mos
                    .iter()
                    .map(|splits| splits[bucket_idx].clone())
                    .collect();

                let phase = if descending {
                    PARTIAL_CARRYOVER_BACKWARD_PHASE
                } else {
                    PARTIAL_CARRYOVER_FORWARD_PHASE
                };
                let submitted = self.submit_top1_carryover_task(
                    mos_for_bucket,
                    descending,
                    phase,
                    Some(SchedulingStrategy::WorkerAffinity {
                        worker_id: worker_id.clone(),
                        soft: false,
                    }),
                    task_id_counter,
                    scheduler_handle,
                )?;

                bucket_tasks.push(submitted);
            }
            tasks_by_bucket.push(bucket_tasks);
        }

        Ok(tasks_by_bucket)
    }

    /// For each partition, reduces the per-worker partial top_n(1) results into a single global top_n(1).
    /// `descending=true` picks the max (backward carryover); `false` picks the min (forward carryover).
    ///
    /// Returns `Vec<Option<SubmittedTask>>` where `None` means the partition had no data.
    fn create_final_carryover_tasks(
        &self,
        partial_carryovers: Vec<Vec<MaterializedOutput>>,
        descending: bool,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<Vec<Option<SubmittedTask>>> {
        partial_carryovers
            .into_iter()
            .map(|per_worker_mos| {
                let non_empty: Vec<MaterializedOutput> = per_worker_mos
                    .into_iter()
                    .filter(|mo| mo.num_rows() > 0)
                    .collect();

                if non_empty.is_empty() {
                    return Ok(None);
                }

                let phase = if descending {
                    FINAL_CARRYOVER_BACKWARD_PHASE
                } else {
                    FINAL_CARRYOVER_FORWARD_PHASE
                };
                self.submit_top1_carryover_task(
                    non_empty,
                    descending,
                    phase,
                    None,
                    task_id_counter,
                    scheduler_handle,
                )
                .map(Some)
            })
            .collect()
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
        res.push(format!(
            "Needs range repartition: {}",
            self.needs_range_repartition
        ));
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
