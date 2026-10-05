use std::{collections::HashMap, future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::{
    AggExpr, bound_col,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    lit,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{AsofJoinStrategy, stats::StatsState};
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
};
use futures::{TryStreamExt, future::try_join_all};

use super::stats::BasicJoinStats;
use crate::{
    pipeline_node::{
        ClusteringStrategy, DistributedPipelineNode, MaterializedOutput, NodeID,
        PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
        clustering::BoundClusteringSpec, sort::range_repartition_two_sides,
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
    /// Inputs are asserted to be co-partitioned: same partition count, identical range
    /// boundaries, rows sorted ascending by the on-key. Skips the sample + range shuffle and
    /// zips input partitions by index instead.
    assume_aligned: bool,

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
        assume_aligned: bool,
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
            ClusteringStrategy::Explicit(BoundClusteringSpec::unknown(num_partitions)),
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
            assume_aligned,
            left,
            right,
        }
    }

    async fn create_and_submit_join_task(
        self: &Arc<Self>,
        left_partition_group: Vec<MaterializedOutput>,
        right_partition_group: Vec<MaterializedOutput>,
        carryovers: Vec<MaterializedOutput>,
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

        for carryover in carryovers {
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
                    vec![],
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

        self.join_partition_groups(
            left_partition_groups,
            right_partition_groups,
            task_id_counter,
            result_tx,
            scheduler_handle,
        )
        .await
    }

    /// Computes carryovers across the per-partition groups and dispatches one local asof join
    /// task per partition. Partition `i` joins against right group `i` plus the carryovers from
    /// its neighbors. Shared by the shuffle path and the aligned (pre-partitioned) path.
    async fn join_partition_groups(
        self: &Arc<Self>,
        left_partition_groups: Vec<Vec<MaterializedOutput>>,
        right_partition_groups: Vec<Vec<MaterializedOutput>>,
        task_id_counter: &TaskIDCounter,
        result_tx: &Sender<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let num_partitions = left_partition_groups.len();
        if num_partitions == 1 {
            let left_group = left_partition_groups.into_iter().next().unwrap();
            let right_group = right_partition_groups.into_iter().next().unwrap();
            return self
                .create_and_submit_join_task(left_group, right_group, vec![], result_tx)
                .await;
        }

        // backward_carryovers[i] = max of bucket i (forward-propagated): used as carryover for bucket i+1.
        // forward_carryovers[i]  = min of bucket i (backward-propagated): used as carryover for bucket i-1.
        let (backward_carryovers, forward_carryovers) = match self.strategy {
            AsofJoinStrategy::Backward => {
                let backward_carryovers = self
                    .compute_carryovers(
                        &right_partition_groups,
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
                        &right_partition_groups,
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
                    &right_partition_groups,
                    true,
                    task_id_counter,
                    scheduler_handle,
                ),
                self.compute_carryovers(
                    &right_partition_groups,
                    false,
                    task_id_counter,
                    scheduler_handle,
                ),
            )?,
        };

        for (i, (left_group, right_group)) in left_partition_groups
            .into_iter()
            .zip(right_partition_groups)
            .enumerate()
        {
            // A partition's match can live arbitrarily many partitions away (sparse or
            // group-sparse neighbors), so offer the extremes of ALL preceding partitions
            // (backward) and ALL following partitions (forward). Extremes are at most one
            // row per group per partition; the local join picks the correct match.
            let carryovers = backward_carryovers[..i]
                .iter()
                .chain(forward_carryovers[i + 1..].iter())
                .flatten()
                .cloned()
                .collect();
            self.create_and_submit_join_task(left_group, right_group, carryovers, result_tx)
                .await?;
        }

        Ok(())
    }

    /// Reduces each right partition group to its carryover extreme rows.
    ///
    /// `is_strategy_backward=true` picks the per-partition (per-group) max;
    /// `is_strategy_backward=false` picks the min. Entries are `None` for empty partitions.
    /// Callers combine all extremes on the relevant side of each partition, so no
    /// cross-partition propagation is needed here.
    async fn compute_carryovers(
        &self,
        right_partition_groups: &[Vec<MaterializedOutput>],
        is_strategy_backward: bool,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<Vec<Option<MaterializedOutput>>> {
        let descending = is_strategy_backward;

        let partial_carryover_tasks = self.create_partial_carryover_tasks(
            right_partition_groups,
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

        let final_carryovers: Vec<Option<MaterializedOutput>> =
            try_join_all(final_carryover_tasks.into_iter().map(|t| async {
                match t {
                    Some(task) => task.await.map(|mo| mo.filter(|m| m.num_rows() > 0)),
                    None => Ok(None),
                }
            }))
            .await?;

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
        // The aligned path keeps empty partitions: dropping them would break index alignment.
        let keep_empty = self.assume_aligned;
        let left_materialized = left_inputs
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_filter(|output| future::ready(keep_empty || output.num_rows() > 0))
            .try_collect::<Vec<_>>()
            .await?;

        let right_materialized = right_inputs
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_filter(|output| future::ready(keep_empty || output.num_rows() > 0))
            .try_collect::<Vec<_>>()
            .await?;

        if self.assume_aligned {
            if left_materialized.len() != right_materialized.len() {
                return Err(common_error::DaftError::InternalError(format!(
                    "AsofJoin (aligned): partition count mismatch at execution time:                      left={}, right={}. The _assume_sorted_and_aligned guarantee was violated.",
                    left_materialized.len(),
                    right_materialized.len(),
                )));
            }
            if left_materialized.is_empty() {
                return Ok(());
            }
            let left_groups = left_materialized.into_iter().map(|mo| vec![mo]).collect();
            let right_groups = right_materialized.into_iter().map(|mo| vec![mo]).collect();
            return self
                .join_partition_groups(
                    left_groups,
                    right_groups,
                    &task_id_counter,
                    &result_tx,
                    &scheduler_handle,
                )
                .await;
        }

        if left_materialized.is_empty() {
            return Ok(());
        }

        if right_materialized.is_empty() {
            return self
                .create_and_submit_join_task(left_materialized, vec![], vec![], &result_tx)
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

    /// Builds and submits a task reducing `inputs` to the carryover rows: every right row whose
    /// on-key equals the partition extreme (per by-key group, or over the whole frame when there are
    /// no by-keys). `descending=true` keeps the max on-key (backward carryover), `false` the min
    /// (forward). Keeping *all* rows tied at the extreme rather than one means the local operator
    /// stays the single place tie-breaks happen; the carryover only promises not to drop a candidate.
    fn submit_carryover_task(
        &self,
        inputs: Vec<MaterializedOutput>,
        descending: bool,
        phase: &str,
        strategy: Option<SchedulingStrategy>,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<SubmittedTask> {
        let node_id = self.node_id();
        let right_schema = self.right.config().schema.clone();
        let context = LocalNodeContext::new(Some(node_id as usize)).with_phase(phase);

        let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
            inputs,
            right_schema.clone(),
            node_id,
            phase,
        );

        const EXTREME_COL: &str = "__asof_carryover_extreme";
        const PART_COL: &str = "__asof_carryover_part";
        let on_expr = self.right_on.inner().clone();
        let on_field = on_expr.to_field(&right_schema)?;
        let agg = if descending {
            AggExpr::Max(on_expr.clone())
        } else {
            AggExpr::Min(on_expr.clone())
        };

        // Partition by the by-keys, or by an injected constant column when there are none (the whole
        // frame is one partition; window_partition_only requires a non-empty partition_by).
        let (windowed_input, partition_by, base_schema) = if self.right_by.is_empty() {
            let part_field = Field::new(PART_COL, DataType::Boolean);
            let base_schema = Arc::new(Schema::new(
                right_schema
                    .fields()
                    .iter()
                    .cloned()
                    .chain(std::iter::once(part_field.clone())),
            ));
            let mut projection: Vec<BoundExpr> = right_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| BoundExpr::new_unchecked(bound_col(i, f.clone())))
                .collect();
            projection.push(BoundExpr::new_unchecked(lit(true).alias(PART_COL)));
            let scan = LocalPhysicalPlan::project(
                in_memory_scan,
                projection,
                base_schema.clone(),
                StatsState::NotMaterialized,
                context.clone(),
            );
            let part_col = BoundExpr::new_unchecked(bound_col(right_schema.len(), part_field));
            (scan, vec![part_col], base_schema)
        } else {
            (in_memory_scan, self.right_by.clone(), right_schema.clone())
        };

        let extreme_field = Field::new(EXTREME_COL, on_field.dtype);
        let window_schema = Arc::new(Schema::new(
            base_schema
                .fields()
                .iter()
                .cloned()
                .chain(std::iter::once(extreme_field.clone())),
        ));
        let window = LocalPhysicalPlan::window_partition_only(
            windowed_input,
            partition_by,
            window_schema,
            StatsState::NotMaterialized,
            vec![BoundAggExpr::new_unchecked(agg)],
            vec![EXTREME_COL.to_string()],
            context.clone(),
        );

        // Keep every row whose on-key equals the partition extreme. Null on-keys compare to null and
        // drop out, which is correct: they can never be an asof match.
        let predicate =
            BoundExpr::new_unchecked(on_expr.eq(bound_col(base_schema.len(), extreme_field)));
        let filtered = LocalPhysicalPlan::filter(
            window,
            predicate,
            StatsState::NotMaterialized,
            context.clone(),
        );

        // Project back to the right schema, dropping the extreme and any injected partition column.
        let projection = right_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| BoundExpr::new_unchecked(bound_col(i, f.clone())))
            .collect();
        let plan = LocalPhysicalPlan::project(
            filtered,
            projection,
            right_schema.clone(),
            StatsState::NotMaterialized,
            context,
        );

        SwordfishTaskBuilder::new(plan, self, node_id)
            // Backward (max) and forward (min) carryover tasks differ only in the window agg, so
            // give them distinct fingerprints to avoid sharing a worker pipeline.
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
        right_partition_groups: &[Vec<MaterializedOutput>],
        descending: bool,
        task_id_counter: &TaskIDCounter,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<Vec<Vec<SubmittedTask>>> {
        let phase = if descending {
            PARTIAL_CARRYOVER_BACKWARD_PHASE
        } else {
            PARTIAL_CARRYOVER_FORWARD_PHASE
        };

        right_partition_groups
            .iter()
            .map(|bucket| {
                let mut worker_groups: HashMap<WorkerId, Vec<MaterializedOutput>> = HashMap::new();
                for mo in bucket {
                    worker_groups
                        .entry(mo.worker_id().clone())
                        .or_default()
                        .push(mo.clone());
                }

                worker_groups
                    .into_iter()
                    .map(|(worker_id, mos_for_bucket)| {
                        self.submit_carryover_task(
                            mos_for_bucket,
                            descending,
                            phase,
                            Some(SchedulingStrategy::WorkerAffinity {
                                worker_id,
                                soft: false,
                            }),
                            task_id_counter,
                            scheduler_handle,
                        )
                    })
                    .collect::<DaftResult<Vec<_>>>()
            })
            .collect()
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
                self.submit_carryover_task(
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
        let mut res = vec![if self.assume_aligned {
            "AsofJoin (assume_sorted_and_aligned)".to_string()
        } else {
            "AsofJoin".to_string()
        }];
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
