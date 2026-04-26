use std::{collections::HashMap, future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_recordbatch::{RecordBatch, record_batch_max_composite};
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

const SENTINEL_PHASE: &str = "sentinel";

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

        let sentinel_tasks = create_sentinel_tasks(
            right_partitioned_outputs.clone(),
            self.right.config().schema.clone(),
            right_composite_key.clone(),
            num_partitions,
            self.as_ref(),
            task_id_counter,
            scheduler_handle,
        )?;

        let sentinel_groups: Vec<Vec<MaterializedOutput>> =
            try_join_all(sentinel_tasks.into_iter().map(try_join_all))
                .await?
                .into_iter()
                .map(|bucket| bucket.into_iter().flatten().collect())
                .collect();

        let sentinels = reduce_sentinels(sentinel_groups, &right_composite_key).await?;

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
            let sentinel = if i == 0 {
                None
            } else {
                sentinels[i - 1].clone()
            };
            self.create_and_submit_join_task(
                i as u32,
                left_group,
                right_group,
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

/// Creates top_n(limit=1, descending=true) tasks per (bucket, worker) pair to compute
/// sentinel (max) rows from the right-side range-repartitioned outputs.
///
/// Returns `Vec<Vec<SubmittedTask>>` where outer index = bucket_idx, inner = per-worker tasks.
fn create_sentinel_tasks(
    materialized_outputs: Vec<MaterializedOutput>,
    input_schema: SchemaRef,
    composite_keys: Vec<BoundExpr>,
    num_partitions: usize,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<Vec<Vec<SubmittedTask>>> {
    let context = pipeline_node.context();
    let node_id = pipeline_node.node_id();
    let descending = vec![true; composite_keys.len()];
    let nulls_first = vec![false; composite_keys.len()];

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

            let (in_memory_scan, psets) =
                MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                    mos_for_bucket,
                    input_schema.clone(),
                    node_id,
                    SENTINEL_PHASE,
                );

            let plan = LocalPhysicalPlan::top_n(
                in_memory_scan,
                composite_keys.clone(),
                descending.clone(),
                nulls_first.clone(),
                1,
                None,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)).with_phase(SENTINEL_PHASE),
            );

            let submitted = SwordfishTaskBuilder::new(plan, pipeline_node, node_id)
                .with_psets(node_id, psets)
                .with_strategy(Some(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker_id.clone(),
                    soft: false,
                }))
                .build(context.query_idx, task_id_counter)
                .submit(scheduler_handle)?;

            bucket_tasks.push(submitted);
        }
        tasks_by_bucket.push(bucket_tasks);
    }

    Ok(tasks_by_bucket)
}

/// Reduces sentinel MaterializedOutputs into one `Option<RecordBatch>` per bucket.
///
/// `sentinel_groups[bucket_idx]` contains one 1-row MaterializedOutput per worker
/// (the top_n result for that bucket from that worker).
///
/// This function:
/// 1. Flattens all refs across all buckets and fetches via one ray.get() call
/// 2. Reconstructs per-bucket groups and finds the cross-worker max for each bucket
/// 3. Forward-fills: if bucket j has no valid sentinel, inherit from bucket j-1
///
/// Returns `Vec<Option<RecordBatch>>` of length `sentinel_groups.len()`.
#[cfg(feature = "python")]
async fn reduce_sentinels(
    sentinel_groups: Vec<Vec<MaterializedOutput>>,
    composite_keys: &[BoundExpr],
) -> DaftResult<Vec<Option<RecordBatch>>> {
    use pyo3::prelude::*;

    let num_partitions = sentinel_groups.len();

    if sentinel_groups.iter().all(|g| g.is_empty()) {
        return Ok(vec![None; num_partitions]);
    }

    let bucket_sizes: Vec<usize> = sentinel_groups.iter().map(|g| g.len()).collect();

    let ray_partition_refs = sentinel_groups
        .into_iter()
        .flatten()
        .flat_map(|mo| mo.into_inner().0)
        .map(|pr| {
            use crate::python::ray::RayPartitionRef;

            let ray_partition_ref = pr.as_any().downcast_ref::<RayPartitionRef>().ok_or(
                common_error::DaftError::InternalError(
                    "Failed to downcast partition ref".to_string(),
                ),
            )?;
            Ok(ray_partition_ref.clone())
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let sentinel_mps = tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let ray = py.import(pyo3::intern!(py, "ray"))?;
            let py_object_refs = ray_partition_refs
                .into_iter()
                .map(|pr| pr.get_object_ref(py))
                .collect::<Vec<_>>();
            let results = ray.call_method1("get", (py_object_refs,))?;
            let mut mps = Vec::new();
            for item in results.try_iter()? {
                let item = item?;
                let inner = item.getattr("_micropartition").unwrap_or(item);
                let mp: daft_micropartition::python::PyMicroPartition =
                    inner.extract().map_err(pyo3::PyErr::from)?;
                mps.push(mp);
            }
            Ok::<_, pyo3::PyErr>(mps)
        })
    })
    .await
    .map_err(|e| {
        common_error::DaftError::InternalError(format!("Join error in spawn_blocking: {e}"))
    })??;

    // Each MP is a 1-row result from top_n. Empty buckets produce None via concat_or_get.
    let all_batches: Vec<Option<RecordBatch>> = sentinel_mps
        .into_iter()
        .map(|py_mp| py_mp.inner.concat_or_get().ok().flatten())
        .collect();

    let mut sentinels: Vec<Option<RecordBatch>> = Vec::with_capacity(num_partitions);
    let mut batch_iter = all_batches.into_iter();
    for bucket_size in &bucket_sizes {
        let candidates: Vec<RecordBatch> = (0..*bucket_size)
            .filter_map(|_| batch_iter.next().flatten())
            .filter(|b| !b.is_empty())
            .collect();
        if candidates.is_empty() {
            sentinels.push(None);
        } else if candidates.len() == 1 {
            sentinels.push(Some(candidates.into_iter().next().unwrap()));
        } else {
            let combined = RecordBatch::concat(&candidates.iter().collect::<Vec<_>>())?;
            sentinels.push(record_batch_max_composite(&combined, composite_keys)?);
        }
    }

    for j in 1..num_partitions {
        if sentinels[j].is_none() {
            let prev = sentinels[j - 1].clone();
            sentinels[j] = prev;
        }
    }

    Ok(sentinels)
}

#[cfg(not(feature = "python"))]
async fn reduce_sentinels(
    _sentinel_groups: Vec<Vec<MaterializedOutput>>,
    _composite_keys: &[BoundExpr],
) -> DaftResult<Vec<Option<RecordBatch>>> {
    unimplemented!("Distributed asof join sentinel reduction requires Python feature to be enabled")
}
