use std::{future, sync::Arc};

use daft_common::error::DaftResult;
use daft_common::metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, SamplingMethod, ShuffleBackend};
use daft_logical_plan::{
    partitioning::{RangeRepartitionConfig, RepartitionSpec},
    stats::StatsState,
};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::{Schema, SchemaRef};
use futures::{TryStreamExt, future::try_join_all};

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_vec,
    },
};

const SAMPLE_PHASE: &str = "sample";
const REPARTITION_PHASE: &str = "repartition";
const FINAL_SORT_PHASE: &str = "final-sort";

struct SortStats {
    base: BaseCounters,
}

impl SortStats {
    fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for SortStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        // All phases contribute duration
        self.base.add_duration_us(snapshot.duration_us());

        // For row counts, ignore snapshots from the sample and repartition phases
        // (if they occurred - skipped for the single-partition case).
        // The final sort phase produces two snapshots:
        // * a Source snapshot from the in_memory_scan, which we also ignore to avoid double-counting rows_out
        // * a Default snapshot from the sort, which we report as the rows_in and rows_out for the distributed operator
        if let StatSnapshot::Default(snapshot) = snapshot
            && let Some(phase) = &node_info.node_phase
            && phase == FINAL_SORT_PHASE
        {
            self.base.add_rows_in(snapshot.rows_in);
            self.base.add_rows_out(snapshot.rows_out);
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn increment_num_tasks(&self) {
        self.base.increment_num_tasks();
    }
}

/// Computes partition boundaries from sampled data for range partitioning.
/// Takes already-sampled materialized outputs and computes boundaries that divide the data
/// into approximately equal-sized partitions.
#[cfg(feature = "python")]
pub(crate) async fn get_partition_boundaries_from_samples(
    samples: Vec<MaterializedOutput>,
    partition_by: &[BoundExpr],
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    num_partitions: usize,
) -> DaftResult<RecordBatch> {
    use pyo3::prelude::*;

    // Extract partition refs from samples
    let ray_partition_refs = samples
        .into_iter()
        .flat_map(|mo| mo.into_inner().0)
        .map(|pr| {
            use crate::python::ray::RayPartitionRef;

            let ray_partition_ref = pr.as_any().downcast_ref::<RayPartitionRef>().ok_or(
                daft_common::error::DaftError::InternalError(
                    "Failed to downcast partition ref".to_string(),
                ),
            )?;
            Ok(ray_partition_ref.clone())
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let py_sort_by = partition_by
        .iter()
        .map(|e| daft_dsl::python::PyExpr {
            expr: e.inner().clone(),
        })
        .collect::<Vec<_>>();

    let boundaries = daft_common::runtime::python::execute_python_coroutine::<
        _,
        daft_micropartition::python::PyMicroPartition,
    >(move |py| {
        let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
        let py_object_refs = ray_partition_refs
            .into_iter()
            .map(|pr| pr.get_object_ref(py))
            .collect::<Vec<_>>();
        flotilla_module.call_method1(
            pyo3::intern!(py, "get_boundaries"),
            (
                py_object_refs,
                py_sort_by,
                descending,
                nulls_first,
                num_partitions,
            ),
        )
    })
    .await?;

    let boundaries = boundaries.inner.concat_or_get()?.ok_or_else(|| {
        daft_common::error::DaftError::InternalError(
            "No boundaries found for daft-distributed::sort::get_boundaries".to_string(),
        )
    })?;
    Ok(boundaries)
}

// Non-Python builds are only used in Rust-only test runs (`cargo test`).
// Production always enables the python feature.
#[cfg(not(feature = "python"))]
pub(crate) async fn get_partition_boundaries_from_samples(
    samples: Vec<MaterializedOutput>,
    partition_by: &[BoundExpr],
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    num_partitions: usize,
) -> DaftResult<RecordBatch> {
    use daft_micropartition::MicroPartition;

    // Extract partition refs and downcast to MicroPartition.
    let micro_partitions: Vec<MicroPartition> = samples
        .into_iter()
        .flat_map(|mo| mo.into_inner().0)
        .map(|pr| {
            pr.as_any()
                .downcast_ref::<MicroPartition>()
                .ok_or_else(|| {
                    daft_common::error::DaftError::InternalError(
                        "Expected MicroPartition in local mode".to_string(),
                    )
                })
                .cloned()
        })
        .collect::<DaftResult<Vec<_>>>()?;

    // Concat all samples, sort by the partition columns, then compute quantiles.
    let merged = MicroPartition::concat(micro_partitions)?;
    let sorted = merged.sort(partition_by, &descending, &nulls_first)?;
    let boundaries = sorted.quantiles(num_partitions)?;

    boundaries.concat_or_get()?.ok_or_else(|| {
        daft_common::error::DaftError::InternalError(
            "No boundaries found for range partitioning".to_string(),
        )
    })
}

/// Creates sample tasks from materialized outputs, projecting the specified columns.
/// This is used to sample data before computing partition boundaries for range partitioning.
pub(crate) fn create_sample_tasks(
    materialized_outputs: Vec<MaterializedOutput>,
    input_schema: SchemaRef,
    sample_by: Vec<BoundExpr>,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
    fingerprint_salt: Option<u32>,
) -> DaftResult<Vec<SubmittedTask>> {
    let sample_size = pipeline_node.config().execution_config.sample_size_for_sort;
    let context = pipeline_node.context();
    let sample_schema = Arc::new(Schema::new(sample_by.iter().map(|e| {
        e.inner()
            .to_field(&input_schema)
            .expect("Sample by column not found in input schema")
    })));

    materialized_outputs
        .into_iter()
        .map(|mo| {
            let (in_memory_scan, psets) =
                MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                    vec![mo],
                    input_schema.clone(),
                    pipeline_node.node_id(),
                    SAMPLE_PHASE,
                );
            let sample = LocalPhysicalPlan::sample(
                in_memory_scan,
                SamplingMethod::Size(sample_size),
                true,
                None,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(pipeline_node.node_id() as usize))
                    .with_phase(SAMPLE_PHASE),
            );
            let plan = LocalPhysicalPlan::project(
                sample,
                sample_by.clone(),
                sample_schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(pipeline_node.node_id() as usize))
                    .with_phase(SAMPLE_PHASE),
            );
            let mut builder =
                SwordfishTaskBuilder::new(plan, pipeline_node, pipeline_node.node_id())
                    .with_psets(pipeline_node.node_id(), psets);
            if let Some(salt) = fingerprint_salt {
                builder = builder.extend_fingerprint(salt);
            }
            let submittable_task = builder.build(context.query_idx, task_id_counter);
            let submitted_task = submittable_task.submit(scheduler_handle)?;
            Ok(submitted_task)
        })
        .collect::<DaftResult<Vec<_>>>()
}

/// Creates range repartition tasks from materialized outputs using the provided boundaries.
/// This is used to repartition data after computing partition boundaries from samples.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_range_repartition_tasks(
    materialized_outputs: Vec<MaterializedOutput>,
    input_schema: SchemaRef,
    partition_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    boundaries: RecordBatch,
    num_partitions: usize,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
    fingerprint_salt: Option<u32>,
) -> DaftResult<Vec<SubmittedTask>> {
    let context = pipeline_node.context();
    let node_id = pipeline_node.node_id();
    materialized_outputs
        .into_iter()
        .map(|mo| {
            let (in_memory_source_plan, psets) =
                MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                    vec![mo],
                    input_schema.clone(),
                    node_id,
                    REPARTITION_PHASE,
                );
            let plan = LocalPhysicalPlan::repartition_write(
                in_memory_source_plan,
                num_partitions,
                input_schema.clone(),
                ShuffleBackend::Ray,
                RepartitionSpec::Range(RangeRepartitionConfig::new(
                    Some(num_partitions),
                    boundaries.clone(),
                    partition_by.clone(),
                    descending.clone(),
                )),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)).with_phase(REPARTITION_PHASE),
            );
            let mut builder =
                SwordfishTaskBuilder::new(plan, pipeline_node, pipeline_node.node_id())
                    .with_psets(node_id, psets);
            if let Some(salt) = fingerprint_salt {
                builder = builder.extend_fingerprint(salt);
            }
            let submittable_task = builder.build(context.query_idx, task_id_counter);
            let submitted_task = submittable_task.submit(scheduler_handle)?;
            Ok(submitted_task)
        })
        .collect::<DaftResult<Vec<_>>>()
}

/// Samples both sides, computes range partition boundaries from the combined samples,
/// repartitions both sides in parallel, and returns the flattened outputs.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn range_repartition_two_sides(
    left_materialized: Vec<MaterializedOutput>,
    right_materialized: Vec<MaterializedOutput>,
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    num_partitions: usize,
    pipeline_node: &dyn PipelineNodeImpl,
    task_id_counter: &TaskIDCounter,
    scheduler_handle: &SchedulerHandle<SwordfishTask>,
) -> DaftResult<(Vec<MaterializedOutput>, Vec<MaterializedOutput>)> {
    let descending = vec![false; left_on.len()];
    let nulls_first = vec![false; left_on.len()];

    let left_boundary_key_names = left_on
        .iter()
        .map(|expr| expr.inner().to_field(&left_schema).map(|f| f.name))
        .collect::<DaftResult<Vec<_>>>()?;

    let right_sample_by_aliased = right_on
        .iter()
        .zip(left_boundary_key_names.into_iter())
        .map(|(expr, key_name)| BoundExpr::new_unchecked(expr.inner().alias(key_name)))
        .collect::<Vec<_>>();

    let left_sample_tasks = create_sample_tasks(
        left_materialized.clone(),
        left_schema.clone(),
        left_on.clone(),
        pipeline_node,
        task_id_counter,
        scheduler_handle,
        Some(0),
    )?;

    let right_sample_tasks = create_sample_tasks(
        right_materialized.clone(),
        right_schema.clone(),
        right_sample_by_aliased,
        pipeline_node,
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

    let left_boundaries = get_partition_boundaries_from_samples(
        combined_sampled_outputs,
        &left_on,
        descending.clone(),
        nulls_first,
        num_partitions,
    )
    .await?;

    let left_partition_tasks = create_range_repartition_tasks(
        left_materialized,
        left_schema,
        left_on,
        descending.clone(),
        left_boundaries.clone(),
        num_partitions,
        pipeline_node,
        task_id_counter,
        scheduler_handle,
        Some(0),
    )?;

    let right_boundary_names = right_on
        .iter()
        .map(|expr| expr.inner().to_field(&right_schema).map(|f| f.name))
        .collect::<DaftResult<Vec<_>>>()?;

    let right_boundaries = RecordBatch::from_nonempty_columns(
        left_boundaries
            .columns()
            .iter()
            .zip(right_boundary_names)
            .map(|(col, name)| col.as_materialized_series().rename(&name))
            .collect::<Vec<_>>(),
    )?;

    let right_partition_tasks = create_range_repartition_tasks(
        right_materialized,
        right_schema,
        right_on,
        descending,
        right_boundaries,
        num_partitions,
        pipeline_node,
        task_id_counter,
        scheduler_handle,
        Some(1),
    )?;

    let (left_partitioned, right_partitioned) = futures::try_join!(
        try_join_all(left_partition_tasks),
        try_join_all(right_partition_tasks)
    )?;

    Ok((
        left_partitioned.into_iter().flatten().collect(),
        right_partitioned.into_iter().flatten().collect(),
    ))
}

pub(crate) struct SortNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    // Sort properties
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    child: DistributedPipelineNode,
}

impl SortNode {
    const NODE_NAME: &'static str = "Sort";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        output_schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Sort,
            NodeCategory::BlockingSink,
        );

        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            sort_by,
            descending,
            nulls_first,
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_outputs = input_node
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_filter(|mo| future::ready(mo.num_rows() > 0))
            .try_collect::<Vec<_>>()
            .await?;

        if materialized_outputs.is_empty() {
            return Ok(());
        }

        if materialized_outputs.len() == 1 {
            // skip straight to the final sort phase
            let (in_memory_scan, psets) =
                MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                    materialized_outputs,
                    self.config.schema.clone(),
                    self.node_id(),
                    FINAL_SORT_PHASE,
                );
            let plan = LocalPhysicalPlan::sort(
                in_memory_scan,
                self.sort_by.clone(),
                self.descending.clone(),
                self.nulls_first.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)).with_phase(FINAL_SORT_PHASE),
            );
            let task = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
                .with_psets(self.node_id(), psets);
            let _ = result_tx.send(task).await;
            return Ok(());
        }

        let num_partitions = self.child.config().clustering_spec.num_partitions();

        // Sample the data
        let sample_tasks = create_sample_tasks(
            materialized_outputs.clone(),
            self.config.schema.clone(),
            self.sort_by.clone(),
            self.as_ref(),
            &task_id_counter,
            &scheduler_handle,
            None,
        )?;

        let sampled_outputs = try_join_all(sample_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Compute partition boundaries from samples
        let boundaries = get_partition_boundaries_from_samples(
            sampled_outputs,
            &self.sort_by,
            self.descending.clone(),
            self.nulls_first.clone(),
            num_partitions,
        )
        .await?;

        let partition_tasks = create_range_repartition_tasks(
            materialized_outputs,
            self.config.schema.clone(),
            self.sort_by.clone(),
            self.descending.clone(),
            boundaries,
            num_partitions,
            self.as_ref(),
            &task_id_counter,
            &scheduler_handle,
            None,
        )?;

        let partitioned_outputs = try_join_all(partition_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let transposed_outputs =
            transpose_materialized_outputs_from_vec(partitioned_outputs, num_partitions);

        for partition_group in transposed_outputs {
            let (in_memory_source_plan, psets) =
                MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
                    partition_group,
                    self.config.schema.clone(),
                    self.node_id(),
                    FINAL_SORT_PHASE,
                );
            let plan = LocalPhysicalPlan::sort(
                in_memory_source_plan,
                self.sort_by.clone(),
                self.descending.clone(),
                self.nulls_first.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)).with_phase(FINAL_SORT_PHASE),
            );
            let task = SwordfishTaskBuilder::new(plan, self.as_ref(), self.node_id())
                .with_psets(self.node_id(), psets);
            let _ = result_tx.send(task).await;
        }
        Ok(())
    }
}

impl PipelineNodeImpl for SortNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SortStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["Sort".to_string()];
        res.push(format!(
            "Sort by: {}",
            self.sort_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Descending = {}",
            self.descending.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Nulls first = {}",
            self.nulls_first.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);
        plan_context.spawn(self.execution_loop(
            input_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}

#[cfg(test)]
mod tests {
    use daft_common::error::DaftResult;
    use daft_common::metrics::{Meter, StatSnapshot};
    use daft_micropartition::MicroPartition;

    use super::*;
    use crate::pipeline_node::test_helpers::{
        bound_col_x, build_in_memory_source, make_partition, run_pipeline_and_get_stats,
        test_schema,
    };

    fn build_sort_pipeline(
        partitions: Vec<Arc<MicroPartition>>,
        meter: &Meter,
    ) -> DistributedPipelineNode {
        let (source, plan_config) = build_in_memory_source(0, partitions, meter);
        let sort_node = SortNode::new(
            1,
            &plan_config,
            vec![bound_col_x()],
            vec![false],
            vec![false],
            test_schema(),
            source,
        );
        DistributedPipelineNode::new(Arc::new(sort_node), meter)
    }

    /// Single-partition sort: exercises the FINAL_SORT_PHASE-only path.
    /// SortStats should count rows only from the Default snapshot with
    /// phase "final-sort", ignoring the Source snapshot from in_memory_scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_partition_stats() -> DaftResult<()> {
        let meter = Meter::test_scope("test_sort_stats");
        let pipeline = build_sort_pipeline(vec![make_partition(&[5, 3, 1, 4, 2])], &meter);

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 1)
            .expect("sort node stats");
        match snapshot {
            StatSnapshot::Default(s) => {
                assert_eq!(s.rows_in, 5);
                assert_eq!(s.rows_out, 5);
                assert!(s.cpu_us > 0);
            }
            other => panic!("expected Default snapshot for sort, got: {other:?}"),
        }

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 0)
            .expect("source node stats");
        assert!(matches!(snapshot, StatSnapshot::Source(_)));

        Ok(())
    }

    /// Multi-partition sort: exercises all three phases (sample, repartition,
    /// final-sort). SortStats should aggregate duration from all phases but
    /// count rows only from final-sort Default snapshots.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_partition_stats() -> DaftResult<()> {
        let meter = Meter::test_scope("test_sort_multi");
        let pipeline = build_sort_pipeline(
            vec![
                make_partition(&[5, 3, 1]),
                make_partition(&[4, 2, 6]),
                make_partition(&[9, 7, 8]),
            ],
            &meter,
        );

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 1)
            .expect("sort node stats");
        match snapshot {
            StatSnapshot::Default(s) => {
                assert_eq!(s.rows_in, 9);
                assert_eq!(s.rows_out, 9);
                assert!(s.cpu_us > 0);
            }
            other => panic!("expected Default snapshot for sort, got: {other:?}"),
        }

        Ok(())
    }
}
