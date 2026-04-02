use std::{future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, SamplingMethod};
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
        PipelineNodeContext, TaskOutput,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef, SimpleCounters},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_vec,
    },
};

const SAMPLE_PHASE: &str = "sample";
const REPARTITION_PHASE: &str = "repartition";
const FINAL_SORT_PHASE: &str = "final-sort";

pub struct SortStats {
    base: BaseCounters,
    node_id: u32,
    sample_counters: SimpleCounters,
    repartition_counters: SimpleCounters,
    final_sort_counters: SimpleCounters,
}

impl SortStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext, node_id: u32) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
            node_id,
            sample_counters: SimpleCounters::new(),
            repartition_counters: SimpleCounters::new(),
            final_sort_counters: SimpleCounters::new(),
        }
    }

    fn phase_counters(&self, phase: &str) -> Option<&SimpleCounters> {
        match phase {
            SAMPLE_PHASE => Some(&self.sample_counters),
            REPARTITION_PHASE => Some(&self.repartition_counters),
            FINAL_SORT_PHASE => Some(&self.final_sort_counters),
            _ => None,
        }
    }
}

impl RuntimeStats for SortStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        match snapshot {
            StatSnapshot::Source(snapshot) => {
                // InMemorySource nodes only contribute duration
                self.base.add_duration_us(snapshot.cpu_us);
                if let Some(phase) = &node_info.node_phase {
                    if let Some(counters) = self.phase_counters(phase) {
                        counters.add_duration_us(snapshot.cpu_us);
                    }
                }
            }
            StatSnapshot::Default(snapshot) => {
                self.base.add_duration_us(snapshot.cpu_us);
                if let Some(phase) = &node_info.node_phase {
                    if let Some(counters) = self.phase_counters(phase) {
                        // Only track duration per-phase; row counts would be
                        // misleading because each phase has multiple local
                        // pipeline nodes (e.g., InMemoryScan → Sample → Project)
                        // whose rows_in/out would all be summed together.
                        counters.add_duration_us(snapshot.cpu_us);
                    }
                    if phase == FINAL_SORT_PHASE {
                        // Only the final sort phase contributes to the aggregate row counts
                        self.base.add_rows_in(snapshot.rows_in);
                        self.base.add_rows_out(snapshot.rows_out);
                    }
                }
            }
            _ => {}
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn export_phase_snapshots(&self) -> Vec<(usize, StatSnapshot)> {
        use crate::pipeline_node::phase_node_id;
        vec![
            (phase_node_id(self.node_id, 0), self.sample_counters.export_default_snapshot()),
            (phase_node_id(self.node_id, 1), self.repartition_counters.export_default_snapshot()),
            (phase_node_id(self.node_id, 2), self.final_sort_counters.export_default_snapshot()),
        ]
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
                common_error::DaftError::InternalError(
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

    let boundaries = common_runtime::python::execute_python_coroutine::<
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
        common_error::DaftError::InternalError(
            "No boundaries found for daft-distributed::sort::get_boundaries".to_string(),
        )
    })?;
    Ok(boundaries)
}

#[cfg(not(feature = "python"))]
pub(crate) async fn get_partition_boundaries_from_samples(
    _samples: Vec<MaterializedOutput>,
    _partition_by: &[BoundExpr],
    _descending: Vec<bool>,
    _nulls_first: Vec<bool>,
    _num_partitions: usize,
) -> DaftResult<RecordBatch> {
    unimplemented!("Distributed range partitioning requires Python feature to be enabled")
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
            let in_memory_source_plan = LocalPhysicalPlan::in_memory_scan(
                node_id,
                input_schema.clone(),
                mo.size_bytes(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)).with_phase(REPARTITION_PHASE),
            );
            let plan = LocalPhysicalPlan::repartition(
                in_memory_source_plan,
                RepartitionSpec::Range(RangeRepartitionConfig::new(
                    Some(num_partitions),
                    boundaries.clone(),
                    partition_by.clone(),
                    descending.clone(),
                )),
                num_partitions,
                input_schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)).with_phase(REPARTITION_PHASE),
            );
            let mut builder =
                SwordfishTaskBuilder::new(plan, pipeline_node, pipeline_node.node_id())
                    .with_psets(node_id, mo.into_inner().0);
            if let Some(salt) = fingerprint_salt {
                builder = builder.extend_fingerprint(salt);
            }
            let submittable_task = builder.build(context.query_idx, task_id_counter);
            let submitted_task = submittable_task.submit(scheduler_handle)?;
            Ok(submitted_task)
        })
        .collect::<DaftResult<Vec<_>>>()
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
            .map(TaskOutput::into_materialized)
            .collect::<DaftResult<Vec<_>>>()?;

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
            .map(TaskOutput::into_materialized)
            .collect::<DaftResult<Vec<_>>>()?;

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
        Arc::new(SortStats::new(meter, self.context(), self.node_id()))
    }

    fn phases(&self) -> &[&str] {
        &[SAMPLE_PHASE, REPARTITION_PHASE, FINAL_SORT_PHASE]
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
    use common_metrics::{Meter, ops::NodeCategory, snapshot::SourceSnapshot};

    use super::*;
    use crate::{
        pipeline_node::{PipelineNodeContext, phase_node_id},
        statistics::stats::test_utils::*,
    };

    fn make_sort_stats(node_id: u32) -> SortStats {
        let meter = Meter::test_scope("test.sort");
        let context = PipelineNodeContext::new(
            0,
            "test".into(),
            node_id,
            "Sort".into(),
            NodeType::Sort,
            NodeCategory::BlockingSink,
        );
        SortStats::new(&meter, &context, node_id)
    }

    #[test]
    fn test_sort_stats_aggregate_only_final_sort_rows() {
        let stats = make_sort_stats(5);

        // Sample phase: should contribute duration but NOT rows to aggregate
        let sample_info = make_node_info(5, Some(SAMPLE_PHASE));
        stats.handle_worker_node_stats(&sample_info, &make_default_snapshot(100, 1000, 50));

        // Repartition phase: should contribute duration but NOT rows to aggregate
        let repart_info = make_node_info(5, Some(REPARTITION_PHASE));
        stats.handle_worker_node_stats(&repart_info, &make_default_snapshot(200, 500, 500));

        // Final sort phase: should contribute both duration AND rows to aggregate
        let final_info = make_node_info(5, Some(FINAL_SORT_PHASE));
        stats.handle_worker_node_stats(&final_info, &make_default_snapshot(300, 500, 500));

        let (cpu, rows_in, rows_out) = extract_default(&stats.export_snapshot());
        assert_eq!(cpu, 600); // all phases contribute duration
        assert_eq!(rows_in, 500); // only final-sort rows
        assert_eq!(rows_out, 500); // only final-sort rows
    }

    #[test]
    fn test_sort_stats_per_phase_duration_only() {
        let stats = make_sort_stats(5);

        let sample_info = make_node_info(5, Some(SAMPLE_PHASE));
        stats.handle_worker_node_stats(&sample_info, &make_default_snapshot(100, 1000, 50));

        let repart_info = make_node_info(5, Some(REPARTITION_PHASE));
        stats.handle_worker_node_stats(&repart_info, &make_default_snapshot(200, 500, 500));

        let final_info = make_node_info(5, Some(FINAL_SORT_PHASE));
        stats.handle_worker_node_stats(&final_info, &make_default_snapshot(300, 500, 500));

        let phase_snapshots = stats.export_phase_snapshots();
        assert_eq!(phase_snapshots.len(), 3);

        // Per-phase snapshots only track duration (not rows, since a phase
        // has multiple local pipeline nodes whose rows would be double-counted)
        let (id, snap) = &phase_snapshots[0];
        assert_eq!(*id, phase_node_id(5, 0));
        let (cpu, rows_in, rows_out) = extract_default(snap);
        assert_eq!(cpu, 100);
        assert_eq!(rows_in, 0);
        assert_eq!(rows_out, 0);

        let (id, snap) = &phase_snapshots[1];
        assert_eq!(*id, phase_node_id(5, 1));
        let (cpu, _, _) = extract_default(snap);
        assert_eq!(cpu, 200);

        let (id, snap) = &phase_snapshots[2];
        assert_eq!(*id, phase_node_id(5, 2));
        let (cpu, _, _) = extract_default(snap);
        assert_eq!(cpu, 300);
    }

    #[test]
    fn test_sort_stats_multiple_tasks_per_phase_accumulate() {
        let stats = make_sort_stats(3);

        // Two tasks in the sample phase
        let info = make_node_info(3, Some(SAMPLE_PHASE));
        stats.handle_worker_node_stats(&info, &make_default_snapshot(50, 100, 10));
        stats.handle_worker_node_stats(&info, &make_default_snapshot(60, 200, 20));

        // One task in final sort
        let info = make_node_info(3, Some(FINAL_SORT_PHASE));
        stats.handle_worker_node_stats(&info, &make_default_snapshot(300, 500, 500));

        let phase_snapshots = stats.export_phase_snapshots();
        // sample duration should accumulate
        let (_, snap) = &phase_snapshots[0];
        let (cpu, _, _) = extract_default(snap);
        assert_eq!(cpu, 110); // 50 + 60
    }

    #[test]
    fn test_sort_stats_source_snapshot_only_contributes_duration() {
        let stats = make_sort_stats(5);

        // InMemorySource nodes produce Source snapshots
        let info = make_node_info(5, Some(SAMPLE_PHASE));
        let source_snap = StatSnapshot::Source(SourceSnapshot {
            cpu_us: 150,
            rows_out: 100,
            bytes_read: 0,
        });
        stats.handle_worker_node_stats(&info, &source_snap);

        // Aggregate: source contributes duration but not rows (no final-sort)
        let (cpu, rows_in, rows_out) = extract_default(&stats.export_snapshot());
        assert_eq!(cpu, 150);
        assert_eq!(rows_in, 0);
        assert_eq!(rows_out, 0);

        // Phase: source contributes duration to its phase counter
        let phase_snapshots = stats.export_phase_snapshots();
        let (_, snap) = &phase_snapshots[0]; // sample phase
        let (cpu, _, _) = extract_default(snap);
        assert_eq!(cpu, 150);
    }

    #[test]
    fn test_sort_stats_empty_phases() {
        let stats = make_sort_stats(5);

        // No events -> all zeros
        let phase_snapshots = stats.export_phase_snapshots();
        assert_eq!(phase_snapshots.len(), 3);
        for (_, snap) in &phase_snapshots {
            let (cpu, rows_in, rows_out) = extract_default(snap);
            assert_eq!(cpu, 0);
            assert_eq!(rows_in, 0);
            assert_eq!(rows_out, 0);
        }
    }

    #[test]
    fn test_phase_node_id_scheme() {
        assert_eq!(phase_node_id(0, 0), 0);
        assert_eq!(phase_node_id(0, 1), 1);
        assert_eq!(phase_node_id(5, 0), 5_000_000);
        assert_eq!(phase_node_id(5, 1), 5_000_001);
        assert_eq!(phase_node_id(5, 2), 5_000_002);
        // Ensure no collision with reasonable node IDs
        assert_eq!(phase_node_id(100, 0), 100_000_000);
    }
}
