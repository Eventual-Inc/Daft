use std::{future, sync::Arc};

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, RepartitionWriteBackend, SamplingMethod,
};
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
                RepartitionWriteBackend::Ray,
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

    /// Build the final-sort task for a single partition (skipping sample + repartition).
    fn build_final_sort_task(
        &self,
        materialized_outputs: Vec<MaterializedOutput>,
    ) -> SwordfishTaskBuilder {
        let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets_and_phase(
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
        SwordfishTaskBuilder::new(plan, self, self.node_id()).with_psets(self.node_id(), psets)
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
            let task = self.build_final_sort_task(materialized_outputs);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_daft_config::DaftExecutionConfig;
    use common_metrics::{
        Meter, StatSnapshot,
        ops::{NodeCategory, NodeInfo, NodeType},
        snapshot::{DefaultSnapshot, SourceSnapshot},
    };
    use daft_local_plan::{ExecutionStats, LocalPhysicalPlan};
    use daft_logical_plan::ClusteringSpec;
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;
    use crate::{
        pipeline_node::{PipelineNodeConfig, PipelineNodeContext, tests::MockNode},
        plan::PlanConfig,
        scheduling::tests::create_mock_partition_ref,
        statistics::{TaskEvent, stats::RuntimeNodeManager},
    };

    const SORT_NODE_ID: NodeID = 42;
    const SORT_ORIGIN_ID: common_metrics::NodeID = SORT_NODE_ID as common_metrics::NodeID;

    // ---------------------------------------------------------------
    // Helpers to construct a real SortNode for testing
    // ---------------------------------------------------------------

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]))
    }

    fn test_sort_by(schema: &Schema) -> Vec<BoundExpr> {
        vec![BoundExpr::try_new(daft_dsl::resolved_col("x"), schema).unwrap()]
    }

    /// Create a real SortNode backed by a mock child node.
    fn make_sort_node() -> SortNode {
        let schema = test_schema();
        let sort_by = test_sort_by(&schema);
        let plan_config = PlanConfig::new(
            0,
            Arc::from("test-query"),
            Arc::new(DaftExecutionConfig::default()),
        );
        let child_op = Arc::new(MockNode::new(0));
        let meter = Meter::test_scope("sort_child");
        let child = DistributedPipelineNode::new(child_op, &meter);

        SortNode::new(
            SORT_NODE_ID,
            &plan_config,
            sort_by,
            vec![false],
            vec![false],
            schema,
            child,
        )
    }

    /// Create a single MaterializedOutput with the given row count.
    fn mock_materialized(num_rows: usize) -> MaterializedOutput {
        MaterializedOutput::new(
            vec![create_mock_partition_ref(num_rows, num_rows * 10)],
            "worker1".into(),
            "127.0.0.1".to_string(),
            0,
        )
    }

    // ---------------------------------------------------------------
    // Simulated ExecutionStats — derived from the plan's actual phases
    //
    // Contract with daft-local-execution:
    //   • InMemoryScan → StatSnapshot::Source
    //   • Sort (BlockingSink) → StatSnapshot::Default
    //   • Both carry the phase from their LocalNodeContext
    // ---------------------------------------------------------------

    fn execution_stats_for_sort_plan(plan: &LocalPhysicalPlan, num_rows: u64) -> ExecutionStats {
        let LocalPhysicalPlan::Sort(sort) = plan else {
            panic!("expected Sort at top of plan");
        };
        let LocalPhysicalPlan::InMemoryScan(scan) = sort.input.as_ref() else {
            panic!("expected InMemoryScan as input to Sort");
        };

        ExecutionStats::new(
            "test".into(),
            vec![
                (
                    Arc::new(NodeInfo {
                        name: Arc::from("In Memory Scan"),
                        node_origin_id: scan.context.origin_node_id.unwrap_or(0),
                        node_type: NodeType::InMemoryScan,
                        node_category: NodeCategory::Source,
                        node_phase: scan.context.phase.clone(),
                        ..Default::default()
                    }),
                    StatSnapshot::Source(SourceSnapshot {
                        cpu_us: 10,
                        rows_out: num_rows,
                        bytes_read: 0,
                        bytes_out: 0,
                    }),
                ),
                (
                    Arc::new(NodeInfo {
                        name: Arc::from("Sort"),
                        node_origin_id: sort.context.origin_node_id.unwrap_or(0),
                        node_type: NodeType::Sort,
                        node_category: NodeCategory::BlockingSink,
                        node_phase: sort.context.phase.clone(),
                        ..Default::default()
                    }),
                    StatSnapshot::Default(DefaultSnapshot {
                        cpu_us: 50,
                        rows_in: num_rows,
                        rows_out: num_rows,
                        bytes_in: 0,
                        bytes_out: 0,
                    }),
                ),
            ],
        )
    }

    // ---------------------------------------------------------------
    // RuntimeNodeManager + SortStats wiring
    // ---------------------------------------------------------------

    fn make_sort_manager() -> RuntimeNodeManager {
        let meter = Meter::test_scope("sort_stats_test");
        let context = PipelineNodeContext::new(
            0,
            Arc::from("test-query"),
            SORT_NODE_ID,
            Arc::from("Sort"),
            NodeType::Sort,
            NodeCategory::BlockingSink,
        );
        let sort_stats: RuntimeStatsRef = Arc::new(SortStats::new(&meter, &context));
        let node_info = Arc::new(NodeInfo {
            name: Arc::from("Sort"),
            node_origin_id: SORT_ORIGIN_ID,
            node_type: NodeType::Sort,
            node_category: NodeCategory::BlockingSink,
            ..Default::default()
        });
        RuntimeNodeManager::new(&meter, sort_stats, node_info)
    }

    fn completed_event(stats: ExecutionStats) -> TaskEvent {
        TaskEvent::Completed {
            context: crate::scheduling::task::TaskContext {
                query_idx: 0,
                last_node_id: SORT_NODE_ID,
                task_id: 0,
                node_ids: vec![SORT_NODE_ID],
                plan_fingerprint: 0,
                checkpoint_id: None,
            },
            stats,
        }
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    /// Call the real `SortNode::build_final_sort_task` production code
    /// and verify that the plan it produces carries the correct phases.
    /// If execution_loop's plan construction changes, this test breaks
    /// because it calls the same method execution_loop delegates to.
    #[test]
    fn test_build_final_sort_task_produces_correct_phases() {
        let sort_node = make_sort_node();
        let mo = mock_materialized(1000);

        // Call the actual production method that execution_loop uses.
        let builder = sort_node.build_final_sort_task(vec![mo]);

        // Inspect the plan via the test-only accessor.
        let plan = builder.plan();
        let LocalPhysicalPlan::Sort(sort) = plan.as_ref() else {
            panic!("expected Sort plan");
        };
        assert_eq!(
            sort.context.phase.as_deref(),
            Some(FINAL_SORT_PHASE),
            "sort operator must carry the final-sort phase"
        );

        let LocalPhysicalPlan::InMemoryScan(scan) = sort.input.as_ref() else {
            panic!("expected InMemoryScan input");
        };
        assert_eq!(
            scan.context.phase.as_deref(),
            Some(FINAL_SORT_PHASE),
            "in-memory scan must carry the final-sort phase"
        );

        // Both nodes must set origin_node_id to the sort node's ID
        // so RuntimeNodeManager routes their stats correctly.
        assert_eq!(
            scan.context.origin_node_id,
            Some(SORT_NODE_ID as usize),
            "scan origin_node_id must match the sort node"
        );
        assert_eq!(
            sort.context.origin_node_id,
            Some(SORT_NODE_ID as usize),
            "sort origin_node_id must match the sort node"
        );
    }

    /// Run `build_final_sort_task`, derive ExecutionStats from the plan
    /// it produced, feed those stats through the full pipeline
    /// (TaskEvent → RuntimeNodeManager → SortStats), and verify
    /// that row counts are correct.
    ///
    /// This catches regressions where the plan's phases or node types
    /// drift out of sync with what SortStats expects.
    #[test]
    fn test_sort_stats_from_real_plan() {
        let sort_node = make_sort_node();
        let manager = make_sort_manager();

        // Build the task the same way execution_loop would.
        let builder = sort_node.build_final_sort_task(vec![mock_materialized(1000)]);
        let plan = builder.plan().clone();

        // Derive stats from the plan that was actually produced.
        let stats = execution_stats_for_sort_plan(&plan, 1000);
        manager.handle_task_event(&completed_event(stats));

        let (_, snapshot) = manager.export_snapshot();
        let StatSnapshot::Default(snap) = snapshot else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.rows_in, 1000);
        assert_eq!(snap.rows_out, 1000);
        // Duration: 10 (scan Source) + 50 (sort Default) = 60
        assert_eq!(snap.cpu_us, 60);
    }

    /// Multiple partition groups go through final sort. Verify that
    /// row counts accumulate across all of them.
    #[test]
    fn test_multiple_partitions_accumulate() {
        let sort_node = make_sort_node();
        let manager = make_sort_manager();

        for num_rows in [400u64, 600] {
            let builder =
                sort_node.build_final_sort_task(vec![mock_materialized(num_rows as usize)]);
            let stats = execution_stats_for_sort_plan(builder.plan(), num_rows);
            manager.handle_task_event(&completed_event(stats));
        }

        let (_, snapshot) = manager.export_snapshot();
        let StatSnapshot::Default(snap) = snapshot else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.rows_in, 1000);
        assert_eq!(snap.rows_out, 1000);
    }

    /// Stats from a node whose origin_id doesn't match the sort node
    /// must be silently ignored by RuntimeNodeManager.
    #[test]
    fn test_wrong_origin_id_ignored() {
        let manager = make_sort_manager();

        let wrong_origin = Arc::new(NodeInfo {
            node_origin_id: 999,
            node_phase: Some(FINAL_SORT_PHASE.to_string()),
            ..Default::default()
        });
        let stats = ExecutionStats::new(
            "test".into(),
            vec![(
                wrong_origin,
                StatSnapshot::Default(DefaultSnapshot {
                    cpu_us: 100,
                    rows_in: 500,
                    rows_out: 500,
                    bytes_in: 0,
                    bytes_out: 0,
                }),
            )],
        );
        manager.handle_task_event(&completed_event(stats));

        let (_, snapshot) = manager.export_snapshot();
        let StatSnapshot::Default(snap) = snapshot else {
            panic!("expected Default snapshot");
        };
        assert_eq!(snap.rows_in, 0);
        assert_eq!(snap.rows_out, 0);
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
