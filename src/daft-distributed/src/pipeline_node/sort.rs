use std::{future, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
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
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_vec,
    },
};

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
            let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                vec![mo],
                input_schema.clone(),
                pipeline_node.node_id(),
            );
            let sample = LocalPhysicalPlan::sample(
                in_memory_scan,
                SamplingMethod::Size(sample_size),
                true,
                None,
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(pipeline_node.node_id() as usize),
                    additional: None,
                },
            );
            let plan = LocalPhysicalPlan::project(
                sample,
                sample_by.clone(),
                sample_schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(pipeline_node.node_id() as usize),
                    additional: None,
                },
            );
            let builder = SwordfishTaskBuilder::new(plan, pipeline_node)
                .with_psets(pipeline_node.node_id(), psets);
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
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
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
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            );
            let builder = SwordfishTaskBuilder::new(plan, pipeline_node)
                .with_psets(node_id, mo.into_inner().0);
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
    const NODE_NAME: NodeName = "Sort";

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
            Self::NODE_NAME,
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
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
            let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                materialized_outputs,
                self.config.schema.clone(),
                self.node_id(),
            );
            let plan = LocalPhysicalPlan::sort(
                in_memory_scan,
                self.sort_by.clone(),
                self.descending.clone(),
                self.nulls_first.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let task =
                SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(self.node_id(), psets);
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
        )?;

        let partitioned_outputs = try_join_all(partition_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let transposed_outputs =
            transpose_materialized_outputs_from_vec(partitioned_outputs, num_partitions);

        for partition_group in transposed_outputs {
            let (in_memory_source_plan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                partition_group,
                self.config.schema.clone(),
                self.node_id(),
            );
            let plan = LocalPhysicalPlan::sort(
                in_memory_source_plan,
                self.sort_by.clone(),
                self.descending.clone(),
                self.nulls_first.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let task =
                SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(self.node_id(), psets);
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
