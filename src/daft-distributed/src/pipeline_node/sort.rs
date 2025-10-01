use std::{future, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_local_plan::{LocalPhysicalPlan, SamplingMethod};
use daft_logical_plan::{
    partitioning::{RangeRepartitionConfig, RepartitionSpec},
    stats::StatsState,
};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};
#[cfg(feature = "python")]
use pyo3::{Python, prelude::*};

use super::{PipelineNodeImpl, SubmittableTaskStream, make_new_task_from_materialized_outputs};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_vec,
    },
};

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
        logical_node_id: Option<NodeID>,
        plan_config: &PlanConfig,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        output_schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.plan_id,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
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

    #[cfg(feature = "python")]
    async fn get_partition_boundaries(
        &self,
        input: Vec<MaterializedOutput>,
        num_partitions: usize,
    ) -> DaftResult<RecordBatch> {
        let ray_partition_refs = input
            .into_iter()
            .flat_map(|mo| mo.into_inner().0)
            .map(|pr| {
                let ray_partition_ref = pr
                    .as_any()
                    .downcast_ref::<crate::python::ray::RayPartitionRef>()
                    .ok_or(DaftError::InternalError(
                        "Failed to downcast partition ref".to_string(),
                    ))?;
                Ok(ray_partition_ref.clone())
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let (task_locals, py_object_refs, py_sort_by, descending, nulls_first) =
            Python::with_gil(|py| {
                let task_locals = crate::utils::runtime::PYO3_ASYNC_RUNTIME_LOCALS
                    .get()
                    .expect("Python task locals not initialized")
                    .clone_ref(py);

                let py_object_refs = ray_partition_refs
                    .into_iter()
                    .map(|pr| pr.get_object_ref(py))
                    .collect::<Vec<_>>();

                let py_sort_by = self
                    .sort_by
                    .iter()
                    .map(|e| daft_dsl::python::PyExpr {
                        expr: e.inner().clone(),
                    })
                    .collect::<Vec<_>>();

                (
                    task_locals,
                    py_object_refs,
                    py_sort_by,
                    self.descending.clone(),
                    self.nulls_first.clone(),
                )
            });

        let await_coroutine = async move {
            let result = Python::with_gil(|py| {
                let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

                let coroutine = flotilla_module.call_method1(
                    pyo3::intern!(py, "get_boundaries"),
                    (
                        py_object_refs,
                        py_sort_by,
                        descending,
                        nulls_first,
                        num_partitions,
                    ),
                )?;
                pyo3_async_runtimes::tokio::into_future(coroutine)
            })?
            .await?;
            DaftResult::Ok(result)
        };

        let boundaries = pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await?;
        let boundaries = Python::with_gil(|py| {
            boundaries.extract::<daft_micropartition::python::PyMicroPartition>(py)
        })?;

        let boundaries = boundaries
            .inner
            .concat_or_get(IOStatsContext::new(
                "daft-distributed::sort::get_boundaries",
            ))?
            .ok_or(DaftError::InternalError(
                "No boundaries found for sort node".to_string(),
            ))?;
        Ok(boundaries)
    }

    #[cfg(not(feature = "python"))]
    async fn get_partition_boundaries(
        &self,
        input: Vec<MaterializedOutput>,
        num_partitions: usize,
    ) -> DaftResult<RecordBatch> {
        unimplemented!("Distributed sort requires Python feature to be enabled")
    }

    async fn execution_loop(
        self: Arc<Self>,
        input_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_outputs = input_node
            .materialize(scheduler_handle.clone())
            .try_filter(|mo| future::ready(mo.num_rows().unwrap_or(0) > 0))
            .try_collect::<Vec<_>>()
            .await?;

        if materialized_outputs.is_empty() {
            return Ok(());
        }

        if materialized_outputs.len() == 1 {
            let self_clone = self.clone();
            let task = make_new_task_from_materialized_outputs(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                materialized_outputs,
                &(self_clone.clone() as Arc<dyn PipelineNodeImpl>),
                move |input| {
                    LocalPhysicalPlan::sort(
                        input,
                        self_clone.sort_by.clone(),
                        self_clone.descending.clone(),
                        self_clone.nulls_first.clone(),
                        StatsState::NotMaterialized,
                    )
                },
                None,
            )?;
            let _ = result_tx.send(task).await;
            return Ok(());
        }

        let submitted_sample_tasks = materialized_outputs
            .clone()
            .into_iter()
            .map(|mo| {
                let self_clone = self.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    vec![mo],
                    &(self.clone() as Arc<dyn PipelineNodeImpl>),
                    move |input| {
                        let sample = LocalPhysicalPlan::sample(
                            input,
                            SamplingMethod::Size(
                                self_clone.config.execution_config.sample_size_for_sort,
                            ),
                            false,
                            None,
                            StatsState::NotMaterialized,
                        );
                        LocalPhysicalPlan::project(
                            sample,
                            self_clone.sort_by.clone(),
                            self_clone.config.schema.clone(),
                            StatsState::NotMaterialized,
                        )
                    },
                    None,
                )?;
                let submitted_task = task.submit(&scheduler_handle)?;
                Ok(submitted_task)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        let sampled_outputs = try_join_all(submitted_sample_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let num_partitions = self.child.config().clustering_spec.num_partitions();
        let boundaries = self
            .get_partition_boundaries(sampled_outputs, num_partitions)
            .await?;

        let partition_tasks = materialized_outputs
            .into_iter()
            .map(|mo| {
                let self_clone = self.clone();
                let boundaries = boundaries.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    vec![mo],
                    &(self.clone() as Arc<dyn PipelineNodeImpl>),
                    move |input| {
                        LocalPhysicalPlan::repartition(
                            input,
                            RepartitionSpec::Range(RangeRepartitionConfig::new(
                                Some(num_partitions),
                                boundaries,
                                self_clone.sort_by.clone(),
                                self_clone.descending.clone(),
                            )),
                            num_partitions,
                            self_clone.config.schema.clone(),
                            StatsState::NotMaterialized,
                        )
                    },
                    None,
                )?;
                let submitted_task = task.submit(&scheduler_handle)?;
                Ok(submitted_task)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let partitioned_outputs = try_join_all(partition_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let transposed_outputs =
            transpose_materialized_outputs_from_vec(partitioned_outputs, num_partitions)?;

        for partition_group in transposed_outputs {
            let self_clone = self.clone();

            let task = make_new_task_from_materialized_outputs(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                partition_group,
                &(self_clone.clone() as Arc<dyn PipelineNodeImpl>),
                move |input| {
                    LocalPhysicalPlan::sort(
                        input,
                        self_clone.sort_by.clone(),
                        self_clone.descending.clone(),
                        self_clone.nulls_first.clone(),
                        StatsState::NotMaterialized,
                    )
                },
                None,
            )?;
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
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);
        plan_context.spawn(self.execution_loop(
            input_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        SubmittableTaskStream::from(result_rx)
    }
}
