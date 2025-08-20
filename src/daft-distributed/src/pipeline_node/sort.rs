use std::{future, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
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
use futures::{future::try_join_all, TryStreamExt};
#[cfg(feature = "python")]
use pyo3::{prelude::*, Python};

use super::{
    make_new_task_from_materialized_outputs, DistributedPipelineNode, SubmittableTaskStream,
};
use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::{
        channel::{create_channel, Sender},
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
    child: Arc<dyn DistributedPipelineNode>,
}

impl SortNode {
    const NODE_NAME: NodeName = "Sort";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        output_schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );

        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
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

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    #[cfg(feature = "python")]
    fn get_partition_boundaries(
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

        let boundaries = Python::with_gil(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

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

            let boundaries = flotilla_module.call_method1(
                pyo3::intern!(py, "get_boundaries"),
                (
                    py_object_refs,
                    py_sort_by,
                    self.descending.clone(),
                    self.nulls_first.clone(),
                    num_partitions,
                ),
            )?;
            let boundaries =
                boundaries.extract::<daft_micropartition::python::PyMicroPartition>()?;
            Ok::<_, PyErr>(boundaries)
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
    fn get_partition_boundaries(
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
                &(self_clone.clone() as Arc<dyn DistributedPipelineNode>),
                move |input| {
                    LocalPhysicalPlan::sort(
                        input,
                        self_clone.sort_by.clone(),
                        self_clone.descending.clone(),
                        self_clone.nulls_first.clone(),
                        StatsState::NotMaterialized,
                    )
                },
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
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
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
        let boundaries = self.get_partition_boundaries(sampled_outputs, num_partitions)?;

        let partition_tasks = materialized_outputs
            .into_iter()
            .map(|mo| {
                let self_clone = self.clone();
                let boundaries = boundaries.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    vec![mo],
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
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
                &(self_clone.clone() as Arc<dyn DistributedPipelineNode>),
                move |input| {
                    LocalPhysicalPlan::sort(
                        input,
                        self_clone.sort_by.clone(),
                        self_clone.descending.clone(),
                        self_clone.nulls_first.clone(),
                        StatsState::NotMaterialized,
                    )
                },
            )?;
            let _ = result_tx.send(task).await;
        }
        Ok(())
    }

    fn multiline_display(&self) -> Vec<String> {
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
}

impl TreeDisplay for SortNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for SortNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(stage_context);
        let (result_tx, result_rx) = create_channel(1);
        stage_context.spawn(self.execution_loop(
            input_node,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        ));
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
