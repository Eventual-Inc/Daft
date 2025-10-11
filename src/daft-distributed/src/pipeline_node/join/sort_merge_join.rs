use std::{future, sync::Arc};

use common_display::{DisplayLevel, tree::TreeDisplay};
use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_local_plan::{LocalPhysicalPlan, SamplingMethod};
use daft_logical_plan::{
    JoinType,
    partitioning::{RangeRepartitionConfig, RepartitionSpec},
    stats::StatsState,
};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use futures::{TryStreamExt, future::try_join_all};
#[cfg(feature = "python")]
use pyo3::{Python, prelude::*};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, SubmittableTaskStream, make_new_task_from_materialized_outputs,
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

pub(crate) struct SortMergeJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    #[allow(dead_code)] // Not currently used by LocalPhysicalPlan::sort_merge_join
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,

    left: Arc<dyn DistributedPipelineNode>,
    right: Arc<dyn DistributedPipelineNode>,
}

impl SortMergeJoinNode {
    const NODE_NAME: NodeName = "SortMergeJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        plan_config: &PlanConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        _num_partitions: usize,
        left: Arc<dyn DistributedPipelineNode>,
        right: Arc<dyn DistributedPipelineNode>,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.plan_id,
            node_id,
            Self::NODE_NAME,
            vec![left.node_id(), right.node_id()],
            vec![left.name(), right.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            left.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            left,
            right,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["Sort Merge Join".to_string()];
        res.push(format!(
            "Left on: {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right on: {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }

    #[cfg(feature = "python")]
    async fn get_partition_boundaries(
        &self,
        left_samples: Vec<MaterializedOutput>,
        right_samples: Vec<MaterializedOutput>,
        num_partitions: usize,
    ) -> DaftResult<RecordBatch> {
        let mut all_samples = left_samples;
        all_samples.extend(right_samples);

        let ray_partition_refs = all_samples
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
                    .left_on
                    .iter()
                    .map(|e| daft_dsl::python::PyExpr {
                        expr: e.inner().clone(),
                    })
                    .collect::<Vec<_>>();

                let descending = vec![false; self.left_on.len()];
                let nulls_first = vec![false; self.left_on.len()];

                (
                    task_locals,
                    py_object_refs,
                    py_sort_by,
                    descending,
                    nulls_first,
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
                "daft-distributed::sort_merge_join::get_boundaries",
            ))?
            .ok_or(DaftError::InternalError(
                "No boundaries found for sort merge join node".to_string(),
            ))?;
        Ok(boundaries)
    }

    #[cfg(not(feature = "python"))]
    async fn get_partition_boundaries(
        &self,
        left_samples: Vec<MaterializedOutput>,
        right_samples: Vec<MaterializedOutput>,
        num_partitions: usize,
    ) -> DaftResult<RecordBatch> {
        unimplemented!("Distributed sort merge join requires Python feature to be enabled")
    }

    async fn execution_loop(
        self: Arc<Self>,
        left_input: SubmittableTaskStream,
        right_input: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Materialize both inputs
        let left_materialized = left_input
            .materialize(scheduler_handle.clone())
            .try_filter(|mo| future::ready(mo.num_rows().unwrap_or(0) > 0))
            .try_collect::<Vec<_>>()
            .await?;

        let right_materialized = right_input
            .materialize(scheduler_handle.clone())
            .try_filter(|mo| future::ready(mo.num_rows().unwrap_or(0) > 0))
            .try_collect::<Vec<_>>()
            .await?;

        // Handle empty inputs
        if left_materialized.is_empty() || right_materialized.is_empty() {
            return Ok(());
        }

        // Special case: if both sides have only 1 partition, just do a direct join
        if left_materialized.len() == 1 && right_materialized.len() == 1 {
            let self_clone = self.clone();
            let left_node_id = self_clone.left.node_id();
            let right_node_id = self_clone.right.node_id();
            let right_schema = self_clone.right.config().schema.clone();

            let left_partition_group = left_materialized;
            let right_partition_group = right_materialized;

            // Extract right partition refs and compute metadata
            let right_num_partitions = right_partition_group
                .iter()
                .flat_map(|output| output.partitions())
                .count();

            let mut right_total_size_bytes = 0usize;
            let mut right_total_num_rows = 0usize;

            for output in &right_partition_group {
                right_total_size_bytes += output.size_bytes()?;
                right_total_num_rows += output.num_rows()?;
            }

            // Create the task with both left and right partition refs
            let task_context = TaskContext::from((&self_clone.context, task_id_counter.next()));

            let left_in_memory_source_plan =
                crate::pipeline_node::make_in_memory_scan_from_materialized_outputs(
                    &left_partition_group,
                    self_clone.left.config().schema.clone(),
                    left_node_id,
                )?;

            let left_partition_refs = left_partition_group
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>();

            let right_partition_refs_for_psets = right_partition_group
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>();

            // Build the join plan
            let plan = {
                let right_in_memory_info = daft_logical_plan::InMemoryInfo::new(
                    right_schema.clone(),
                    right_node_id.to_string(),
                    None,
                    right_num_partitions,
                    right_total_size_bytes,
                    right_total_num_rows,
                    None,
                    None,
                );
                let right_in_memory_source = LocalPhysicalPlan::in_memory_scan(
                    right_in_memory_info,
                    StatsState::NotMaterialized,
                );

                LocalPhysicalPlan::sort_merge_join(
                    left_in_memory_source_plan,
                    right_in_memory_source,
                    self_clone.left_on.clone(),
                    self_clone.right_on.clone(),
                    self_clone.join_type,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                )
            };

            // Create psets with both left and right partition refs
            let mut psets = std::collections::HashMap::new();
            psets.insert(left_node_id.to_string(), left_partition_refs);
            psets.insert(right_node_id.to_string(), right_partition_refs_for_psets);

            let task = SwordfishTask::new(
                task_context,
                plan,
                self_clone.config.execution_config.clone(),
                psets,
                crate::scheduling::task::SchedulingStrategy::Spread,
                self_clone.context().to_hashmap(),
            );

            let _ = result_tx.send(SubmittableTask::new(task)).await;
            return Ok(());
        }

        // Sample both sides
        let left_sample_tasks = left_materialized
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
                            self_clone.left_on.clone(),
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

        let right_sample_tasks = right_materialized
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
                            self_clone.right_on.clone(),
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

        let left_sampled_outputs = try_join_all(left_sample_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let right_sampled_outputs = try_join_all(right_sample_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Get partition boundaries
        let num_partitions = self.left.config().clustering_spec.num_partitions();
        let boundaries = self
            .get_partition_boundaries(left_sampled_outputs, right_sampled_outputs, num_partitions)
            .await?;

        // Range repartition left side
        let left_partition_tasks = left_materialized
            .into_iter()
            .map(|mo| {
                let self_clone = self.clone();
                let boundaries = boundaries.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    vec![mo],
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    move |input| {
                        let descending = vec![false; self_clone.left_on.len()];
                        LocalPhysicalPlan::repartition(
                            input,
                            RepartitionSpec::Range(RangeRepartitionConfig::new(
                                Some(num_partitions),
                                boundaries,
                                self_clone.left_on.clone(),
                                descending,
                            )),
                            num_partitions,
                            self_clone.left.config().schema.clone(),
                            StatsState::NotMaterialized,
                        )
                    },
                    None,
                )?;
                let submitted_task = task.submit(&scheduler_handle)?;
                Ok(submitted_task)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // Range repartition right side
        let right_partition_tasks = right_materialized
            .into_iter()
            .map(|mo| {
                let self_clone = self.clone();
                let boundaries = boundaries.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    vec![mo],
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    move |input| {
                        let descending = vec![false; self_clone.right_on.len()];
                        LocalPhysicalPlan::repartition(
                            input,
                            RepartitionSpec::Range(RangeRepartitionConfig::new(
                                Some(num_partitions),
                                boundaries,
                                self_clone.right_on.clone(),
                                descending,
                            )),
                            num_partitions,
                            self_clone.right.config().schema.clone(),
                            StatsState::NotMaterialized,
                        )
                    },
                    None,
                )?;
                let submitted_task = task.submit(&scheduler_handle)?;
                Ok(submitted_task)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // Wait for both sides to be partitioned
        let left_partitioned_outputs = try_join_all(left_partition_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let right_partitioned_outputs = try_join_all(right_partition_tasks)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // Transpose outputs to group by partition index
        let left_transposed_outputs =
            transpose_materialized_outputs_from_vec(left_partitioned_outputs, num_partitions)?;
        let right_transposed_outputs =
            transpose_materialized_outputs_from_vec(right_partitioned_outputs, num_partitions)?;

        // Emit sort-merge join tasks for each partition pair
        for (left_partition_group, right_partition_group) in left_transposed_outputs
            .into_iter()
            .zip(right_transposed_outputs)
        {
            let self_clone = self.clone();
            let left_node_id = self_clone.left.node_id();
            let right_node_id = self_clone.right.node_id();
            let right_schema = self_clone.right.config().schema.clone();

            // Extract right partition refs and compute metadata
            let right_num_partitions = right_partition_group
                .iter()
                .flat_map(|output| output.partitions())
                .count();
            let mut right_total_size_bytes = 0usize;
            let mut right_total_num_rows = 0usize;

            for output in &right_partition_group {
                right_total_size_bytes += output.size_bytes()?;
                right_total_num_rows += output.num_rows()?;
            }

            // Create the task with both left and right partition refs
            let task_context = TaskContext::from((&self_clone.context, task_id_counter.next()));

            let left_in_memory_source_plan =
                crate::pipeline_node::make_in_memory_scan_from_materialized_outputs(
                    &left_partition_group,
                    self_clone.left.config().schema.clone(),
                    left_node_id,
                )?;

            let left_partition_refs = left_partition_group
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>();

            let right_partition_refs_for_psets = right_partition_group
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>();

            // Build the join plan
            let plan = {
                let right_in_memory_info = daft_logical_plan::InMemoryInfo::new(
                    right_schema.clone(),
                    right_node_id.to_string(),
                    None,
                    right_num_partitions,
                    right_total_size_bytes,
                    right_total_num_rows,
                    None,
                    None,
                );
                let right_in_memory_source = LocalPhysicalPlan::in_memory_scan(
                    right_in_memory_info,
                    StatsState::NotMaterialized,
                );

                LocalPhysicalPlan::sort_merge_join(
                    left_in_memory_source_plan,
                    right_in_memory_source,
                    self_clone.left_on.clone(),
                    self_clone.right_on.clone(),
                    self_clone.join_type,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                )
            };

            // Create psets with both left and right partition refs
            let mut psets = std::collections::HashMap::new();
            psets.insert(left_node_id.to_string(), left_partition_refs);
            psets.insert(right_node_id.to_string(), right_partition_refs_for_psets);

            let task = SwordfishTask::new(
                task_context,
                plan,
                self_clone.config.execution_config.clone(),
                psets,
                crate::scheduling::task::SchedulingStrategy::Spread,
                self_clone.context().to_hashmap(),
            );

            let _ = result_tx.send(SubmittableTask::new(task)).await;
        }
        Ok(())
    }
}

impl TreeDisplay for SortMergeJoinNode {
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
        vec![self.left.as_tree_display(), self.right.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for SortMergeJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
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
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
