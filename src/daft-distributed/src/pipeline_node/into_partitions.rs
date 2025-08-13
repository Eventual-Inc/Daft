use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, SubmittableTaskStream};
use crate::{
    pipeline_node::{
        append_plan_to_existing_task, make_in_memory_task_from_materialized_outputs, NodeID,
        NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::{
        channel::{create_channel, Sender},
        joinset::OrderedJoinSet,
    },
};

#[derive(Clone)]
pub(crate) struct IntoPartitionsNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    num_partitions: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl IntoPartitionsNode {
    const NODE_NAME: NodeName = "IntoPartitions";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        num_partitions: usize,
        schema: SchemaRef,
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
            schema,
            stage_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(num_partitions).into()),
        );

        Self {
            config,
            context,
            num_partitions,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "IntoPartitions".to_string(),
            format!("Num partitions = {}", self.num_partitions),
        ]
    }

    async fn coalesce_tasks(
        self: Arc<Self>,
        tasks: Vec<SubmittableTask<SwordfishTask>>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        // Group tasks into num_partitions groups maintaining order
        // Fill each partition sequentially before moving to the next
        let mut task_groups: Vec<Vec<SubmittableTask<SwordfishTask>>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();

        let tasks_per_partition = tasks.len().div_ceil(self.num_partitions);
        for (i, task) in tasks.into_iter().enumerate() {
            let partition_idx = i / tasks_per_partition;
            if partition_idx < self.num_partitions {
                task_groups[partition_idx].push(task);
            }
        }

        let mut output_futures = OrderedJoinSet::new();

        for task_group in task_groups.into_iter().filter(|x| !x.is_empty()) {
            // Multiple tasks, materialize and combine
            let mut output_futures_for_group = Vec::new();
            for task in task_group {
                let future = task.submit(scheduler_handle)?;
                output_futures_for_group.push(future);
            }
            output_futures.spawn(async move {
                let materialized_output = futures::future::try_join_all(output_futures_for_group)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                DaftResult::Ok(materialized_output)
            });
        }

        while let Some(result) = output_futures.join_next().await {
            let materialized_outputs = result??;
            let self_arc = self.clone();
            let task = make_in_memory_task_from_materialized_outputs(
                TaskContext::from((&self.context, task_id_counter.next())),
                materialized_outputs,
                &(self_arc as Arc<dyn DistributedPipelineNode>),
            )?;
            if result_tx.send(task).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn split_tasks(
        self: Arc<Self>,
        tasks: Vec<SubmittableTask<SwordfishTask>>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
    ) -> DaftResult<()> {
        // Split tasks into more partitions by splitting each input task into multiple output tasks.
        // This preserves order and tries to distribute outputs as evenly as possible.
        let num_input_partitions = tasks.len();
        let num_output_partitions = self.num_partitions;
        assert!(
            num_output_partitions >= num_input_partitions,
            "Cannot split from {} to {} partitions.",
            num_input_partitions,
            num_output_partitions
        );

        let base_splits_per_partition = num_output_partitions / num_input_partitions;
        let num_partitions_with_extra_output = num_output_partitions % num_input_partitions;

        let mut output_futures = OrderedJoinSet::new();

        for (input_partition_idx, task) in tasks.into_iter().enumerate() {
            // Each input partition gets either base_splits_per_partition or base_splits_per_partition+1 outputs
            let num_out = if input_partition_idx < num_partitions_with_extra_output {
                base_splits_per_partition + 1
            } else {
                base_splits_per_partition
            };
            let into_partitions_task = append_plan_to_existing_task(
                task,
                &(self.clone() as Arc<dyn DistributedPipelineNode>),
                &move |plan| {
                    LocalPhysicalPlan::into_partitions(plan, num_out, StatsState::NotMaterialized)
                },
            );
            let submitted_task = into_partitions_task.submit(scheduler_handle)?;
            output_futures.spawn(submitted_task);
        }

        // Collect all the split outputs and send as new tasks
        while let Some(result) = output_futures.join_next().await {
            let materialized_outputs = result??;
            if let Some(output) = materialized_outputs {
                for output in output.split_into_materialized_outputs() {
                    let self_arc = self.clone();
                    let task = make_in_memory_task_from_materialized_outputs(
                        TaskContext::from((&self.context, task_id_counter.next())),
                        vec![output],
                        &(self_arc as Arc<dyn DistributedPipelineNode>),
                    )?;
                    if result_tx.send(task).await.is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn execution_loop(
        self: Arc<Self>,
        input_stream: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Collect all input tasks without materializing to count them
        let input_tasks: Vec<SubmittableTask<SwordfishTask>> = input_stream.collect().await;
        let num_input_tasks = input_tasks.len();

        match num_input_tasks.cmp(&self.num_partitions) {
            std::cmp::Ordering::Equal => {
                // Exact match - pass through as-is
                for task in input_tasks {
                    let _ = result_tx.send(task).await;
                }
            }
            std::cmp::Ordering::Greater => {
                // Too many tasks - coalesce
                self.coalesce_tasks(input_tasks, &scheduler_handle, &task_id_counter, result_tx)
                    .await?;
            }
            std::cmp::Ordering::Less => {
                // Too few tasks - split
                self.split_tasks(input_tasks, &scheduler_handle, &task_id_counter, result_tx)
                    .await?;
            }
        };
        Ok(())
    }
}

impl TreeDisplay for IntoPartitionsNode {
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

impl DistributedPipelineNode for IntoPartitionsNode {
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
        let input_stream = self.child.clone().produce_tasks(stage_context);
        let (result_tx, result_rx) = create_channel(1);

        let execution_loop = self.execution_loop(
            input_stream,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
