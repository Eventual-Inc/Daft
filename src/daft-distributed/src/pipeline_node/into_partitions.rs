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
        append_plan_to_existing_task, make_in_memory_task_from_materialized_outputs,
        make_new_task_from_materialized_outputs, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext,
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
        assert!(
            tasks.len() >= self.num_partitions,
            "Cannot coalesce from {} to {} partitions.",
            tasks.len(),
            self.num_partitions
        );

        // Coalesce partitions evenly with remainder handling
        // Example: 10 inputs, 3 partitions = 4, 3, 3

        // Base inputs per partition: 10 / 3 = 3 (all tasks get at least 3 inputs)
        let base_inputs_per_partition = tasks.len() / self.num_partitions;
        // Remainder: 10 % 3 = 1 (one task gets an extra input)
        let num_partitions_with_extra_input = tasks.len() % self.num_partitions;

        let mut output_futures = OrderedJoinSet::new();

        let mut task_iter = tasks.into_iter();
        for partition_idx in 0..self.num_partitions {
            let mut chunk_size = base_inputs_per_partition;
            // This partition needs an extra input, i.e. partition_idx == 0 and remainder == 1
            if partition_idx < num_partitions_with_extra_input {
                chunk_size += 1;
            }

            // Submit all the tasks for this partition
            let submitted_tasks = task_iter
                .by_ref()
                .take(chunk_size)
                .map(|task| task.submit(scheduler_handle))
                .collect::<DaftResult<Vec<_>>>()?;
            output_futures.spawn(async move {
                let materialized_output = futures::future::try_join_all(submitted_tasks)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                DaftResult::Ok(materialized_output)
            });
        }

        while let Some(result) = output_futures.join_next().await {
            // Collect all the outputs from this task and coalesce them into a single task.
            let materialized_outputs = result??;
            let self_arc = self.clone();
            let task = make_new_task_from_materialized_outputs(
                TaskContext::from((&self.context, task_id_counter.next())),
                materialized_outputs,
                &(self_arc as Arc<dyn DistributedPipelineNode>),
                move |input| {
                    LocalPhysicalPlan::into_partitions(input, 1, StatsState::NotMaterialized)
                },
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
        assert!(
            tasks.len() <= self.num_partitions,
            "Cannot split from {} to {} partitions.",
            tasks.len(),
            self.num_partitions
        );

        // Split partitions evenly with remainder handling
        // Example: 3 inputs, 10 partitions = 4, 3, 3

        // Base outputs per partition: 10 / 3 = 3 (all partitions will split into at least 3 outputs)
        let base_splits_per_partition = self.num_partitions / tasks.len();
        // Remainder: 10 % 3 = 1 (one partition will split into 4 outputs)
        let num_partitions_with_extra_output = self.num_partitions % tasks.len();

        let mut output_futures = OrderedJoinSet::new();

        for (input_partition_idx, task) in tasks.into_iter().enumerate() {
            let mut num_outputs = base_splits_per_partition;
            // This partition will split into one more output, i.e. input_partition_idx == 0 and remainder == 1
            if input_partition_idx < num_partitions_with_extra_output {
                num_outputs += 1;
            }
            let into_partitions_task = append_plan_to_existing_task(
                task,
                &(self.clone() as Arc<dyn DistributedPipelineNode>),
                &move |plan| {
                    LocalPhysicalPlan::into_partitions(
                        plan,
                        num_outputs,
                        StatsState::NotMaterialized,
                    )
                },
            );
            let submitted_task = into_partitions_task.submit(scheduler_handle)?;
            output_futures.spawn(submitted_task);
        }

        // Collect all the outputs and emit a new task for each output.
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

    async fn execute_into_partitions(
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

        stage_context.spawn(self.execute_into_partitions(
            input_stream,
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
