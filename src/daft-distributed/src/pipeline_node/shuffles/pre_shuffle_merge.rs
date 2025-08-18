use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        make_in_memory_task_from_materialized_outputs, DistributedPipelineNode, MaterializedOutput,
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
        worker::WorkerId,
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct PreShuffleMergeNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pre_shuffle_merge_threshold: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl PreShuffleMergeNode {
    const NODE_NAME: NodeName = "PreShuffleMerge";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        pre_shuffle_merge_threshold: usize,
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
            child.config().clustering_spec.clone(),
        );

        Self {
            config,
            context,
            pre_shuffle_merge_threshold,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("Pre-Shuffle Merge"),
            format!("Threshold: {}", self.pre_shuffle_merge_threshold),
        ]
    }
}

impl TreeDisplay for PreShuffleMergeNode {
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

impl DistributedPipelineNode for PreShuffleMergeNode {
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

        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

        let merge_execution = async move {
            self.execute_merge(input_node, task_id_counter, result_tx, scheduler_handle)
                .await
        };

        stage_context.spawn(merge_execution);
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl PreShuffleMergeNode {
    async fn execute_merge(
        self: Arc<Self>,
        input_stream: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // First, materialize all input data.
        let mut materialized_stream = input_stream.materialize(scheduler_handle.clone());

        // Bucket materialized outputs by worker ID
        let mut worker_buckets: HashMap<WorkerId, Vec<MaterializedOutput>> = HashMap::new();

        while let Some(output) = materialized_stream.try_next().await? {
            // Push the output to the appropriate bucket
            let worker_id = output.worker_id().clone();
            let bucket = worker_buckets.entry(worker_id.clone()).or_default();
            bucket.push(output);

            // Check if this bucket has reached the threshold
            if bucket
                .iter()
                .map(|output| {
                    output
                        .size_bytes()
                        .unwrap_or(self.pre_shuffle_merge_threshold) // If the size is not available, err on the safe side and use the threshold as a fallback
                })
                .sum::<usize>()
                >= self.pre_shuffle_merge_threshold
            {
                // Drain the bucket and create a task to merge the outputs
                if let Some(materialized_outputs) = worker_buckets.remove(&worker_id) {
                    let self_clone = self.clone();
                    let task = make_in_memory_task_from_materialized_outputs(
                        TaskContext::from((self.context(), task_id_counter.next())),
                        materialized_outputs,
                        &(self_clone as Arc<dyn DistributedPipelineNode>),
                    )?;

                    // Send the task directly to result_tx
                    if result_tx.send(task).await.is_err() {
                        break;
                    }
                }
            }
        }

        // Handle any remaining buckets that haven't reached the threshold
        for (_, materialized_outputs) in worker_buckets {
            if !materialized_outputs.is_empty() {
                let self_clone = self.clone();
                let task = make_in_memory_task_from_materialized_outputs(
                    TaskContext::from((self.context(), task_id_counter.next())),
                    materialized_outputs,
                    &(self_clone as Arc<dyn DistributedPipelineNode>),
                )?;

                if result_tx.send(task).await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }
}
