use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, SubmittableTaskStream,
        make_in_memory_task_from_materialized_outputs,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
        worker::WorkerId,
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct PreShuffleMergeNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pre_shuffle_merge_threshold: usize,
    child: DistributedPipelineNode,
}

impl PreShuffleMergeNode {
    const NODE_NAME: NodeName = "PreShuffleMerge";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pre_shuffle_merge_threshold: usize,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );

        Self {
            config,
            context,
            pre_shuffle_merge_threshold,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for PreShuffleMergeNode {
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
        vec![
            format!("Pre-Shuffle Merge"),
            format!("Threshold: {}", self.pre_shuffle_merge_threshold),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let merge_execution = async move {
            self.execute_merge(input_node, task_id_counter, result_tx, scheduler_handle)
                .await
        };

        plan_context.spawn(merge_execution);
        SubmittableTaskStream::from(result_rx)
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
                .map(|output| output.size_bytes())
                .sum::<usize>()
                >= self.pre_shuffle_merge_threshold
            {
                // Drain the bucket and create a task to merge the outputs
                if let Some(materialized_outputs) = worker_buckets.remove(&worker_id) {
                    let self_clone = self.clone();
                    let task = make_in_memory_task_from_materialized_outputs(
                        TaskContext::from((self.context(), task_id_counter.next())),
                        materialized_outputs,
                        self_clone.config.schema.clone(),
                        &(self_clone as Arc<dyn PipelineNodeImpl>),
                        Some(SchedulingStrategy::WorkerAffinity {
                            worker_id,
                            soft: false,
                        }),
                    );

                    // Send the task directly to result_tx
                    if result_tx.send(task).await.is_err() {
                        break;
                    }
                }
            }
        }

        // Handle any remaining buckets that haven't reached the threshold
        for (worker_id, materialized_outputs) in worker_buckets {
            if !materialized_outputs.is_empty() {
                let self_clone = self.clone();
                let task = make_in_memory_task_from_materialized_outputs(
                    TaskContext::from((self.context(), task_id_counter.next())),
                    materialized_outputs,
                    self_clone.config.schema.clone(),
                    &(self_clone as Arc<dyn PipelineNodeImpl>),
                    Some(SchedulingStrategy::WorkerAffinity {
                        worker_id,
                        soft: false,
                    }),
                );

                if result_tx.send(task).await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }
}
