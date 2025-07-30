use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, make_new_task_from_materialized_outputs,
        shuffle_exchange::ShuffleExchangeNode, MaterializedOutput, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
        task::{SwordfishTask, TaskContext},
        worker::WorkerId,
    },
    stage::TaskIDCounter,
    utils::channel::Sender,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MapReduceWithPreShuffleMerge {
    pub pre_shuffle_merge_threshold: usize,
}

impl MapReduceWithPreShuffleMerge {
    pub fn new(pre_shuffle_merge_threshold: usize) -> Self {
        Self {
            pre_shuffle_merge_threshold,
        }
    }
}

impl MapReduceWithPreShuffleMerge {
    pub async fn execute(
        &self,
        shuffle_exchange_node: Arc<ShuffleExchangeNode>,
        input_stream: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // First, materialize all input data (don't pipeline repartition)
        let materialized_stream = input_stream.materialize(scheduler_handle.clone());

        // Bucket materialized outputs by worker ID
        let mut worker_buckets: HashMap<WorkerId, Vec<MaterializedOutput>> = HashMap::new();
        let mut submitted_tasks: Vec<SubmittedTask> = Vec::new();

        // Iterate through the stream directly
        let mut stream = materialized_stream;
        while let Some(output) = stream.try_next().await? {
            let worker_id = output.worker_id().clone();
            worker_buckets
                .entry(worker_id.clone())
                .or_default()
                .push(output);

            // Check if this bucket has reached the threshold
            if let Some(bucket) = worker_buckets.get(&worker_id) {
                if bucket
                    .iter()
                    .map(|output| output.size_bytes().unwrap_or(0))
                    .sum::<usize>()
                    >= self.pre_shuffle_merge_threshold
                {
                    // Drain the bucket and create a new task
                    if let Some(materialized_outputs) = worker_buckets.remove(&worker_id) {
                        let shuffle_exchange_node_clone = shuffle_exchange_node.clone();
                        let columns = shuffle_exchange_node_clone.columns().clone();
                        let num_partitions = shuffle_exchange_node.num_partitions();
                        let schema = shuffle_exchange_node.config().schema.clone();
                        let task = make_new_task_from_materialized_outputs(
                            TaskContext::from((
                                shuffle_exchange_node.context(),
                                task_id_counter.next(),
                            )),
                            materialized_outputs,
                            &(shuffle_exchange_node_clone
                                as Arc<dyn crate::pipeline_node::DistributedPipelineNode>),
                            move |plan| {
                                LocalPhysicalPlan::repartition(
                                    plan,
                                    columns,
                                    num_partitions,
                                    schema,
                                    StatsState::NotMaterialized,
                                )
                            },
                        )?;

                        // Submit the task to the scheduler and collect it
                        let submitted_task = task.submit(&scheduler_handle)?;
                        submitted_tasks.push(submitted_task);
                    }
                }
            }
        }

        // Handle any remaining buckets that haven't reached the threshold
        for (_, materialized_outputs) in worker_buckets {
            if !materialized_outputs.is_empty() {
                let shuffle_exchange_node_clone = shuffle_exchange_node.clone();
                let columns = shuffle_exchange_node_clone.columns().clone();
                let num_partitions = shuffle_exchange_node_clone.num_partitions();
                let schema = shuffle_exchange_node_clone.config().schema.clone();
                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((
                        shuffle_exchange_node_clone.context(),
                        task_id_counter.next(),
                    )),
                    materialized_outputs,
                    &(shuffle_exchange_node_clone
                        as Arc<dyn crate::pipeline_node::DistributedPipelineNode>),
                    move |plan| {
                        LocalPhysicalPlan::repartition(
                            plan,
                            columns,
                            num_partitions,
                            schema,
                            StatsState::NotMaterialized,
                        )
                    },
                )?;

                // Submit the task to the scheduler and collect it
                let submitted_task = task.submit(&scheduler_handle)?;
                submitted_tasks.push(submitted_task);
            }
        }

        // Await all submitted tasks to get their materialized outputs
        let mut all_materialized_outputs = Vec::new();
        for submitted_task in submitted_tasks {
            if let Some(materialized_output) = submitted_task.await? {
                all_materialized_outputs.push(materialized_output);
            }
        }

        // Convert Vec to stream for transpose_materialized_outputs
        let materialized_stream =
            futures::stream::iter(all_materialized_outputs.into_iter().map(Ok));

        // Transpose the materialized outputs
        let transposed_outputs = ShuffleExchangeNode::transpose_materialized_outputs(
            materialized_stream,
            shuffle_exchange_node.num_partitions(),
        )
        .await?;

        // Make each partition group input to an in-memory scan
        for partition_group in transposed_outputs {
            let shuffle_exchange_node_clone = shuffle_exchange_node.clone();
            let task = make_in_memory_scan_from_materialized_outputs(
                TaskContext::from((
                    shuffle_exchange_node_clone.context(),
                    task_id_counter.next(),
                )),
                partition_group,
                &(shuffle_exchange_node_clone
                    as Arc<dyn crate::pipeline_node::DistributedPipelineNode>),
            )?;

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }
}
