use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;

use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, shuffle_exchange::ShuffleExchangeNode,
        SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::TaskIDCounter,
    utils::channel::Sender,
};

#[derive(Clone, Debug)]
pub struct NaiveFullyMaterializingMapReduce;

impl NaiveFullyMaterializingMapReduce {
    pub async fn execute(
        &self,
        shuffle_exchange_node: Arc<ShuffleExchangeNode>,
        input_stream: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Create the repartition instruction pipeline
        let self_clone = shuffle_exchange_node.clone();
        let local_repartition_node =
            input_stream.pipeline_instruction(shuffle_exchange_node.clone(), move |input| {
                LocalPhysicalPlan::repartition(
                    input,
                    self_clone.columns().clone(),
                    self_clone.num_partitions(),
                    self_clone.config().schema.clone(),
                    StatsState::NotMaterialized,
                )
            });

        // Trigger materialization of the partitions
        let materialized_stream = local_repartition_node.materialize(scheduler_handle.clone());
        let transposed_outputs = ShuffleExchangeNode::transpose_materialized_outputs(
            materialized_stream,
            shuffle_exchange_node.num_partitions(),
        )
        .await?;

        // Make each partition group input to a in-memory scan
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
