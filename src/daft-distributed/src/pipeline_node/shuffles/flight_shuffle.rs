use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext, TaskID},
    },
    utils::channel::{Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.plan_id as u64) << 32) | (context.node_id as u64)
}

pub(crate) struct FlightShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    shuffle_id: u64,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    child: DistributedPipelineNode,
}

impl FlightShuffleNode {
    const NODE_NAME: NodeName = "FlightShuffle";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
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
        let shuffle_id = make_shuffle_id(&context);
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            shuffle_id,
            repartition_spec,
            num_partitions,
            shuffle_dirs,
            compression,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    // Async execution to handle flight shuffle write and read operations
    async fn execution_loop(
        self: Arc<Self>,
        local_flight_shuffle_write_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of the partitions with flight shuffle write operations
        let outputs = local_flight_shuffle_write_node
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;

        // Collect server addresses and their corresponding task_ids (cache_ids)
        let mut server_cache_mapping: std::collections::HashMap<String, Vec<TaskID>> =
            std::collections::HashMap::new();
        for output in &outputs {
            let server_address = output.ip_address().clone();
            let task_id = output.task_id();
            server_cache_mapping
                .entry(server_address)
                .or_default()
                .push(task_id);
        }

        // For each partition group, create tasks that read from flight servers
        for partition_idx in 0..self.num_partitions {
            // Create a flight shuffle read task for this partition
            let flight_shuffle_read_plan = LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                self.shuffle_id,
                partition_idx,
                server_cache_mapping.clone(),
                self.config.schema.clone(),
                StatsState::NotMaterialized,
            );

            // For flight shuffle, we create a task directly with the flight shuffle read plan
            // instead of using make_in_memory_task_from_materialized_outputs
            let task_context = TaskContext::from((&self.context, task_id_counter.next()));
            let task = SubmittableTask::new(SwordfishTask::new(
                task_context,
                flight_shuffle_read_plan,
                self.config.execution_config.clone(),
                std::collections::HashMap::new(), // No input partition sets needed for flight shuffle read
                crate::scheduling::task::SchedulingStrategy::Spread,
                self.context.to_hashmap(),
            ));

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }
}

impl PipelineNodeImpl for FlightShuffleNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let self_arc = self.clone();

        let partition_by = match &self.repartition_spec {
            RepartitionSpec::Hash(hash_spec) => Some(hash_spec.by.clone()),
            RepartitionSpec::Random(_) => None,
            RepartitionSpec::Range(_) => {
                unreachable!("Range repartition is not supported for flight shuffle")
            }
            RepartitionSpec::IntoPartitions(_) => {
                unreachable!("IntoPartitions repartition is not supported for flight shuffle")
            }
        };
        let local_flight_shuffle_write_node =
            input_node.pipeline_instruction(self.clone(), move |input| {
                LocalPhysicalPlan::flight_shuffle_write(
                    input,
                    partition_by.clone(),
                    self.num_partitions,
                    self.config.schema.clone(),
                    self.shuffle_id,
                    self.shuffle_dirs.clone(),
                    self.compression.clone(),
                    StatsState::NotMaterialized,
                )
            });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let flight_shuffle_execution = async move {
            self_arc
                .execution_loop(
                    local_flight_shuffle_write_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(flight_shuffle_execution);
        SubmittableTaskStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!(
            "FlightShuffle: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
