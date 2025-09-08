use std::{collections::HashSet, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    rand::random::<u64>()
}

pub(crate) struct FlightShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    shuffle_id: u64,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl FlightShuffleNode {
    const NODE_NAME: NodeName = "FlightShuffle";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
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
        let shuffle_id = make_shuffle_id(&context);
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
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

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "FlightShuffle: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res.push(format!("Shuffle ID: {}", self.shuffle_id));
        res
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

        let server_addresses = outputs
            .iter()
            .map(|output| output.ip_address().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        println!("shuffle id: {}, num server addresses: {}", self.shuffle_id, server_addresses.len());

        // For each partition group, create tasks that read from flight servers
        for partition_idx in 0..self.num_partitions {
            // Create a flight shuffle read task for this partition
            let self_clone = self.clone();
            let flight_shuffle_read_plan = LocalPhysicalPlan::flight_shuffle_read(
                self_clone.shuffle_id,
                partition_idx,
                server_addresses.clone(),
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            );

            // For flight shuffle, we create a task directly with the flight shuffle read plan
            // instead of using make_in_memory_task_from_materialized_outputs
            let task_context = TaskContext::from((&self_clone.context, task_id_counter.next()));
            let task = SubmittableTask::new(SwordfishTask::new(
                task_context,
                flight_shuffle_read_plan,
                self_clone.config.execution_config.clone(),
                std::collections::HashMap::new(), // No input partition sets needed for flight shuffle read
                crate::scheduling::task::SchedulingStrategy::Spread,
                self_clone.context.to_hashmap(),
            ));

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }
}

impl TreeDisplay for FlightShuffleNode {
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

impl DistributedPipelineNode for FlightShuffleNode {
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

        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

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

        stage_context.spawn(flight_shuffle_execution);
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
