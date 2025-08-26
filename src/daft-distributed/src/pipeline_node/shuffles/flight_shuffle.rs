use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        make_in_memory_task_from_materialized_outputs, DistributedPipelineNode, NodeID, NodeName,
        PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::{
        channel::{create_channel, Sender},
        transpose::transpose_materialized_outputs_from_stream,
    },
};

pub(crate) struct FlightShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl FlightShuffleNode {
    const NODE_NAME: NodeName = "FlightShuffle";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        repartition_spec: RepartitionSpec,
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
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            repartition_spec,
            num_partitions,
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
        res.push(format!("Num partitions: {}", self.num_partitions));
        res
    }

    // Flight shuffle execution loop - implements map-reduce pattern
    async fn execution_loop(
        self: Arc<Self>,
        flight_shuffle_write_stream: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Phase 1: Execute flight shuffle write tasks (map phase)
        // This writes data to local disk in arrow flight format
        let materialized_write_results =
            flight_shuffle_write_stream.materialize(scheduler_handle.clone());

        // Wait for all write tasks to complete (all data is now written to flight caches)
        let _write_outputs =
            transpose_materialized_outputs_from_stream(materialized_write_results, 1).await?;

        // Phase 2: Create flight shuffle read tasks (reduce phase)
        // Each task reads from all flight servers for its assigned partition

        // Generate shuffle_id from the task context/stage info
        let shuffle_id = format!("shuffle_{}", self.context.node_id);

        // TODO: Get actual node IPs from the cluster/scheduler
        // For now, we'll use an empty list and let the flight client discover nodes
        let node_ips: Vec<String> = vec![];

        for partition_idx in 0..self.num_partitions {
            let self_clone = self.clone();

            // Create a flight shuffle read task that will read partition_idx from all flight servers
            let task_plan = LocalPhysicalPlan::flight_shuffle_read(
                shuffle_id.clone(),
                partition_idx,
                node_ips.clone(),
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            );

            let task = SwordfishTask::new(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                task_plan,
                self_clone.config.execution_config.clone(),
                Default::default(),
                crate::scheduling::task::SchedulingStrategy::Spread,
                self.context.to_hashmap(),
            );

            let _ = result_tx.send(SubmittableTask::new(task)).await;
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

        // Create flight shuffle write tasks that write to local flight caches
        let self_clone = self.clone();
        let shuffle_id = format!("shuffle_{}", self.context.node_id);
        let flight_shuffle_write_stream =
            input_node.pipeline_instruction(self.clone(), move |input| {
                LocalPhysicalPlan::flight_shuffle_write(
                    input,
                    shuffle_id.clone(),
                    self_clone.repartition_spec.clone(),
                    self_clone.num_partitions,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                )
            });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

        let flight_shuffle_execution = async move {
            self_arc
                .execution_loop(
                    flight_shuffle_write_stream,
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
