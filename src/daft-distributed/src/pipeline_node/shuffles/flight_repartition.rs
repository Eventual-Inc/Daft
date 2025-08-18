use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct FlightRepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    shuffle_dirs: Vec<String>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl FlightRepartitionNode {
    const NODE_NAME: NodeName = "FlightRepartition";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_dirs: Vec<String>,
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
            shuffle_dirs,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("FlightRepartition: {}", self.repartition_spec.var_name())];
        res.extend(self.repartition_spec.multiline_display());
        res.push(format!("Shuffle dirs: {:?}", self.shuffle_dirs));
        res
    }

    // Flight repartition execution - use flight shuffle sink  
    async fn execution_loop(
        self: Arc<Self>,
        local_repartition_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Step 1: Create a flight shuffle sink task that pipelines to the input tasks
        let self_clone = self.clone();
        let flight_shuffle_sink_node = local_repartition_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::flight_shuffle_sink(
                input,
                self_clone.repartition_spec.clone(),
                self_clone.num_partitions,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
                self_clone.shuffle_dirs.clone(),
                format!("node_{}", self_clone.context.node_id),
                0, // shuffle_stage_id
            )
        });

        // Step 2: Materialize the flight shuffle sink tasks
        let materialized_stream = flight_shuffle_sink_node.materialize(scheduler_handle.clone());
        
        // Step 3: Collect materialized outputs (these should contain flight server info)
        let materialized_outputs: Vec<MaterializedOutput> = materialized_stream.try_collect().await?;
        
        // Step 4: For each materialized output, create flight shuffle source tasks
        // In a real implementation, we would extract server addresses and ports from the materialized outputs
        // For now, we'll create placeholder source tasks
        for (partition_idx, _materialized_output) in materialized_outputs.iter().enumerate() {
            if partition_idx < self.num_partitions {
                let self_clone = self.clone();
                let flight_source_plan = LocalPhysicalPlan::flight_shuffle_source(
                    "127.0.0.1".to_string(), // placeholder server address
                    8080,                    // placeholder port
                    partition_idx,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                );

                let task = crate::scheduling::task::SwordfishTask::new(
                    TaskContext::from((&self_clone.context, task_id_counter.next())),
                    flight_source_plan,
                    self_clone.config.execution_config.clone(),
                    std::collections::HashMap::new(), // empty psets
                    crate::scheduling::task::SchedulingStrategy::Spread,
                    std::collections::HashMap::new(), // empty context
                );
                
                let _ = result_tx.send(crate::pipeline_node::SubmittableTask::new(task)).await;
            }
        }

        Ok(())
    }
}

impl TreeDisplay for FlightRepartitionNode {
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

impl DistributedPipelineNode for FlightRepartitionNode {
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

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

        let flight_shuffle_execution = async move {
            self_arc
                .execution_loop(
                    input_node,
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