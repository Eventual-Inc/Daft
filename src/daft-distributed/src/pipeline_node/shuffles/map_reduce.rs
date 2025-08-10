use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::{
    pipeline_node::{
        make_in_memory_task_from_materialized_outputs, DistributedPipelineNode, MaterializedOutput,
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

#[derive(Clone)]
pub(crate) struct MapReduceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl MapReduceNode {
    const NODE_NAME: NodeName = "MapReduce";

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
            child.config().clustering_spec.clone(),
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
        vec![
            format!("Repartition: {}", self.repartition_spec.var_name()),
            self.repartition_spec.multiline_display().join("\n"),
        ]
    }

    pub(crate) async fn transpose_materialized_outputs(
        materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
    ) -> DaftResult<Vec<Vec<MaterializedOutput>>> {
        let materialized_partitions = materialized_stream
            .map(|mat| mat.map(|mat| mat.split_into_materialized_outputs()))
            .try_collect::<Vec<_>>()
            .await?;

        debug_assert!(
            materialized_partitions
                .iter()
                .all(|mat| mat.len() == num_partitions),
            "Expected all outputs to have {} partitions, got {}",
            num_partitions,
            materialized_partitions
                .iter()
                .map(|mat| mat.len())
                .join(", ")
        );

        let mut transposed_outputs = vec![];
        for idx in 0..num_partitions {
            let mut partition_group = vec![];
            for materialized_partition in &materialized_partitions {
                let part = &materialized_partition[idx];
                if part.num_rows()? > 0 {
                    partition_group.push(part.clone());
                }
            }
            transposed_outputs.push(partition_group);
        }

        assert_eq!(transposed_outputs.len(), num_partitions);
        Ok(transposed_outputs)
    }

    // Async execution to get all partitions out
    async fn execution_loop(
        self: Arc<Self>,
        local_repartition_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of the partitions
        // This will produce a stream of materialized outputs, each containing a vector of num_partitions partitions
        let materialized_stream = local_repartition_node.materialize(scheduler_handle.clone());
        let transposed_outputs =
            Self::transpose_materialized_outputs(materialized_stream, self.num_partitions).await?;

        // Make each partition group (partitions equal by (hash % num_partitions)) input to a in-memory scan
        for partition_group in transposed_outputs {
            let self_clone = self.clone();
            let task = make_in_memory_task_from_materialized_outputs(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                partition_group,
                &(self_clone as Arc<dyn DistributedPipelineNode>),
            )?;

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }
}

impl TreeDisplay for MapReduceNode {
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

impl DistributedPipelineNode for MapReduceNode {
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

        // First pipeline the local repartition op
        let self_clone = self.clone();
        let local_repartition_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition(
                input,
                self_clone.repartition_spec.clone(),
                self_clone.num_partitions,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
            )
        });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = stage_context.task_id_counter();
        let scheduler_handle = stage_context.scheduler_handle();

        let map_reduce_execution = async move {
            self_arc
                .execution_loop(
                    local_repartition_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        stage_context.spawn(map_reduce_execution);
        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
