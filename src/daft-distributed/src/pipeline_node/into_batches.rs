use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    make_new_task_from_materialized_outputs, DistributedPipelineNode, SubmittableTaskStream,
};
use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct IntoBatchesNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    batch_size: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

// The threshold at which we will emit a batch of data to the next task.
// For instance, if the batch size is 100 and the threshold is 0.8, we will emit a batch
// of data to the next task once we have 80 rows of data.
// This is a heuristic to avoid creating batches that are too big. For instance, if we had
// materialized outputs from two partitions that of size 80, we would emit two batches of size 80
// instead of one batch of size 160.
const BATCH_SIZE_THRESHOLD: f64 = 0.8;

impl IntoBatchesNode {
    const NODE_NAME: NodeName = "IntoBatches";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        batch_size: usize,
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
            batch_size,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("IntoBatches: {}", self.batch_size)]
    }

    async fn execute_into_batches(
        self: Arc<Self>,
        input_node: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream = input_node.materialize(scheduler_handle.clone());

        let mut current_group: Vec<MaterializedOutput> = Vec::new();
        let mut current_group_size = 0;

        while let Some(mat) = materialized_stream.next().await {
            for mat in mat?.split_into_materialized_outputs() {
                let rows = mat.num_rows()?;
                if rows == 0 {
                    continue;
                }

                current_group.push(mat);
                current_group_size += rows;
                if current_group_size >= (self.batch_size as f64 * BATCH_SIZE_THRESHOLD) as usize {
                    let group_size = std::mem::take(&mut current_group_size);

                    let self_clone = self.clone();
                    let task = make_new_task_from_materialized_outputs(
                        TaskContext::from((&self_clone.context, task_id_counter.next())),
                        std::mem::take(&mut current_group),
                        &(self_clone.clone() as Arc<dyn DistributedPipelineNode>),
                        move |input| {
                            LocalPhysicalPlan::into_batches(
                                input,
                                group_size,
                                true, // Strict batch sizes for the downstream tasks, as they have been coalesced.
                                StatsState::NotMaterialized,
                            )
                        },
                    )?;
                    if result_tx.send(task).await.is_err() {
                        break;
                    }
                }
            }
        }

        if !current_group.is_empty() {
            let self_clone = self.clone();
            let task = make_new_task_from_materialized_outputs(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                current_group,
                &(self_clone.clone() as Arc<dyn DistributedPipelineNode>),
                move |input| {
                    LocalPhysicalPlan::into_batches(
                        input,
                        current_group_size,
                        true, // Strict batch sizes for the downstream tasks, as they have been coalesced.
                        StatsState::NotMaterialized,
                    )
                },
            )?;
            let _ = result_tx.send(task).await;
        }
        Ok(())
    }
}

impl TreeDisplay for IntoBatchesNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.name()).unwrap();
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
        self.name().to_string()
    }
}

impl DistributedPipelineNode for IntoBatchesNode {
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
        let self_clone = self.clone();
        let local_into_batches_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::into_batches(
                input,
                self_clone.batch_size,
                false, // No need strict batch sizes for the child tasks, as we coalesce them later on.
                StatsState::NotMaterialized,
            )
        });

        let (result_tx, result_rx) = create_channel(1);
        let execution_future = self.execute_into_batches(
            local_into_batches_node,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_future);

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
