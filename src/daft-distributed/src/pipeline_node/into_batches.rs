use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct IntoBatchesNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    batch_size: usize,
    child: DistributedPipelineNode,
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
        plan_config: &PlanConfig,
        batch_size: usize,
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
            Arc::new(
                UnknownClusteringConfig::new(
                    child.config().clustering_spec.num_partitions().max(2),
                )
                .into(),
            ),
        );
        Self {
            config,
            context,
            batch_size,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    async fn execute_into_batches(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream =
            input_node.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        let mut current_group: Vec<MaterializedOutput> = Vec::new();
        let mut current_group_size = 0;

        while let Some(mat) = materialized_stream.next().await {
            for mat in mat?.split_into_materialized_outputs() {
                let rows = mat.num_rows();
                if rows == 0 {
                    continue;
                }

                current_group.push(mat);
                current_group_size += rows;
                if current_group_size >= (self.batch_size as f64 * BATCH_SIZE_THRESHOLD) as usize {
                    let group_size = std::mem::take(&mut current_group_size);

                    let materialized_outputs = std::mem::take(&mut current_group);
                    let (in_memory_scan, psets) =
                        MaterializedOutput::into_in_memory_scan_with_psets(
                            materialized_outputs,
                            self.config.schema.clone(),
                            self.node_id(),
                        );
                    let plan = LocalPhysicalPlan::into_batches(
                        in_memory_scan,
                        group_size,
                        true, // Strict batch sizes for the downstream tasks, as they have been coalesced.
                        StatsState::NotMaterialized,
                        LocalNodeContext {
                            origin_node_id: Some(self.node_id() as usize),
                            additional: None,
                        },
                    );
                    let builder = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);
                    if result_tx.send(builder).await.is_err() {
                        break;
                    }
                }
            }
        }

        if !current_group.is_empty() {
            let (in_memory_source_plan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                current_group,
                self.config.schema.clone(),
                self.node_id(),
            );
            let plan = LocalPhysicalPlan::into_batches(
                in_memory_source_plan,
                current_group_size,
                true, // Strict batch sizes for the downstream tasks, as they have been coalesced.
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(self.node_id() as usize),
                    additional: None,
                },
            );
            let builder = SwordfishTaskBuilder::new(plan, self.as_ref()).with_psets(psets);
            let _ = result_tx.send(builder).await;
        }
        Ok(())
    }
}

impl PipelineNodeImpl for IntoBatchesNode {
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
        vec![format!("IntoBatches: {}", self.batch_size)]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let node_id = self.node_id();
        let batch_size = self.batch_size;
        let local_into_batches_node = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::into_batches(
                input,
                batch_size,
                false, // No need strict batch sizes for the child tasks, as we coalesce them later on.
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        });

        let (result_tx, result_rx) = create_channel(1);
        let execution_future = self.execute_into_batches(
            local_into_batches_node,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_future);

        TaskBuilderStream::from(result_rx)
    }
}
