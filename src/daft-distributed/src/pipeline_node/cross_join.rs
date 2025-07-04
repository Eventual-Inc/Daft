use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;
use itertools::Itertools;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineOutput,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct CrossJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    left: Arc<dyn DistributedPipelineNode>,
    right: Arc<dyn DistributedPipelineNode>,
}

impl CrossJoinNode {
    const NODE_NAME: NodeName = "CrossJoin";

    pub fn new(
        stage_config: &StageConfig,
        node_id: NodeID,
        left: Arc<dyn DistributedPipelineNode>,
        right: Arc<dyn DistributedPipelineNode>,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![left.node_id(), right.node_id()],
            vec![left.name(), right.name()],
        );
        let config = PipelineNodeConfig::new(output_schema, stage_config.config.clone());
        Self {
            config,
            context,
            left,
            right,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Cross Join".to_string()]
    }

    fn make_in_memory_scan_from_materialized_outputs(
        materialized_outputs: Vec<MaterializedOutput>,
        node: &Arc<dyn DistributedPipelineNode>,
    ) -> DaftResult<(LocalPhysicalPlanRef, Vec<PartitionRef>)> {
        let mut total_size_bytes = 0;
        let mut total_num_rows = 0;
        let mut partition_refs = vec![];

        let num_partitions = materialized_outputs.len();
        for materialized_output in materialized_outputs {
            let partition_ref = materialized_output.partition().clone();
            total_size_bytes += partition_ref.size_bytes()?.unwrap_or(0);
            total_num_rows += partition_ref.num_rows().unwrap_or(0);
            partition_refs.push(partition_ref);
        }

        let info = InMemoryInfo::new(
            node.config().schema.clone(),
            node.context().node_id.to_string(),
            None,
            num_partitions,
            total_size_bytes,
            total_num_rows,
            None,
            None,
        );

        Ok((
            LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized),
            partition_refs,
        ))
    }

    async fn execution_loop(
        self: Arc<Self>,
        left_repartition: RunningPipelineNode,
        right_repartition: RunningPipelineNode,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of the partitions
        let left_materialized = left_repartition
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;
        let right_materialized = right_repartition
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;

        // Make each pair of partition groups input into in-memory scans -> hash join
        for (left_out, right_out) in left_materialized
            .into_iter()
            .cartesian_product(right_materialized.into_iter())
        {
            if left_out
                .partition()
                .num_rows()
                .map(|rows| rows == 0)
                .unwrap_or(false)
                || right_out
                    .partition()
                    .num_rows()
                    .map(|rows| rows == 0)
                    .unwrap_or(false)
            {
                continue;
            }

            let (left_plan, left_partition_refs) =
                Self::make_in_memory_scan_from_materialized_outputs(vec![left_out], &self.left)?;
            let (right_plan, right_partition_refs) =
                Self::make_in_memory_scan_from_materialized_outputs(vec![right_out], &self.right)?;

            let plan = LocalPhysicalPlan::cross_join(
                left_plan,
                right_plan,
                self.config.schema.clone(),
                StatsState::NotMaterialized,
            );

            let psets = HashMap::from([
                (self.left.context().node_id.to_string(), left_partition_refs),
                (
                    self.right.context().node_id.to_string(),
                    right_partition_refs,
                ),
            ]);

            let task = SwordfishTask::new(
                TaskContext::from((&self.context, task_id_counter.next())),
                plan,
                self.config().execution_config.clone(),
                psets,
                SchedulingStrategy::Spread,
                self.context().to_hashmap(),
            );

            let task = SubmittableTask::new(task);
            let _ = result_tx.send(PipelineOutput::Task(task)).await;
        }

        Ok(())
    }
}

impl TreeDisplay for CrossJoinNode {
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
        vec![self.left.as_tree_display(), self.right.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for CrossJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let left_input = self.left.clone().start(stage_context);
        let right_input = self.right.clone().start(stage_context);

        // Materialize and cartesian product the outputs
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            left_input,
            right_input,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
