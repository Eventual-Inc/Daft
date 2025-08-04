use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, JoinType};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};

use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, DistributedPipelineNode, NodeID, NodeName,
        PipelineNodeConfig, PipelineNodeContext, SubmittableTaskStream,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct BroadcastJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,
    is_swapped: bool,

    broadcaster: Arc<dyn DistributedPipelineNode>,
    broadcaster_schema: SchemaRef,
    receiver: Arc<dyn DistributedPipelineNode>,
}

impl BroadcastJoinNode {
    const NODE_NAME: NodeName = "BroadcastJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        is_swapped: bool,
        broadcaster: Arc<dyn DistributedPipelineNode>,
        receiver: Arc<dyn DistributedPipelineNode>,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![broadcaster.node_id(), receiver.node_id()],
            vec![broadcaster.name(), receiver.name()],
            logical_node_id,
        );

        // For broadcast joins, we use the receiver's clustering spec since the broadcaster
        // will be gathered to all partitions
        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
            receiver.config().clustering_spec.clone(),
        );

        let broadcaster_schema = broadcaster.config().schema.clone();
        Self {
            config,
            context,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            is_swapped,
            broadcaster,
            broadcaster_schema,
            receiver,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["Broadcast Join".to_string()];
        res.push(format!(
            "Left on: {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right on: {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Join type: {}", self.join_type));
        res.push(format!("Is swapped: {}", self.is_swapped));
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals nulls: [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        res
    }

    async fn execution_loop(
        self: Arc<Self>,
        broadcaster_input: SubmittableTaskStream,
        mut receiver_input: SubmittableTaskStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_broadcast_data = broadcaster_input
            .materialize(scheduler_handle.clone())
            .try_collect::<Vec<_>>()
            .await?;
        let materialized_broadcast_data_plan = make_in_memory_scan_from_materialized_outputs(
            &materialized_broadcast_data,
            self.broadcaster_schema.clone(),
            self.node_id(),
        )?;
        let broadcast_psets = HashMap::from([(
            self.node_id().to_string(),
            materialized_broadcast_data
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>(),
        )]);
        while let Some(task) = receiver_input.next().await {
            let input_plan = task.task().plan();
            let (left_plan, right_plan) = if self.is_swapped {
                (input_plan, materialized_broadcast_data_plan.clone())
            } else {
                (materialized_broadcast_data_plan.clone(), input_plan)
            };
            let join_plan = LocalPhysicalPlan::hash_join(
                left_plan,
                right_plan,
                self.left_on.clone(),
                self.right_on.clone(),
                self.null_equals_nulls.clone(),
                self.join_type,
                self.config.schema.clone(),
                StatsState::NotMaterialized,
            );

            let mut psets = task.task().psets().clone();
            psets.extend(broadcast_psets.clone());

            let config = task.task().config().clone();

            let task = task.with_new_task(SwordfishTask::new(
                TaskContext::from((self.context(), task_id_counter.next())),
                join_plan,
                config,
                psets,
                SchedulingStrategy::Spread,
                self.context().to_hashmap(),
            ));
            if result_tx.send(task).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl TreeDisplay for BroadcastJoinNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![
            self.broadcaster.as_tree_display(),
            self.receiver.as_tree_display(),
        ]
    }

    fn get_name(&self) -> String {
        Self::NODE_NAME.to_string()
    }
}

impl DistributedPipelineNode for BroadcastJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.broadcaster.clone(), self.receiver.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let broadcaster_input = self.broadcaster.clone().produce_tasks(stage_context);
        let receiver_input = self.receiver.clone().produce_tasks(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            broadcaster_input,
            receiver_input,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
