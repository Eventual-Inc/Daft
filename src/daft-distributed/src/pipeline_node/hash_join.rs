use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    partitioning::HashClusteringConfig, stats::StatsState, InMemoryInfo, JoinType,
};
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{
        repartition::RepartitionNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineOutput,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct HashJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Shuffle properties
    num_partitions: usize,
    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,

    left: Arc<dyn DistributedPipelineNode>,
    right: Arc<dyn DistributedPipelineNode>,
}

impl HashJoinNode {
    const NODE_NAME: NodeName = "HashJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stage_config: &StageConfig,
        node_id: NodeID,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
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
        let num_partitions = left.config().clustering_spec.num_partitions();
        let partition_cols = left_on
            .iter()
            .chain(right_on.iter())
            .map(BoundExpr::inner)
            .cloned()
            .collect::<Vec<_>>();
        let config = PipelineNodeConfig::new(
            output_schema,
            stage_config.config.clone(),
            Arc::new(HashClusteringConfig::new(num_partitions, partition_cols).into()),
        );
        Self {
            config,
            context,
            num_partitions,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            left,
            right,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["Hash Join".to_string()];
        res.push(format!(
            "Left on: {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right on: {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res
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
            total_size_bytes += materialized_output.size_bytes()?;
            total_num_rows += materialized_output.num_rows()?;
            let partition_ref = materialized_output.partitions();
            partition_refs.extend_from_slice(partition_ref);
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
        let left_transposed = RepartitionNode::transpose_materialized_outputs(
            left_repartition.materialize(scheduler_handle.clone()),
            self.num_partitions,
        )
        .await?;

        // Same for the the right
        let right_transposed = RepartitionNode::transpose_materialized_outputs(
            right_repartition.materialize(scheduler_handle.clone()),
            self.num_partitions,
        )
        .await?;

        // Make each pair of partition groups input into in-memory scans -> hash join
        for (left_group, right_group) in left_transposed
            .into_iter()
            .zip(right_transposed.into_iter())
        {
            let (left_plan, left_partition_refs) =
                Self::make_in_memory_scan_from_materialized_outputs(left_group, &self.left)?;
            let (right_plan, right_partition_refs) =
                Self::make_in_memory_scan_from_materialized_outputs(right_group, &self.right)?;

            let plan = LocalPhysicalPlan::hash_join(
                left_plan,
                right_plan,
                self.left_on.clone(),
                self.right_on.clone(),
                self.null_equals_nulls.clone(),
                self.join_type,
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

impl TreeDisplay for HashJoinNode {
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

impl DistributedPipelineNode for HashJoinNode {
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

        // First pipeline a repartition to both
        let self_clone = self.clone();
        let left_repartition =
            left_input.pipeline_instruction(stage_context, self.clone(), move |input| {
                Ok(LocalPhysicalPlan::repartition(
                    input,
                    self_clone.left_on.clone(),
                    self_clone.num_partitions,
                    self_clone.left.config().schema.clone(),
                    StatsState::NotMaterialized,
                ))
            });
        let self_clone = self.clone();
        let right_repartition =
            right_input.pipeline_instruction(stage_context, self.clone(), move |input| {
                Ok(LocalPhysicalPlan::repartition(
                    input,
                    self_clone.right_on.clone(),
                    self_clone.num_partitions,
                    self_clone.right.config().schema.clone(),
                    StatsState::NotMaterialized,
                ))
            });

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            left_repartition,
            right_repartition,
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
