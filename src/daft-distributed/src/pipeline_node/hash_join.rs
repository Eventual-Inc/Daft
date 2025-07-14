use std::{cmp::max, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::{expr::bound_expr::BoundExpr, join::normalize_join_keys};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{
    ops::join::JoinPredicate, partitioning::HashClusteringConfig, stats::StatsState,
    ClusteringSpec, JoinType,
};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, repartition::RepartitionNode,
        translate::LogicalPlanToPipelineNodeTranslator, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineOutput,
    },
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct HashJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

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
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        num_partitions: usize,
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
            logical_node_id,
        );
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

    fn make_in_memory_task(
        self: Arc<Self>,
        input: PipelineOutput<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        match input {
            PipelineOutput::Running(_) => {
                unreachable!("All running tasks should be materialized before this point")
            }
            PipelineOutput::Materialized(materialized_output) => {
                make_in_memory_scan_from_materialized_outputs(
                    TaskContext::from((self.context(), task_id_counter.next())),
                    vec![materialized_output],
                    &(self as Arc<dyn DistributedPipelineNode>),
                )
            }
            PipelineOutput::Task(task) => Ok(task),
        }
    }

    fn build_hash_join_task(
        &self,
        left_task: SubmittableTask<SwordfishTask>,
        right_task: SubmittableTask<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
    ) -> SubmittableTask<SwordfishTask> {
        let left_plan = left_task.task().plan();
        let right_plan = right_task.task().plan();
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

        let mut psets = left_task.task().psets().clone();
        psets.extend(right_task.task().psets().clone());

        let config = left_task.task().config().clone();

        left_task.with_new_task(SwordfishTask::new(
            TaskContext::from((self.context(), task_id_counter.next())),
            plan,
            config,
            psets,
            SchedulingStrategy::Spread,
            self.context().to_hashmap(),
        ))
    }

    async fn execution_loop(
        self: Arc<Self>,
        left_input: RunningPipelineNode,
        right_input: RunningPipelineNode,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        // Materialize both sides of the join in parallel
        let left_transposed = left_input.materialize_running();
        let right_transposed = right_input.materialize_running();
        let mut outputs = left_transposed.zip(right_transposed);

        // Make each pair of partition groups input into in-memory scans -> hash join
        while let Some((left_group, right_group)) = outputs.next().await {
            let left_task = self
                .clone()
                .make_in_memory_task(left_group?, &task_id_counter)?;
            let right_task = self
                .clone()
                .make_in_memory_task(right_group?, &task_id_counter)?;

            let task = self.build_hash_join_task(left_task, right_task, &task_id_counter);
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

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            left_input,
            right_input,
            stage_context.task_id_counter(),
            result_tx,
        );
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

fn gen_num_partitions(
    left_spec: &ClusteringSpec,
    right_spec: &ClusteringSpec,
    cfg: &DaftExecutionConfig,
) -> usize {
    let is_left_hash_partitioned = matches!(left_spec, ClusteringSpec::Hash(_));
    let is_right_hash_partitioned = matches!(right_spec, ClusteringSpec::Hash(_));
    let num_left_partitions = left_spec.num_partitions();
    let num_right_partitions = right_spec.num_partitions();

    match (
        is_left_hash_partitioned,
        is_right_hash_partitioned,
        num_left_partitions,
        num_right_partitions,
    ) {
        (true, true, a, b) | (false, false, a, b) => max(a, b),
        (_, _, 1, x) | (_, _, x, 1) => x,
        (true, false, a, b) if (a as f64) >= (b as f64) * cfg.hash_join_partition_size_leniency => {
            a
        }
        (false, true, a, b) if (b as f64) >= (a as f64) * cfg.hash_join_partition_size_leniency => {
            b
        }
        (_, _, a, b) => max(a, b),
    }
}

impl LogicalPlanToPipelineNodeTranslator {
    pub(crate) fn gen_hash_join_nodes(
        &mut self,
        logical_node_id: Option<NodeID>,
        join_on: JoinPredicate,

        left: Arc<dyn DistributedPipelineNode>,
        right: Arc<dyn DistributedPipelineNode>,

        join_type: JoinType,
        output_schema: SchemaRef,
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        let (_, left_on, right_on, null_equals_nulls) = join_on.split_eq_preds();

        let (left_on, right_on) = normalize_join_keys(
            left_on,
            right_on,
            left.config().schema.clone(),
            right.config().schema.clone(),
        )?;
        let left_on = BoundExpr::bind_all(&left_on, &left.config().schema)?;
        let right_on = BoundExpr::bind_all(&right_on, &right.config().schema)?;

        let num_partitions = gen_num_partitions(
            left.config().clustering_spec.as_ref(),
            right.config().clustering_spec.as_ref(),
            self.stage_config.config.as_ref(),
        );

        let left = RepartitionNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            left_on.clone(),
            Some(num_partitions),
            left.config().schema.clone(),
            left,
        )
        .arced();

        let right = RepartitionNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            right_on.clone(),
            Some(num_partitions),
            right.config().schema.clone(),
            right,
        )
        .arced();

        Ok(HashJoinNode::new(
            self.get_next_pipeline_node_id(),
            logical_node_id,
            &self.stage_config,
            left_on,
            right_on,
            Some(null_equals_nulls),
            join_type,
            num_partitions,
            left,
            right,
            output_schema,
        )
        .arced())
    }
}
