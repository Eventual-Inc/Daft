use std::{cmp::max, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{
    partitioning::HashClusteringConfig, stats::StatsState, ClusteringSpec, JoinType,
};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        SubmittableTaskStream,
    },
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
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

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let left_input = self.left.clone().produce_tasks(stage_context);
        let right_input = self.right.clone().produce_tasks(stage_context);
        let task_id_counter = stage_context.task_id_counter();

        SubmittableTaskStream::new(
            left_input
                .zip(right_input)
                .map(move |(left_task, right_task)| {
                    self.build_hash_join_task(left_task, right_task, &task_id_counter)
                })
                .boxed(),
        )
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

pub(crate) fn gen_num_partitions(
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
