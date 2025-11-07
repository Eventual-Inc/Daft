use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{JoinType, partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
};

pub(crate) struct HashJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,

    left: DistributedPipelineNode,
    right: DistributedPipelineNode,
}

impl HashJoinNode {
    const NODE_NAME: NodeName = "HashJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        num_partitions: usize,
        left: DistributedPipelineNode,
        right: DistributedPipelineNode,
        output_schema: SchemaRef,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let partition_cols = left_on
            .iter()
            .chain(right_on.iter())
            .map(BoundExpr::inner)
            .cloned()
            .collect::<Vec<_>>();
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
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

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
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
            None,
            self.null_equals_nulls.clone(),
            self.join_type,
            self.config.schema.clone(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
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

impl PipelineNodeImpl for HashJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
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

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let left_input = self.left.clone().produce_tasks(plan_context);
        let right_input = self.right.clone().produce_tasks(plan_context);
        let task_id_counter = plan_context.task_id_counter();

        SubmittableTaskStream::new(
            left_input
                .zip(right_input)
                .map(move |(left_task, right_task)| {
                    self.build_hash_join_task(left_task, right_task, &task_id_counter)
                })
                .boxed(),
        )
    }
}
