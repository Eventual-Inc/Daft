//! Fixed-point hash join: distributed orchestration that composes ordinary `hash_join` tasks each
//! round until global convergence (TODO). Join implementations remain unaware of iteration.

use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{JoinType, partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream, join::stats::BasicJoinStats,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::RuntimeStatsRef,
    utils::channel::{Sender, create_channel},
};

pub(crate) struct IterativeHashJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,
    /// Bound on join output schema; used when multi-round execution computes global hash sums (TODO).
    #[allow(dead_code)]
    signature: Vec<BoundExpr>,
    #[allow(dead_code)]
    max_iterations: Option<usize>,
    left: DistributedPipelineNode,
    right: DistributedPipelineNode,
}

impl IterativeHashJoinNode {
    const NODE_NAME: &'static str = "IterativeHashJoin";

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
        signature: Vec<BoundExpr>,
        max_iterations: Option<usize>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::IterativeHashJoin,
            NodeCategory::BlockingSink,
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
            signature,
            max_iterations,
            left,
            right,
        }
    }

    /// Template: forwards one round of hash-join task builders (same shape as `HashJoinNode`).
    /// Extend with materialized frontier, global hash-sum aggregation, and iteration until stable.
    async fn execution_loop(
        self: Arc<Self>,
        left_input: TaskBuilderStream,
        right_input: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let mut zipped = left_input.zip(right_input);
        while let Some((left_task, right_task)) = zipped.next().await {
            let builder = SwordfishTaskBuilder::combine_with(
                &left_task,
                &right_task,
                self.as_ref(),
                |left_plan, right_plan| {
                    LocalPhysicalPlan::hash_join(
                        left_plan,
                        right_plan,
                        self.left_on.clone(),
                        self.right_on.clone(),
                        None,
                        self.null_equals_nulls.clone(),
                        self.join_type,
                        self.config.schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.node_id() as usize)),
                    )
                },
            );
            result_tx.send(builder).await.map_err(|_| {
                common_error::DaftError::InternalError(
                    "IterativeHashJoin: downstream channel closed".to_string(),
                )
            })?;
        }
        Ok(())
    }
}

impl PipelineNodeImpl for IterativeHashJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(BasicJoinStats::new(meter, self.context()))
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["IterativeHashJoin".to_string()];
        res.push(format!("Type: {}", self.join_type));
        res.push(format!(
            "Left key: {}",
            self.left_on.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Right key: {}",
            self.right_on.iter().map(|e| e.to_string()).join(", ")
        ));
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals nulls: [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        res.push(format!(
            "Signature columns: {}",
            self.signature.iter().map(|e| e.to_string()).join(", ")
        ));
        if let Some(m) = self.max_iterations {
            res.push(format!("Max iterations: {m}"));
        }
        res.push("Note: emits a single join round; iteration + global hash-sum TBD.".to_string());
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let left_input = self.left.clone().produce_tasks(plan_context);
        let right_input = self.right.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);
        plan_context.spawn(async move {
            self.execution_loop(left_input, right_input, result_tx)
                .await
        });
        TaskBuilderStream::from(result_rx)
    }
}
