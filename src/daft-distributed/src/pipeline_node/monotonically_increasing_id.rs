use std::sync::{Arc, atomic::AtomicU64};

use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, SubmittableTaskStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
};

pub(crate) struct MonotonicallyIncreasingIdNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    column_name: String,
    child: DistributedPipelineNode,
}

impl MonotonicallyIncreasingIdNode {
    const NODE_NAME: NodeName = "MonotonicallyIncreasingId";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        column_name: String,
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
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            column_name,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

/// The maximum number of rows per partition for the monotonically increasing ID node.
/// https://docs.getdaft.io/en/stable/api/functions/#daft.functions.monotonically_increasing_id
///
/// The implementation puts the partition number in the upper 28 bits, and the row number in each
/// partition in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and
/// 2^36 ≈ 68 billion rows per partition.
const MAX_ROWS_PER_PARTITION: u64 = 1u64 << 36;

impl PipelineNodeImpl for MonotonicallyIncreasingIdNode {
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
        vec![format!("MonotonicallyIncreasingId: {}", self.column_name)]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let column_name = self.column_name.clone();
        let schema = self.config.schema.clone();

        let next_starting_offset = AtomicU64::new(0);
        let node_id = self.node_id();

        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::monotonically_increasing_id(
                input,
                column_name.clone(),
                Some(
                    next_starting_offset
                        .fetch_add(MAX_ROWS_PER_PARTITION, std::sync::atomic::Ordering::SeqCst),
                ),
                schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        })
    }
}
