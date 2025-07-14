use std::sync::{atomic::AtomicU64, Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        RunningPipelineNode,
    },
    stage::{StageConfig, StageExecutionContext},
};

pub(crate) struct MonotonicallyIncreasingIdNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    column_name: String,
    child: Arc<dyn DistributedPipelineNode>,
}

impl MonotonicallyIncreasingIdNode {
    const NODE_NAME: NodeName = "MonotonicallyIncreasingId";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        column_name: String,
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
            column_name,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("MonotonicallyIncreasingId: {}", self.column_name)]
    }
}

impl TreeDisplay for MonotonicallyIncreasingIdNode {
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

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

/// The maximum number of rows per partition for the monotonically increasing ID node.
/// https://docs.getdaft.io/en/stable/api/functions/#daft.functions.monotonically_increasing_id
///
/// The implementation puts the partition number in the upper 28 bits, and the row number in each
/// partition in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and
/// 2^36 ≈ 68 billion rows per partition.
const MAX_ROWS_PER_PARTITION: u64 = 1u64 << 36;

impl DistributedPipelineNode for MonotonicallyIncreasingIdNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);
        let column_name = self.column_name.clone();
        let schema = self.config.schema.clone();

        let next_starting_offset = AtomicU64::new(0);

        input_node.pipeline_instruction(stage_context, self, move |input| {
            Ok(LocalPhysicalPlan::monotonically_increasing_id(
                input,
                column_name.clone(),
                Some(
                    next_starting_offset
                        .fetch_add(MAX_ROWS_PER_PARTITION, std::sync::atomic::Ordering::SeqCst),
                ),
                schema.clone(),
                StatsState::NotMaterialized,
            ))
        })
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
