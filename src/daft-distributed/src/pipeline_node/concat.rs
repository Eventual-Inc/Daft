use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_logical_plan::{
    partitioning::{ClusteringSpecRef, UnknownClusteringConfig},
    ClusteringSpec,
};
use daft_schema::prelude::SchemaRef;
use futures::StreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        RunningPipelineNode,
    },
    stage::{StageConfig, StageExecutionContext},
    utils::channel::create_channel,
};

pub(crate) struct ConcatNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    child: Arc<dyn DistributedPipelineNode>,
    other: Arc<dyn DistributedPipelineNode>,
}

impl ConcatNode {
    const NODE_NAME: NodeName = "Concat";

    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        schema: SchemaRef,
        other: Arc<dyn DistributedPipelineNode>,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id(), other.node_id()],
            vec![child.name(), other.name()],
            logical_node_id,
        );

        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            ClusteringSpecRef::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                child.config().clustering_spec.num_partitions()
                    + other.config().clustering_spec.num_partitions(),
            ))),
        );

        Self {
            config,
            context,
            child,
            other,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["Concat".to_string()]
    }
}

impl TreeDisplay for ConcatNode {
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
        vec![self.child.as_tree_display(), self.other.as_tree_display()]
    }
}

impl DistributedPipelineNode for ConcatNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone(), self.other.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);
        let other_node = self.other.clone().start(stage_context);

        let (result_tx, result_rx) = create_channel(1);

        let execution_loop = async move {
            let mut chained = input_node.into_stream().chain(other_node.into_stream());
            while let Some(output) = chained.next().await {
                result_tx.send(output).await.unwrap();
            }
            Ok(())
        };

        stage_context.spawn(execution_loop);
        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
