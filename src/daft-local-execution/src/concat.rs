use std::sync::Arc;

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use tracing::instrument;

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{MorselSizeRequirement, PipelineNode, RuntimeContext},
    plan_input::PipelineMessage,
    runtime_stats::{
        CountingSender, DefaultRuntimeStats, InitializingCountingReceiver, RuntimeStats,
    },
};

pub struct ConcatNode {
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

impl ConcatNode {
    pub(crate) fn new(
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
        context: &LocalNodeContext,
    ) -> Self {
        assert_eq!(children.len(), 2, "ConcatNode must have exactly 2 children");
        let name: Arc<str> = "Concat".into();
        let node_info = ctx.next_node_info(
            name,
            NodeType::Concat,
            NodeCategory::Concat,
            output_schema,
            context,
        );
        let runtime_stats = Arc::new(DefaultRuntimeStats::new(node_info.id));

        let morsel_size_requirement = MorselSizeRequirement::default();
        Self {
            children,
            runtime_stats,
            plan_stats,
            node_info: Arc::new(node_info),
            morsel_size_requirement,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for ConcatNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "Concat").unwrap();
            }
            level => {
                writeln!(display, "Concat").unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
                if matches!(level, DisplayLevel::Verbose) {
                    let rt_result = self.runtime_stats.snapshot();
                    for (name, value) in rt_result {
                        writeln!(display, "{} = {}", name.as_ref().capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        let children: Vec<serde_json::Value> = self
            .get_children()
            .iter()
            .map(|child| child.repr_json())
            .collect();

        serde_json::json!({
            "id": self.node_id(),
            "category": "Concat",
            "type": "Concat",
            "name": self.name(),
            "children": children,
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl PipelineNode for ConcatNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        self.children.iter().collect()
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    ) {
        self.morsel_size_requirement = downstream_requirement;
        for child in &mut self.children {
            child.propagate_morsel_size_requirement(downstream_requirement, default_requirement);
        }
    }

    #[instrument(level = "info", skip_all, name = "ConcatNode::start")]
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        // Start first child
        let first_child_receiver: Receiver<PipelineMessage> =
            self.children[0].start(maintain_order, runtime_handle)?;
        let mut first_counting_receiver = InitializingCountingReceiver::new(
            first_child_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        // Start second child
        let second_child_receiver: Receiver<PipelineMessage> =
            self.children[1].start(maintain_order, runtime_handle)?;
        let mut second_counting_receiver = InitializingCountingReceiver::new(
            second_child_receiver,
            self.node_id(),
            self.runtime_stats.clone(),
            runtime_handle.stats_manager(),
        );

        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();

        // Consume first receiver entirely, then second receiver entirely
        runtime_handle.spawn(
            async move {
                // First, consume entire first receiver
                while let Some(msg) = first_counting_receiver.recv().await {
                    match &msg {
                        PipelineMessage::Morsel { input_id, .. } => {
                            debug_assert_eq!(*input_id, 0, "Concat should only see input_id = 0");
                            if counting_sender.send(msg).await.is_err() {
                                return Ok(());
                            }
                        }
                        PipelineMessage::Flush(input_id) => {
                            // Assert that we only ever see input_id = 0
                            debug_assert_eq!(*input_id, 0, "Concat should only see input_id = 0");
                            // Don't send flush yet, wait until second receiver is done
                            break;
                        }
                    }
                }

                // Then, consume entire second receiver
                while let Some(msg) = second_counting_receiver.recv().await {
                    match &msg {
                        PipelineMessage::Morsel { input_id, .. } => {
                            debug_assert_eq!(*input_id, 0, "Concat should only see input_id = 0");
                            if counting_sender.send(msg).await.is_err() {
                                return Ok(());
                            }
                        }
                        PipelineMessage::Flush(input_id) => {
                            // Assert that we only ever see input_id = 0
                            debug_assert_eq!(*input_id, 0, "Concat should only see input_id = 0");
                            // Don't send flush yet, wait until we're done
                            break;
                        }
                    }
                }

                // Call flush once after second receiver is through
                if counting_sender
                    .send(PipelineMessage::Flush(0))
                    .await
                    .is_err()
                {
                    return Ok(());
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );

        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn plan_id(&self) -> Arc<str> {
        Arc::from(self.node_info.context.get("plan_id").unwrap().clone())
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }

    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.runtime_stats.clone()
    }
}
