use std::sync::Arc;

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl,
};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, Sender, create_channel},
    pipeline::{MorselSizeRequirement, PipelineNode, RuntimeContext},
    pipeline_message::PipelineMessage,
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub struct ConcatNode {
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

impl ConcatNode {
    pub(crate) fn new(
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = "Concat".into();
        let node_info =
            ctx.next_node_info(name, NodeType::Concat, NodeCategory::Intermediate, context);
        let runtime_stats = Arc::new(DefaultRuntimeStats::new(node_info.id));
        let morsel_size_requirement = MorselSizeRequirement::default();

        Self {
            left,
            right,
            runtime_stats,
            plan_stats,
            morsel_size_requirement,
            node_info: Arc::new(node_info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    async fn process_child(
        node_id: usize,
        mut receiver: Receiver<PipelineMessage>,
        sender: Sender<PipelineMessage>,
        runtime_stats: Arc<dyn RuntimeStats>,
        stats_manager: &RuntimeStatsManagerHandle,
        node_initialized: &mut bool,
    ) -> DaftResult<Option<bool>> {
        // Returns Some(true) if flush received, Some(false) if receiver closed, None if error
        while let Some(msg) = receiver.recv().await {
            match msg {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    // Only process messages with input_id = 0
                    if input_id != 0 {
                        continue;
                    }
                    if !*node_initialized {
                        stats_manager.activate_node(node_id);
                        *node_initialized = true;
                    }
                    runtime_stats.add_rows_in(partition.len() as u64);
                    runtime_stats.add_rows_out(partition.len() as u64);
                    if sender
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(None);
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    // Assert that flush is for input_id = 0
                    assert_eq!(
                        input_id, 0,
                        "Concat only supports input_id = 0, got flush for input_id = {}",
                        input_id
                    );
                    // Exit processing this child
                    return Ok(Some(true));
                }
            }
        }

        // Receiver closed
        Ok(Some(false))
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
                if matches!(level, DisplayLevel::Verbose) {
                    writeln!(display).unwrap();
                    let rt_result = self.runtime_stats.snapshot();
                    for (name, value) in rt_result.to_stats() {
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
            "category": "Intermediate",
            "type": NodeType::Concat.to_string(),
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
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.left, &self.right]
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
        self.left
            .propagate_morsel_size_requirement(downstream_requirement, default_requirement);
        self.right
            .propagate_morsel_size_requirement(downstream_requirement, default_requirement);
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let left_receiver = self.left.start(maintain_order, runtime_handle)?;
        let right_receiver = self.right.start(maintain_order, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();
        let left_sender = destination_sender.clone();
        let right_sender = destination_sender;

        runtime_handle.spawn(
            async move {
                // Process both children sequentially - first left, then right
                let mut node_initialized = false;
                let destination_sender_for_flush = left_sender.clone();

                // Process first child (left)
                let left_result = Self::process_child(
                    node_id,
                    left_receiver,
                    left_sender,
                    runtime_stats.clone(),
                    &stats_manager,
                    &mut node_initialized,
                )
                .await?;

                // If left child processing failed, exit
                if left_result.is_none() {
                    stats_manager.finalize_node(node_id);
                    return Ok(());
                }

                // Process second child (right)
                let right_result = Self::process_child(
                    node_id,
                    right_receiver,
                    right_sender,
                    runtime_stats.clone(),
                    &stats_manager,
                    &mut node_initialized,
                )
                .await?;

                // If right child processing failed, exit
                if right_result.is_none() {
                    stats_manager.finalize_node(node_id);
                    return Ok(());
                }

                // Send flush for input_id 0 once either:
                // - The flush comes from second child (right_result == Some(true)), OR
                // - If second child receiver is closed (right_result == Some(false))
                if right_result.is_some()
                    && destination_sender_for_flush
                        .send(PipelineMessage::Flush(0))
                        .await
                        .is_err()
                {
                    stats_manager.finalize_node(node_id);
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
