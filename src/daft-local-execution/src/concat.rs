use std::{ops::ControlFlow, sync::Arc};

use daft_common::display::tree::TreeDisplay;
use daft_common::error::DaftResult;
use daft_common::metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, Sender, create_channel},
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineMessage, PipelineNode},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats, RuntimeStatsManagerHandle},
};

pub struct ConcatNode {
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    meter: Meter,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

impl ConcatNode {
    pub(crate) fn new(
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = "Concat".into();
        let node_info =
            ctx.next_node_info(name, NodeType::Concat, NodeCategory::Intermediate, context);
        let morsel_size_requirement = MorselSizeRequirement::default();

        Self {
            left,
            right,
            meter: ctx.meter.clone(),
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
    ) -> DaftResult<ControlFlow<()>> {
        while let Some(msg) = receiver.recv().await {
            if !*node_initialized {
                stats_manager.activate_node(node_id);
                *node_initialized = true;
            }
            match msg {
                PipelineMessage::Morsel {
                    partition,
                    input_id,
                } => {
                    assert_eq!(
                        input_id, 0,
                        "Concat only supports input_id = 0, got input_id = {}",
                        input_id
                    );
                    runtime_stats.add_rows_in(partition.len() as u64);
                    runtime_stats.add_rows_out(partition.len() as u64);
                    if sender
                        .send(PipelineMessage::Morsel {
                            partition,
                            input_id: 0,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                PipelineMessage::Flush(input_id) => {
                    assert_eq!(
                        input_id, 0,
                        "Concat only supports input_id = 0, got input_id = {}",
                        input_id
                    );
                    break;
                }
                PipelineMessage::FlightPartitionRef { .. } => {
                    unreachable!("ConcatNode should not receive flight partition refs from child")
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    }
}

impl TreeDisplay for ConcatNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: daft_common::display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use daft_common::display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "Concat").unwrap();
            }
            _ => {
                writeln!(display, "Concat").unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
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

        let mut json = serde_json::json!({
            "id": self.node_id(),
            "category": "StreamingSink",
            "type": NodeType::Concat.to_string(),
            "name": self.name(),
            "children": children,
        });

        if let StatsState::Materialized(stats) = &self.plan_stats {
            json["approx_stats"] = serde_json::json!(stats);
        }

        json
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
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();

        let left_receiver = self.left.start(maintain_order, runtime_handle)?;
        let right_receiver = self.right.start(maintain_order, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        let stats_manager = runtime_handle.stats_manager();
        let meter = self.meter.clone();
        let node_info = self.node_info.clone();

        runtime_handle.spawn(
            async move {
                let runtime_stats: Arc<dyn RuntimeStats> =
                    Arc::new(DefaultRuntimeStats::new(&meter, &node_info));
                stats_manager.register_runtime_stats(node_id, 0, runtime_stats.clone());

                // Process both children sequentially - first left, then right
                let mut node_initialized = false;

                let control = Self::process_child(
                    node_id,
                    left_receiver,
                    destination_sender.clone(),
                    runtime_stats.clone(),
                    &stats_manager,
                    &mut node_initialized,
                )
                .await?;
                if control.is_break() {
                    stats_manager.finalize_node(node_id);
                    return Ok(());
                }

                let control = Self::process_child(
                    node_id,
                    right_receiver,
                    destination_sender.clone(),
                    runtime_stats.clone(),
                    &stats_manager,
                    &mut node_initialized,
                )
                .await?;
                if control.is_break() {
                    stats_manager.finalize_node(node_id);
                    return Ok(());
                }
                let _ = destination_sender.send(PipelineMessage::Flush(0)).await;

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
}
