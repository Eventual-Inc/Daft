use std::sync::Arc;

use daft_common::checkpoint_config::CheckpointIdMap;
use daft_common::display::tree::TreeDisplay;
use daft_common::metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo, NodeType},
};
use daft_checkpoint::CheckpointStoreRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineMessage, PipelineNode},
    runtime_stats::{DefaultRuntimeStats, RuntimeStats},
};

/// Passthrough node that seals per-input checkpoints when a plan has no
/// native write sink (e.g. external side-effects via UDFs followed by
/// `.collect()`).
///
/// Key staging is handled upstream by `StageCheckpointKeysOperator` (inserted
/// unconditionally by the `rewrite_checkpoint_source` rule). This node forwards
/// morsels unchanged and, on `PipelineMessage::Flush(input_id)`, calls
/// `store.checkpoint(id)` to mark that task's staged keys visible to future
/// runs.
pub struct CheckpointTerminusNode {
    child: Box<dyn PipelineNode>,
    store: CheckpointStoreRef,
    id_map: CheckpointIdMap,
    meter: Meter,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

impl CheckpointTerminusNode {
    pub(crate) fn new(
        child: Box<dyn PipelineNode>,
        store: CheckpointStoreRef,
        id_map: CheckpointIdMap,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = "CheckpointTerminus".into();
        let node_info = ctx.next_node_info(
            name,
            NodeType::StageCheckpointKeys,
            NodeCategory::StreamingSink,
            context,
        );
        Self {
            child,
            store,
            id_map,
            meter: ctx.meter.clone(),
            plan_stats,
            morsel_size_requirement: MorselSizeRequirement::default(),
            node_info: Arc::new(node_info),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for CheckpointTerminusNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: daft_common::display::DisplayLevel) -> String {
        use std::fmt::Write;

        use daft_common::display::DisplayLevel;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "CheckpointTerminus").unwrap();
            }
            _ => {
                writeln!(display, "CheckpointTerminus").unwrap();
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

        serde_json::json!({
            "id": self.node_id(),
            "category": "StreamingSink",
            "type": NodeType::StageCheckpointKeys.to_string(),
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

impl PipelineNode for CheckpointTerminusNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.child]
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
        self.child
            .propagate_morsel_size_requirement(downstream_requirement, default_requirement);
    }

    fn start(
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();

        let mut child_receiver = self.child.start(maintain_order, runtime_handle)?;
        let (output_sender, output_receiver) = create_channel(1);

        let stats_manager = runtime_handle.stats_manager();
        let meter = self.meter.clone();
        let node_info = self.node_info.clone();
        let store = self.store.clone();
        let id_map = self.id_map.clone();

        runtime_handle.spawn(
            async move {
                let runtime_stats: Arc<dyn RuntimeStats> =
                    Arc::new(DefaultRuntimeStats::new(&meter, &node_info));
                stats_manager.register_runtime_stats(node_id, 0, runtime_stats.clone());

                let mut node_initialized = false;
                while let Some(msg) = child_receiver.recv().await {
                    if !node_initialized {
                        stats_manager.activate_node(node_id);
                        node_initialized = true;
                    }
                    match msg {
                        PipelineMessage::Morsel {
                            partition,
                            input_id,
                        } => {
                            let rows = partition.len() as u64;
                            runtime_stats.add_rows_in(rows);
                            runtime_stats.add_rows_out(rows);
                            if output_sender
                                .send(PipelineMessage::Morsel {
                                    partition,
                                    input_id,
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        PipelineMessage::Flush(input_id) => {
                            let checkpoint_id = id_map.get_or_generate(input_id);
                            store.checkpoint(&checkpoint_id).await?;
                            if output_sender
                                .send(PipelineMessage::Flush(input_id))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        PipelineMessage::FlightPartitionRef {
                            input_id,
                            partition_ref,
                        } => {
                            if output_sender
                                .send(PipelineMessage::FlightPartitionRef {
                                    input_id,
                                    partition_ref,
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(output_receiver)
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
