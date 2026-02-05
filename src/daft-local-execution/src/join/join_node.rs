use std::sync::Arc;

use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_metrics::{
    ops::{NodeCategory, NodeInfo},
    snapshot::StatSnapshotImpl,
};
use common_runtime::{OrderingAwareJoinSet, get_compute_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner,
    channel::{Receiver, create_channel},
    dynamic_batching::{BatchManager, DynBatchingStrategy, StaticBatchingStrategy},
    join::{
        build::{BuildExecutionContext, BuildStateBridge},
        join_operator::JoinOperator,
        probe::ProbeExecutionContext,
    },
    pipeline::{MorselSizeRequirement, PipelineNode, RuntimeContext},
    pipeline_message::PipelineMessage,
    runtime_stats::RuntimeStats,
};

pub struct JoinNode<Op: JoinOperator> {
    op: Arc<Op>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    runtime_stats: Arc<dyn RuntimeStats>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
    node_info: Arc<NodeInfo>,
}

impl<Op: JoinOperator + 'static> JoinNode<Op> {
    pub(crate) fn new(
        op: Arc<Op>,
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::Intermediate, context);
        let runtime_stats = op.make_runtime_stats(node_info.id);

        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
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
}

impl<Op: JoinOperator + 'static> TreeDisplay for JoinNode<Op> {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.op.name()).unwrap();
            }
            level => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
                if matches!(level, DisplayLevel::Verbose) {
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
            "type": self.op.op_type().to_string(),
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

impl<Op: JoinOperator + 'static> PipelineNode for JoinNode<Op> {
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
        let operator_morsel_size_requirement = self.op.morsel_size_requirement();
        // Right side (probe): behave like IntermediateNode - combine operator requirement
        // with downstream requirement
        let right_morsel_size_requirement = MorselSizeRequirement::combine_requirements(
            operator_morsel_size_requirement,
            downstream_requirement,
        );

        // Use the right side requirement for the join node itself
        self.morsel_size_requirement = right_morsel_size_requirement;

        self.left
            .propagate_morsel_size_requirement(default_requirement, default_requirement);
        self.right
            .propagate_morsel_size_requirement(right_morsel_size_requirement, default_requirement);
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let build_child_receiver = self.left.start(false, runtime_handle)?;
        let probe_child_receiver = self.right.start(maintain_order, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        // Create task spawners
        let build_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::Build"),
        );
        let probe_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::Probe"),
        );
        let finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            self.runtime_stats.clone(),
            info_span!("JoinNode::FinalizeProbe"),
        );

        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let runtime_stats = self.runtime_stats.clone();
        let op_name = self.op.name().to_string();
        let op = self.op.clone();
        runtime_handle.spawn(
            async move {
                // Create BuildStateBridge shared between build and probe sides
                let build_state_bridge = Arc::new(BuildStateBridge::new());

                // Initialize build side
                let mut build_ctx = BuildExecutionContext::new(
                    op.clone(),
                    build_task_spawner,
                    build_state_bridge.clone(),
                    runtime_stats.clone(),
                    stats_manager.clone(),
                );

                // Initialize probe side
                let mut probe_ctx = ProbeExecutionContext::new(
                    op.clone(),
                    probe_task_spawner,
                    finalize_spawner,
                    destination_sender,
                    Arc::new(BatchManager::new(DynBatchingStrategy::from(
                        StaticBatchingStrategy::new(
                            op.morsel_size_requirement().unwrap_or_default(),
                        ),
                    ))),
                    build_state_bridge.clone(),
                    runtime_stats.clone(),
                    maintain_order,
                    stats_manager,
                );

                // Spawn both processes concurrently
                let (build_result, probe_result) = tokio::join!(
                    build_ctx.process_build_input(node_id, build_child_receiver,),
                    ProbeExecutionContext::process_probe_input(
                        node_id,
                        probe_child_receiver,
                        &mut probe_ctx,
                    ),
                );

                build_result?;
                probe_result?;

                Ok(())
            },
            &op_name,
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
