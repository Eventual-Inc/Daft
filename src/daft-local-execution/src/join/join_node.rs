use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_metrics::{
    Meter,
    ops::{NodeCategory, NodeInfo},
};
use common_runtime::get_compute_runtime;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use tracing::info_span;

use crate::{
    ExecutionRuntimeContext, ExecutionTaskSpawner,
    channel::{Receiver, create_channel},
    join::{
        build::{BuildExecutionContext, BuildStateBridge},
        join_operator::JoinOperator,
        probe::ProbeExecutionContext,
    },
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineMessage, PipelineNode},
};

pub struct JoinNode<Op: JoinOperator> {
    op: Arc<Op>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    meter: Meter,
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
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = op.name().into();
        let node_info = ctx.next_node_info(name, op.op_type(), NodeCategory::Intermediate, context);
        let morsel_size_requirement = op.morsel_size_requirement().unwrap_or_default();
        Self {
            op,
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
            _ => {
                let multiline_display = self.op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
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
            "category": "Intermediate",
            "type": self.op.op_type().to_string(),
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
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();
        let stats_manager = runtime_handle.stats_manager();

        let build_child_receiver = self.left.start(false, runtime_handle)?;
        let probe_child_receiver = self.right.start(maintain_order, runtime_handle)?;

        let (destination_sender, destination_receiver) = create_channel(1);

        // Create task spawners
        let build_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            info_span!("JoinNode::Build"),
        );
        let probe_task_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            info_span!("JoinNode::Probe"),
        );
        let probe_finalize_spawner = ExecutionTaskSpawner::new(
            get_compute_runtime(),
            runtime_handle.memory_manager(),
            info_span!("JoinNode::FinalizeProbe"),
        );

        // Create BuildStateBridge shared between build and probe sides
        let build_state_bridge = Arc::new(BuildStateBridge::new());

        // Initialize build side
        let build_ctx = BuildExecutionContext::new(
            self.op.clone(),
            build_task_spawner,
            build_state_bridge.clone(),
            stats_manager.clone(),
            node_id,
            self.meter.clone(),
            self.node_info.clone(),
        );

        // Initialize probe side
        let probe_ctx = ProbeExecutionContext::new(
            self.op.clone(),
            probe_task_spawner,
            probe_finalize_spawner,
            destination_sender,
            build_state_bridge,
            maintain_order,
            stats_manager.clone(),
            node_id,
            self.meter.clone(),
            self.node_info.clone(),
        );

        runtime_handle.spawn(
            async move {
                let (build_result, probe_result) = tokio::join!(
                    build_ctx.process_build_input(build_child_receiver),
                    probe_ctx.process_probe_input(probe_child_receiver),
                );

                build_result?;
                probe_result?;

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
