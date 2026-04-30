use std::sync::Arc;

use daft_common::checkpoint_config::CheckpointConfig;
use daft_common::metrics::{
    Meter,
    ops::{NodeCategory, NodeType},
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef},
    },
};

pub(crate) struct StageCheckpointKeysNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    checkpoint_config: CheckpointConfig,
    child: DistributedPipelineNode,
}

impl StageCheckpointKeysNode {
    const NODE_NAME: &'static str = "StageCheckpointKeys";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        checkpoint_config: CheckpointConfig,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::StageCheckpointKeys,
            NodeCategory::Intermediate,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            checkpoint_config,
            child,
        }
    }
}

struct StageCheckpointKeysStats {
    base: BaseCounters,
}

impl StageCheckpointKeysStats {
    fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for StageCheckpointKeysStats {
    fn handle_worker_node_stats(
        &self,
        _node_info: &daft_common::metrics::ops::NodeInfo,
        snapshot: &daft_common::metrics::StatSnapshot,
    ) {
        use daft_common::metrics::snapshot::StatSnapshotImpl as _;
        self.base.add_duration_us(snapshot.duration_us());
    }

    fn export_snapshot(&self) -> daft_common::metrics::StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn increment_num_tasks(&self) {
        self.base.increment_num_tasks();
    }
}

impl PipelineNodeImpl for StageCheckpointKeysNode {
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
        vec![format!(
            "StageCheckpointKeys: key_column={}",
            self.checkpoint_config.key_column
        )]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(StageCheckpointKeysStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let checkpoint_config = self.checkpoint_config.clone();
        let node_id = self.node_id();
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::stage_checkpoint_keys(
                input,
                checkpoint_config.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }
}
