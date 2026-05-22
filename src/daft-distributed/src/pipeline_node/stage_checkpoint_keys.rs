use std::sync::{Arc, atomic::Ordering};

use common_checkpoint_config::CheckpointConfig;
use common_metrics::{
    Counter, Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StageCheckpointKeysSnapshot,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use opentelemetry::KeyValue;

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        NodeID, PipelineNodeConfig, PipelineNodeContext, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
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
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    keys_staged: Counter,
    num_tasks: Counter,
    node_kv: Vec<KeyValue>,
}

impl StageCheckpointKeysStats {
    fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            keys_staged: meter.u64_counter("keys_staged"),
            num_tasks: meter.num_tasks_metric(),
            node_kv,
        }
    }
}

impl RuntimeStats for StageCheckpointKeysStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::StageCheckpointKeys(snapshot) = snapshot else {
            return;
        };
        self.duration_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());
        self.bytes_in
            .add(snapshot.bytes_in, self.node_kv.as_slice());
        self.bytes_out
            .add(snapshot.bytes_out, self.node_kv.as_slice());
        self.keys_staged
            .add(snapshot.keys_staged, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::StageCheckpointKeys(StageCheckpointKeysSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            keys_staged: self.keys_staged.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            num_tasks: self.num_tasks.load(Ordering::Relaxed),
        })
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
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
