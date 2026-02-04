use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    CPU_US_KEY, Counter, Gauge, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, snapshot::FilterSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use opentelemetry::{KeyValue, metrics::Meter};

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
};

pub struct FilterStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    selectivity: Gauge,
    node_kv: Vec<KeyValue>,
}

impl FilterStats {
    pub fn new(meter: &Meter, node_id: NodeID) -> Self {
        let node_kv = vec![KeyValue::new("node_id", node_id.to_string())];
        Self {
            cpu_us: Counter::new(meter, CPU_US_KEY, None),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None),
            selectivity: Gauge::new(meter, "selectivity", None),
            node_kv,
        }
    }
}

impl RuntimeStats for FilterStats {
    fn handle_worker_node_stats(&self, snapshot: &StatSnapshot) {
        let StatSnapshot::Filter(snapshot) = snapshot else {
            return;
        };
        self.cpu_us.add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());

        let selectivity = if snapshot.rows_in == 0 {
            100.0
        } else {
            (snapshot.rows_out as f64 / snapshot.rows_in as f64) * 100.0
        };
        self.selectivity
            .update(selectivity, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Filter(FilterSnapshot {
            cpu_us: self.cpu_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            selectivity: self.selectivity.load(Ordering::Relaxed),
        })
    }
}

pub(crate) struct FilterNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    predicate: BoundExpr,
    child: DistributedPipelineNode,
}

impl FilterNode {
    const NODE_NAME: NodeName = "Filter";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        predicate: BoundExpr,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            predicate,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for FilterNode {
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
        vec![format!("Filter: {}", self.predicate)]
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(FilterStats::new(meter, self.node_id()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let predicate = self.predicate.clone();
        let node_id = self.node_id();
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::filter(
                input,
                predicate.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        })
    }
}
