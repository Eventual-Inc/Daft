use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, DURATION_KEY, Gauge, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, UNIT_MICROSECONDS,
    UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::FilterSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use opentelemetry::{KeyValue, metrics::Meter};

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
};

pub struct FilterStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    selectivity: Gauge,
    node_kv: Vec<KeyValue>,
}

impl FilterStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None, Some(UNIT_ROWS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
            selectivity: Gauge::new(meter, "selectivity", None),
            node_kv,
        }
    }

    fn selectivity(rows_in: u64, rows_out: u64) -> f64 {
        if rows_in == 0 {
            100.0
        } else {
            (rows_out as f64 / rows_in as f64) * 100.0
        }
    }
}

impl RuntimeStats for FilterStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Filter(snapshot) = snapshot else {
            return;
        };
        self.duration_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());

        let selectivity = Self::selectivity(snapshot.rows_in, snapshot.rows_out);
        self.selectivity
            .update(selectivity, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        let rows_in = self.rows_in.load(Ordering::SeqCst);
        let rows_out = self.rows_out.load(Ordering::SeqCst);
        let selectivity = Self::selectivity(rows_in, rows_out);
        StatSnapshot::Filter(FilterSnapshot {
            cpu_us: self.duration_us.load(Ordering::SeqCst),
            rows_in,
            rows_out,
            selectivity,
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
            NodeType::Filter,
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
        Arc::new(FilterStats::new(meter, self.context()))
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
