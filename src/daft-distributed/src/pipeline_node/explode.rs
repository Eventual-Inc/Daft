use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, DURATION_KEY, Gauge, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, UNIT_MICROSECONDS,
    UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::ExplodeSnapshot,
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

pub struct ExplodeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    amplification: Gauge,
    node_kv: Vec<KeyValue>,
}

impl ExplodeStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None, Some(UNIT_ROWS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
            amplification: Gauge::new(meter, "amplification", None),
            node_kv,
        }
    }

    fn amplification(rows_in: u64, rows_out: u64) -> f64 {
        if rows_in == 0 {
            1.0
        } else {
            rows_out as f64 / rows_in as f64
        }
    }
}

impl RuntimeStats for ExplodeStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Explode(snapshot) = snapshot else {
            return;
        };
        self.duration_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());

        let amplification = Self::amplification(snapshot.rows_in, snapshot.rows_out);
        self.amplification
            .update(amplification, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        let rows_in = self.rows_in.load(Ordering::SeqCst);
        let rows_out = self.rows_out.load(Ordering::SeqCst);
        let amplification = Self::amplification(rows_in, rows_out);
        StatSnapshot::Explode(ExplodeSnapshot {
            cpu_us: self.duration_us.load(Ordering::SeqCst),
            rows_in,
            rows_out,
            amplification,
        })
    }
}

pub(crate) struct ExplodeNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    to_explode: Vec<BoundExpr>,
    ignore_empty_and_null: bool,
    index_column: Option<String>,
    child: DistributedPipelineNode,
}

impl ExplodeNode {
    const NODE_NAME: NodeName = "Explode";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        to_explode: Vec<BoundExpr>,
        ignore_empty_and_null: bool,
        index_column: Option<String>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::Explode,
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
            to_explode,
            ignore_empty_and_null,
            index_column,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for ExplodeNode {
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
        use itertools::Itertools;
        let mut res = vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )];
        if let Some(ref idx_col) = self.index_column {
            res.push(format!("Index column = {}", idx_col));
        }
        res
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(ExplodeStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let to_explode = self.to_explode.clone();
        let index_column = self.index_column.clone();
        let schema = self.config.schema.clone();
        let node_id = self.node_id();
        let ignore_empty_and_null = self.ignore_empty_and_null;
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::explode(
                input,
                to_explode.clone(),
                ignore_empty_and_null,
                index_column.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        })
    }
}
