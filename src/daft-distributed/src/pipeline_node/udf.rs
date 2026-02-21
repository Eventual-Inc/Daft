use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::Ordering},
};

use common_metrics::{
    Counter, DURATION_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, UNIT_MICROSECONDS, UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::UdfSnapshot,
};
use daft_dsl::{expr::bound_expr::BoundExpr, functions::python::UDFProperties};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::translate_clustering_spec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;
use opentelemetry::{KeyValue, metrics::Meter};

use super::PipelineNodeImpl;
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        TaskBuilderStream, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
};

pub struct UdfStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    custom_counters: Mutex<HashMap<Arc<str>, Counter>>,
    meter: Meter,
    node_kv: Vec<KeyValue>,
}

impl UdfStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None, Some(UNIT_ROWS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
            custom_counters: Mutex::new(HashMap::new()),
            meter: meter.clone(),
            node_kv,
        }
    }
}

impl RuntimeStats for UdfStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let StatSnapshot::Udf(snapshot) = snapshot else {
            return;
        };
        self.duration_us
            .add(snapshot.cpu_us, self.node_kv.as_slice());
        self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
        self.rows_out
            .add(snapshot.rows_out, self.node_kv.as_slice());

        // Handle custom counters dynamically
        let mut custom_counters = self.custom_counters.lock().unwrap();
        for (name, value) in &snapshot.custom_counters {
            let counter = custom_counters.entry(name.clone()).or_insert_with(|| {
                let name = name.as_ref().to_string();
                Counter::new(&self.meter, name, None, None)
            });
            counter.add(*value, self.node_kv.as_slice());
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Udf(UdfSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            custom_counters: self
                .custom_counters
                .lock()
                .unwrap()
                .iter()
                .map(|(name, counter)| (name.clone(), counter.load(Ordering::Relaxed)))
                .collect(),
        })
    }
}

pub(crate) struct UDFNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    child: DistributedPipelineNode,
}

impl UDFNode {
    const NODE_NAME: NodeName = "UDF";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::UDFProject,
            NodeCategory::Intermediate,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            translate_clustering_spec(
                child.config().clustering_spec.clone(),
                &passthrough_columns
                    .iter()
                    .map(|e| e.inner().clone())
                    .collect::<Vec<_>>(),
            ),
        );
        Self {
            config,
            context,
            expr,
            udf_properties,
            passthrough_columns,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for UDFNode {
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
        let mut res = vec![
            format!(
                "{} {}:",
                if self.udf_properties.builtin_name {
                    "Builtin UDF"
                } else {
                    "UDF"
                },
                self.udf_properties.name
            ),
            format!("Expr = {}", self.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.passthrough_columns.iter().join(", ")
            ),
            format!(
                "Properties = {{ {} }}",
                self.udf_properties.multiline_display(false).join(", ")
            ),
        ];

        if let Some(resource_request) = &self.udf_properties.resource_request {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }

        res
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(UdfStats::new(meter, self.context()))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let expr = self.expr.clone();
        let udf_properties = self.udf_properties.clone();
        let passthrough_columns = self.passthrough_columns.clone();
        let schema = self.config.schema.clone();
        let node_id = self.context.node_id;
        let plan_builder = move |input: LocalPhysicalPlanRef| -> LocalPhysicalPlanRef {
            LocalPhysicalPlan::udf_project(
                input,
                expr.clone(),
                udf_properties.clone(),
                passthrough_columns.clone(),
                schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext {
                    origin_node_id: Some(node_id as usize),
                    additional: None,
                },
            )
        };

        input_node.pipeline_instruction(self, plan_builder)
    }
}
