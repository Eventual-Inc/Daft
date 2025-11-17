use std::sync::Arc;

use common_metrics::{QueryID, ROWS_IN_KEY, ROWS_OUT_KEY, Stat};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use opentelemetry::{global, metrics::Gauge};

use super::{DistributedPipelineNode, PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
    statistics::{
        TaskEvent,
        stats::{DefaultRuntimeStats, RuntimeStats},
    },
};

pub struct FilterStats {
    default_stats: DefaultRuntimeStats,
    selectivity: Gauge<f64>,
}

impl RuntimeStats for FilterStats {
    fn handle_task_event(&self, event: &TaskEvent) -> common_error::DaftResult<()> {
        self.default_stats.handle_task_event(event)?;

        // TODO this assumes we will get only one Completed event - can we get multiple?
        if let TaskEvent::Completed { stats, .. } = event {
            // Iterate over all worker stats and extract ones relevant to this node
            let relevant_worker_stats = stats.iter().filter_map(|(stat_node_id, stats)| {
                if *stat_node_id == (self.default_stats.node_id as usize) {
                    Some(stats)
                } else {
                    None
                }
            });
            let (total_rows_in, total_rows_out) =
                relevant_worker_stats.fold((0, 0), |(mut acc_ri, mut acc_ro), worker_stats| {
                    for (k, stat) in worker_stats.iter() {
                        if k == ROWS_IN_KEY
                            && let Stat::Count(worker_ri) = stat
                        {
                            acc_ri += worker_ri;
                        } else if k == ROWS_OUT_KEY
                            && let Stat::Count(worker_ro) = stat
                        {
                            acc_ro += worker_ro;
                        }
                    }

                    (acc_ri, acc_ro)
                });

            let selectivity = if total_rows_in == 0 {
                100.
            } else {
                (total_rows_out as f64 / total_rows_in as f64) * 100.
            };
            self.selectivity
                .record(selectivity, self.default_stats.node_kv.as_slice());
        }

        Ok(())
    }
}

impl FilterStats {
    pub fn new(node_id: NodeID, query_id: QueryID) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let default_stats = DefaultRuntimeStats::new_impl(&meter, node_id, query_id);

        Self {
            default_stats,
            selectivity: meter
                .f64_gauge("daft.distributed.node_stats.selectivity")
                .build(),
        }
    }
}

pub(crate) struct FilterNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    predicate: BoundExpr,
    child: DistributedPipelineNode,
    stats: Arc<FilterStats>,
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
            stats: Arc::new(FilterStats::new(node_id, plan_config.query_id.clone())),
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

    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.stats.clone()
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> SubmittableTaskStream {
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
