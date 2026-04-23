use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, Gauge, Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::FilterSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
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

pub struct FilterStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    selectivity: Gauge,
    node_kv: Vec<KeyValue>,
}

impl FilterStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            selectivity: meter.f64_gauge("selectivity"),
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
        self.bytes_in
            .add(snapshot.bytes_in, self.node_kv.as_slice());
        self.bytes_out
            .add(snapshot.bytes_out, self.node_kv.as_slice());

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
            bytes_in: self.bytes_in.load(Ordering::SeqCst),
            bytes_out: self.bytes_out.load(Ordering::SeqCst),
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
    const NODE_NAME: &'static str = "Filter";

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
            Arc::from(Self::NODE_NAME),
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

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
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
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_metrics::{Meter, StatSnapshot};

    use super::*;
    use crate::pipeline_node::test_helpers::{
        build_in_memory_source, make_partition, predicate_x_gt_2, run_pipeline_and_get_stats,
        test_schema,
    };

    fn build_filter_pipeline(
        partitions: Vec<Arc<daft_micropartition::MicroPartition>>,
        meter: &Meter,
    ) -> DistributedPipelineNode {
        let (source, plan_config) = build_in_memory_source(0, partitions, meter);
        let filter_node =
            FilterNode::new(1, &plan_config, predicate_x_gt_2(), test_schema(), source);
        DistributedPipelineNode::new(Arc::new(filter_node), meter)
    }

    /// Filter(x > 2) on [1,2,3,4,5]: rows_in=5, rows_out=3, selectivity=60%.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stats_aggregation() -> DaftResult<()> {
        let meter = Meter::test_scope("test_filter_stats");
        let pipeline = build_filter_pipeline(vec![make_partition(&[1, 2, 3, 4, 5])], &meter);

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 1)
            .expect("filter node stats");
        match snapshot {
            StatSnapshot::Filter(s) => {
                assert_eq!(s.rows_in, 5);
                assert_eq!(s.rows_out, 3);
                assert!((s.selectivity - 60.0).abs() < 0.01);
                assert!(s.cpu_us > 0);
            }
            other => panic!("expected Filter snapshot, got: {other:?}"),
        }

        Ok(())
    }

    /// Filter across 3 partitions: verifies stats aggregate correctly.
    /// [1,2,3] → 1 passes, [4,5] → 2 pass, [1,1,1,1] → 0 pass.
    /// Total: 9 in, 3 out, selectivity ≈ 33.3%.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stats_multi_partition() -> DaftResult<()> {
        let meter = Meter::test_scope("test_filter_multi");
        let pipeline = build_filter_pipeline(
            vec![
                make_partition(&[1, 2, 3]),
                make_partition(&[4, 5]),
                make_partition(&[1, 1, 1, 1]),
            ],
            &meter,
        );

        let stats = run_pipeline_and_get_stats(&pipeline, &meter).await?;

        let (_, snapshot) = stats
            .iter()
            .find(|(i, _)| i.id == 1)
            .expect("filter node stats");
        match snapshot {
            StatSnapshot::Filter(s) => {
                assert_eq!(s.rows_in, 9);
                assert_eq!(s.rows_out, 3);
                let expected = (3.0 / 9.0) * 100.0;
                assert!((s.selectivity - expected).abs() < 0.01);
            }
            other => panic!("expected Filter snapshot, got: {other:?}"),
        }

        Ok(())
    }
}
