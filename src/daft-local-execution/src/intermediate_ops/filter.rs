use std::sync::{Arc, atomic::Ordering};

use common_error::DaftResult;
use common_metrics::{
    Counter, Gauge, Meter, StatSnapshot,
    ops::{NodeInfo, NodeType},
    snapshot::FilterSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use opentelemetry::KeyValue;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, runtime_stats::RuntimeStats};

pub struct FilterStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    selectivity: Gauge,
    node_kv: Vec<KeyValue>,
}

impl FilterStats {
    fn update_selectivity(&self, rows_in: u64, rows_out: u64) {
        let selectivity = if rows_in == 0 {
            100.0
        } else {
            (rows_out as f64 / rows_in as f64) * 100.0
        };
        self.selectivity
            .update(selectivity, self.node_kv.as_slice());
    }
}

impl RuntimeStats for FilterStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();

        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            selectivity: meter.f64_gauge("selectivity"),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.duration_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);
        let selectivity = self.selectivity.load(ordering);

        StatSnapshot::Filter(FilterSnapshot {
            cpu_us,
            rows_in,
            rows_out,
            selectivity,
            bytes_in: self.bytes_in.load(ordering),
            bytes_out: self.bytes_out.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
        })
    }

    fn add_rows_in(&self, rows: u64) {
        let rows_in = self.rows_in.add(rows, self.node_kv.as_slice());
        self.update_selectivity(rows_in, self.rows_out.load(Ordering::Relaxed));
    }

    fn add_rows_out(&self, rows: u64) {
        let rows_out = self.rows_out.add(rows, self.node_kv.as_slice());
        self.update_selectivity(self.rows_in.load(Ordering::Relaxed), rows_out);
    }

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.add(bytes, self.node_kv.as_slice());
    }

    fn add_bytes_out(&self, bytes: u64) {
        self.bytes_out.add(bytes, self.node_kv.as_slice());
    }

    fn add_num_tasks(&self, num_tasks: u64) {
        self.num_tasks.add(num_tasks, self.node_kv.as_slice());
    }
}

pub struct FilterOperator {
    predicate: BoundExpr,
}

impl FilterOperator {
    pub fn new(predicate: BoundExpr) -> Self {
        Self { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    type State = ();
    type Stats = FilterStats;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "FilterOperator::execute")]
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let predicate = self.predicate.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.filter(&[predicate])?;
                    Ok((state, out))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("Filter: {}", self.predicate)]
    }

    fn name(&self) -> NodeName {
        "Filter".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Filter
    }

    fn make_state(&self) -> Self::State {}
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use common_metrics::{Meter, ops::NodeInfo};

    use super::FilterStats;
    use crate::runtime_stats::RuntimeStats;

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    #[test]
    fn selectivity_updates_after_rows_events() {
        let stats = FilterStats::new(&Meter::test_scope("test_stats"), &node_info_from_id(42));

        stats.add_rows_in(200);
        stats.add_rows_out(50);

        // 50 / 200 = 0.25 -> 25%
        let selectivity = stats.selectivity.load(Ordering::Relaxed);
        assert!(
            (selectivity - 25.0).abs() < f64::EPSILON,
            "expected selectivity to be 25%, got {}",
            selectivity
        );
    }

    #[test]
    fn selectivity_defaults_to_100_percent_on_zero_rows_in() {
        let stats = FilterStats::new(&Meter::test_scope("test_stats"), &node_info_from_id(1));

        stats.add_rows_out(10);

        let selectivity = stats.selectivity.load(Ordering::Relaxed);
        assert!(
            (selectivity - 100.0).abs() < f64::EPSILON,
            "expected selectivity to be 100% for zero input rows, got {}",
            selectivity
        );
    }

    #[test]
    fn selectivity_handles_multiple_updates() {
        let stats = FilterStats::new(&Meter::test_scope("test_stats"), &node_info_from_id(99));

        stats.add_rows_in(100);
        stats.add_rows_out(40);
        let after_first_batch = stats.selectivity.load(Ordering::Relaxed);
        assert!(
            (after_first_batch - 40.0).abs() < f64::EPSILON,
            "expected selectivity to be 40% after first batch, got {}",
            after_first_batch
        );

        stats.add_rows_in(100);
        stats.add_rows_out(10);
        let final_selectivity = stats.selectivity.load(Ordering::Relaxed);
        assert!(
            (final_selectivity - 25.0).abs() < f64::EPSILON,
            "expected selectivity to be 25% after second batch, got {}",
            final_selectivity
        );
    }
}
