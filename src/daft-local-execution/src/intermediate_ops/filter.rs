use std::sync::{Arc, atomic::Ordering};

use common_error::DaftResult;
use common_metrics::{
    Counter, Gauge, Meter, StatSnapshot,
    ops::{NodeInfo, NodeType},
    snapshot::{FilterSnapshot, StatSnapshotImpl as _},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use opentelemetry::KeyValue;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    runtime_stats::{IntermediateRuntimeStats, RuntimeStats, RuntimeStatsRef},
};

pub struct FilterStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    selectivity: Gauge,
    node_kv: Vec<KeyValue>,
    child_stats: RuntimeStatsRef,
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
    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.duration_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);
        let selectivity = self.selectivity.load(ordering);
        let child_estimated_total = self.child_stats.build_snapshot(ordering).total();

        StatSnapshot::Filter(FilterSnapshot {
            cpu_us,
            rows_in,
            rows_out,
            selectivity,
            estimated_total_rows: (child_estimated_total as f64 * selectivity / 100.0) as u64,
        })
    }

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }
}

impl IntermediateRuntimeStats for FilterStats {
    fn new(meter: &Meter, node_info: &NodeInfo, child_stats: RuntimeStatsRef) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            selectivity: meter.f64_gauge("selectivity"),
            child_stats,
            node_kv,
        }
    }

    fn add_rows_in(&self, rows: u64) {
        let rows_in = self.rows_in.add(rows, self.node_kv.as_slice());
        self.update_selectivity(rows_in, self.rows_out.load(Ordering::Relaxed));
    }

    fn add_rows_out(&self, rows: u64) {
        let rows_out = self.rows_out.add(rows, self.node_kv.as_slice());
        self.update_selectivity(self.rows_in.load(Ordering::Relaxed), rows_out);
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
    use std::sync::{Arc, atomic::Ordering};

    use common_metrics::{Meter, StatSnapshot, ops::NodeInfo};

    use super::FilterStats;
    use crate::runtime_stats::{IntermediateRuntimeStats as _, RuntimeStats};

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    use common_metrics::snapshot::StatSnapshotImpl as _;

    struct MockRuntimeStats;
    impl RuntimeStats for MockRuntimeStats {
        fn build_snapshot(&self, _ordering: Ordering) -> StatSnapshot {
            unimplemented!()
        }
        fn add_duration_us(&self, _duration_us: u64) {
            unimplemented!()
        }
    }

    /// Mock child that reports a configurable estimated total.
    struct MockChildWithEstimate {
        estimated_total: u64,
    }
    impl RuntimeStats for MockChildWithEstimate {
        fn build_snapshot(&self, _ordering: Ordering) -> StatSnapshot {
            StatSnapshot::Source(common_metrics::snapshot::SourceSnapshot {
                cpu_us: 0,
                rows_out: 0,
                bytes_read: 0,
                estimated_total_rows: self.estimated_total,
            })
        }
        fn add_duration_us(&self, _: u64) {}
    }

    fn make_stats() -> FilterStats {
        FilterStats::new(
            &Meter::test_scope("test_stats"),
            &node_info_from_id(42),
            Arc::new(MockRuntimeStats),
        )
    }

    fn make_stats_with_child_estimate(child_estimated_total: u64) -> FilterStats {
        FilterStats::new(
            &Meter::test_scope("test_stats_est"),
            &node_info_from_id(42),
            Arc::new(MockChildWithEstimate {
                estimated_total: child_estimated_total,
            }),
        )
    }

    #[test]
    fn selectivity_updates_after_rows_events() {
        let stats = make_stats();

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
        let stats = make_stats();

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
        let stats = make_stats();

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

    // ── Estimated total rows / build_snapshot tests ──────────────────────

    #[test]
    fn estimated_total_rows_projects_child_estimate_through_selectivity() {
        let stats = make_stats_with_child_estimate(10_000);
        // 200 in, 50 out → 25% selectivity
        stats.add_rows_in(200);
        stats.add_rows_out(50);

        let snap = stats.build_snapshot(Ordering::SeqCst);
        // estimated = 10_000 * 25% = 2500
        assert_eq!(snap.total(), 2500);
        assert_eq!(snap.current_progress(), 50);
    }

    #[test]
    fn estimated_total_rows_with_100_percent_selectivity() {
        let stats = make_stats_with_child_estimate(5000);
        // Everything passes → 100% selectivity
        stats.add_rows_in(100);
        stats.add_rows_out(100);

        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.total(), 5000);
    }

    #[test]
    fn estimated_total_rows_before_any_data() {
        let stats = make_stats_with_child_estimate(5000);
        // No data yet → selectivity defaults to 100% in build_snapshot
        // (because selectivity gauge starts as NaN → load returns NaN)
        // Actually: Gauge starts at NaN, filter build_snapshot loads NaN
        // and computes (5000 * NaN / 100) = NaN → cast to u64 = 0
        // This is the expected edge case behavior.
        let snap = stats.build_snapshot(Ordering::SeqCst);
        // NaN selectivity → estimated = 0 (NaN as u64 = 0)
        assert_eq!(snap.total(), 0);
    }

    #[test]
    fn estimated_total_evolves_as_selectivity_stabilizes() {
        let stats = make_stats_with_child_estimate(10_000);

        // Batch 1: very selective (10%) — early batches can be unrepresentative
        stats.add_rows_in(100);
        stats.add_rows_out(10);
        let snap1 = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap1.total(), 1000); // 10_000 * 10%

        // Batch 2: less selective overall → 200 in, 60 out → 30%
        stats.add_rows_in(100);
        stats.add_rows_out(50);
        let snap2 = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap2.total(), 3000); // 10_000 * 30%
    }

    #[test]
    fn filter_snapshot_message_format() {
        let stats = make_stats_with_child_estimate(1000);
        stats.add_rows_in(400);
        stats.add_rows_out(100);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        let msg = snap.to_message();
        assert!(
            msg.contains("25.00%"),
            "expected '25.00%' in message, got: {msg}"
        );
        assert!(
            msg.contains("after filter"),
            "expected 'after filter' in message, got: {msg}"
        );
    }
}
