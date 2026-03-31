use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    meters::Counter,
    ops::{NodeInfo, NodeType},
    snapshot::{ExplodeSnapshot, StatSnapshotImpl as _},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_list::explode;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use opentelemetry::KeyValue;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    runtime_stats::{IntermediateRuntimeStats, RuntimeStats, RuntimeStatsRef},
};

pub struct ExplodeStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
    child_stats: RuntimeStatsRef,
}

impl RuntimeStats for ExplodeStats {
    fn build_snapshot(&self, ordering: std::sync::atomic::Ordering) -> StatSnapshot {
        let cpu_us = self.duration_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);

        let amplification = if rows_in == 0 {
            1.
        } else {
            rows_out as f64 / rows_in as f64
        };

        let child_estimated_total = self.child_stats.build_snapshot(ordering).total();

        StatSnapshot::Explode(ExplodeSnapshot {
            cpu_us,
            rows_in,
            rows_out,
            amplification,
            estimated_total_rows: (child_estimated_total as f64 * amplification) as u64,
        })
    }

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }
}

impl IntermediateRuntimeStats for ExplodeStats {
    fn new(meter: &Meter, node_info: &NodeInfo, child_stats: RuntimeStatsRef) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            child_stats,
            node_kv,
        }
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }
}

pub struct ExplodeOperator {
    to_explode: Arc<Vec<BoundExpr>>,
    index_column: Option<String>,
}

impl ExplodeOperator {
    pub fn new(
        to_explode: Vec<BoundExpr>,
        ignore_empty_and_null: bool,
        index_column: Option<String>,
    ) -> Self {
        Self {
            to_explode: Arc::new(
                to_explode
                    .into_iter()
                    .map(|expr| {
                        BoundExpr::new_unchecked(explode(
                            expr.inner().clone(),
                            daft_dsl::lit(ignore_empty_and_null),
                        ))
                    })
                    .collect(),
            ),
            index_column,
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    type State = ();
    type Stats = ExplodeStats;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let to_explode = self.to_explode.clone();
        let index_column = self.index_column.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode, index_column.as_deref())?;
                    Ok((state, out))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )];
        if let Some(ref idx_col) = self.index_column {
            res.push(format!("Index column = {}", idx_col));
        }
        res
    }

    fn name(&self) -> NodeName {
        "Explode".into()
    }

    fn make_state(&self) -> Self::State {}

    fn op_type(&self) -> NodeType {
        NodeType::Explode
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use common_metrics::{Meter, StatSnapshot, ops::NodeInfo, snapshot::StatSnapshotImpl as _};

    use super::ExplodeStats;
    use crate::runtime_stats::{IntermediateRuntimeStats as _, RuntimeStats};

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    /// A mock child that reports a fixed estimated total.
    struct MockChildStats {
        estimated_total: u64,
    }
    impl RuntimeStats for MockChildStats {
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

    fn make_stats(child_estimated_total: u64) -> ExplodeStats {
        ExplodeStats::new(
            &Meter::test_scope("explode_test"),
            &node_info_from_id(1),
            Arc::new(MockChildStats {
                estimated_total: child_estimated_total,
            }),
        )
    }

    #[test]
    fn amplification_defaults_to_1x_with_no_input() {
        let stats = make_stats(1000);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        // No rows processed yet → amplification = 1.0 → estimated = 1000 * 1.0 = 1000
        assert_eq!(snap.total(), 1000);
        assert_eq!(snap.current_progress(), 0);
    }

    #[test]
    fn amplification_computed_from_rows_in_out() {
        let stats = make_stats(1000);
        // Simulate: 100 rows in, 300 rows out → 3x amplification
        stats.add_rows_in(100);
        stats.add_rows_out(300);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        // estimated = 1000 * 3.0 = 3000
        assert_eq!(snap.total(), 3000);
        assert_eq!(snap.current_progress(), 300);
    }

    #[test]
    fn amplification_updates_over_multiple_batches() {
        let stats = make_stats(2000);

        // Batch 1: 100 in, 200 out → 2x
        stats.add_rows_in(100);
        stats.add_rows_out(200);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.total(), 4000); // 2000 * 2.0

        // Batch 2: another 100 in, 400 out → cumulative 200 in, 600 out → 3x
        stats.add_rows_in(100);
        stats.add_rows_out(400);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.total(), 6000); // 2000 * 3.0
        assert_eq!(snap.current_progress(), 600);
    }

    #[test]
    fn fractional_amplification() {
        let stats = make_stats(1000);
        // 200 in, 100 out → 0.5x (e.g., many null lists filtered)
        stats.add_rows_in(200);
        stats.add_rows_out(100);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        assert_eq!(snap.total(), 500); // 1000 * 0.5
    }

    #[test]
    fn snapshot_message_contains_amplification() {
        let stats = make_stats(1000);
        stats.add_rows_in(100);
        stats.add_rows_out(250);
        let snap = stats.build_snapshot(Ordering::SeqCst);
        let msg = snap.to_message();
        assert!(
            msg.contains("2.50x"),
            "expected '2.50x' in message, got: {msg}"
        );
    }
}
