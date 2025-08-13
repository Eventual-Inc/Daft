use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{snapshot, Stat, StatSnapshotSend};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ops::NodeType,
    pipeline::NodeName,
    runtime_stats::{RuntimeStats, CPU_US_KEY, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY},
    ExecutionTaskSpawner,
};

#[derive(Default)]
pub struct FilterStats {
    cpu_us: AtomicU64,
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
}

impl RuntimeStats for FilterStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_received = self.rows_received.load(ordering);
        let rows_emitted = self.rows_emitted.load(ordering);

        let selectivity = if rows_received == 0 {
            100.0
        } else {
            (rows_emitted as f64 / rows_received as f64) * 100.0
        };
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(cpu_us)),
            ROWS_RECEIVED_KEY; Stat::Count(rows_received),
            ROWS_EMITTED_KEY; Stat::Count(rows_emitted),
            "selectivity"; Stat::Percent(selectivity),
        ]
    }

    fn add_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_emitted(&self, rows: u64) {
        self.rows_emitted.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
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

    #[instrument(skip_all, name = "FilterOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let predicate = self.predicate.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.filter(&[predicate])?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
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

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(FilterStats::default())
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }
}
