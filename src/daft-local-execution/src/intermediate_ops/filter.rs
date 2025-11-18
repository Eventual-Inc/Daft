use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, ops::NodeType, snapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use opentelemetry::{KeyValue, global};
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    runtime_stats::{Counter, RuntimeStats},
};

pub struct FilterStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl FilterStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, CPU_US_KEY.into(), None),
            rows_in: Counter::new(&meter, ROWS_IN_KEY.into(), None),
            rows_out: Counter::new(&meter, ROWS_OUT_KEY.into(), None),
            node_kv,
        }
    }
}

impl RuntimeStats for FilterStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);

        let selectivity = if rows_in == 0 {
            100.0
        } else {
            (rows_out as f64 / rows_in as f64) * 100.0
        };
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(cpu_us)),
            ROWS_IN_KEY; Stat::Count(rows_in),
            ROWS_OUT_KEY; Stat::Count(rows_out),
            "selectivity"; Stat::Percent(selectivity),
        ]
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
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

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(FilterStats::new(id))
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }
}
