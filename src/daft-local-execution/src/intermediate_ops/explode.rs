use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, meters::Counter, ops::NodeType,
    snapshot::ExplodeSnapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_list::explode;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use opentelemetry::{KeyValue, metrics::Meter};
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, runtime_stats::RuntimeStats};

pub struct ExplodeStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl ExplodeStats {
    pub fn new(meter: &Meter, id: usize) -> Self {
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(meter, CPU_US_KEY, None),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None),
            node_kv,
        }
    }
}

impl RuntimeStats for ExplodeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: std::sync::atomic::Ordering) -> StatSnapshot {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_out = self.rows_out.load(ordering);

        let amplification = if rows_in == 0 {
            1.
        } else {
            rows_out as f64 / rows_in as f64
        };

        StatSnapshot::Explode(ExplodeSnapshot {
            cpu_us,
            rows_in,
            rows_out,
            amplification,
        })
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
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let to_explode = self.to_explode.clone();
        let index_column = self.index_column.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode, index_column.as_deref())?;
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

    fn make_runtime_stats(&self, meter: &Meter, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(ExplodeStats::new(meter, id))
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
