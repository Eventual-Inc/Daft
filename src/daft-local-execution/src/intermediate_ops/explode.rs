use std::{sync::Arc, time::Duration};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot, ops::NodeType, snapshot,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_list::explode;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
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

pub struct ExplodeStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl ExplodeStats {
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
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(cpu_us)),
            ROWS_IN_KEY; Stat::Count(rows_in),
            ROWS_OUT_KEY; Stat::Count(rows_out),
            "amplification"; Stat::Float(amplification),
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

pub struct ExplodeOperator {
    to_explode: Arc<Vec<BoundExpr>>,
}

impl ExplodeOperator {
    pub fn new(to_explode: Vec<BoundExpr>) -> Self {
        Self {
            to_explode: Arc::new(
                to_explode
                    .into_iter()
                    .map(|expr| BoundExpr::new_unchecked(explode(expr.inner().clone())))
                    .collect(),
            ),
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
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode)?;
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
        vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )]
    }

    fn name(&self) -> NodeName {
        "Explode".into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }

    fn op_type(&self) -> NodeType {
        NodeType::Explode
    }

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(ExplodeStats::new(id))
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
