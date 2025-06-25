use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

enum RepartitionState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl RepartitionState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for RepartitionState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct RepartitionSink {
    columns: Arc<Vec<BoundExpr>>,
    num_partitions: usize,
}

impl RepartitionSink {
    pub fn new(columns: Vec<BoundExpr>, num_partitions: usize) -> Self {
        Self {
            columns: Arc::new(columns),
            num_partitions,
        }
    }
}

impl BlockingSink for RepartitionSink {
    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        state
            .as_any_mut()
            .downcast_mut::<RepartitionState>()
            .expect("RepartitionSink should have RepartitionState")
            .push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let columns = self.columns.clone();
        let num_partitions = self.num_partitions;

        spawner
            .spawn(
                async move {
                    let all_parts = states.into_iter().flat_map(|mut state| {
                        state
                            .as_any_mut()
                            .downcast_mut::<RepartitionState>()
                            .expect("RepartitionSink should have RepartitionState")
                            .finalize()
                    });
                    let concated = MicroPartition::concat(all_parts)?;
                    let reparted = concated.partition_by_hash(&columns, num_partitions)?;
                    Ok(Some(Arc::new(reparted)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "Repartition"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "Repartition: By {} into {} partitions",
            self.columns.iter().map(|e| e.to_string()).join(", "),
            self.num_partitions
        )]
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(RepartitionState::Accumulating(vec![])))
    }
}
