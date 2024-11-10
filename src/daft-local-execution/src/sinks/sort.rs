use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::{OperatorOutput, NUM_CPUS};

enum SortState {
    Building(Vec<Arc<MicroPartition>>),
    Done,
}

impl SortState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Building(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("SortSink should be in Building state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Building(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("SortSink should be in Building state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for SortState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct SortParams {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
}
pub struct SortSink {
    params: Arc<SortParams>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
            }),
        }
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        _runtime_ref: &RuntimeRef,
    ) -> OperatorOutput<DaftResult<BlockingSinkStatus>> {
        state
            .as_any_mut()
            .downcast_mut::<SortState>()
            .expect("SortSink should have sort state")
            .push(input.clone());
        OperatorOutput::Immediate(Ok(BlockingSinkStatus::NeedMoreInput(state)))
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        runtime: &RuntimeRef,
    ) -> OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>> {
        let params = self.params.clone();
        let fut = runtime.spawn(async move {
            let parts = states.into_iter().flat_map(|mut state| {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<SortState>()
                    .expect("State type mismatch");
                state.finalize()
            });
            let concated = MicroPartition::concat(parts)?;
            let sorted = Arc::new(concated.sort(&params.sort_by, &params.descending)?);
            Ok(Some(sorted.into()))
        });
        OperatorOutput::Future(fut)
    }

    fn name(&self) -> &'static str {
        "SortResult"
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(SortState::Building(Vec::new())))
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }
}
