use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::NUM_CPUS;

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
    nulls_first: Vec<bool>,
}
pub struct SortSink {
    params: Arc<SortParams>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>, nulls_first: Vec<bool>) -> Self {
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
            }),
        }
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        _runtime_ref: &RuntimeRef,
    ) -> BlockingSinkSinkResult {
        state
            .as_any_mut()
            .downcast_mut::<SortState>()
            .expect("SortSink should have sort state")
            .push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        runtime
            .spawn(async move {
                let parts = states.into_iter().flat_map(|mut state| {
                    let state = state
                        .as_any_mut()
                        .downcast_mut::<SortState>()
                        .expect("State type mismatch");
                    state.finalize()
                });
                let concated = MicroPartition::concat(parts)?;
                let sorted = Arc::new(concated.sort(
                    &params.sort_by,
                    &params.descending,
                    &params.nulls_first,
                )?);
                Ok(Some(sorted))
            })
            .into()
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
