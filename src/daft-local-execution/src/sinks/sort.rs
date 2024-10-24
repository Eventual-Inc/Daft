use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkState, BlockingSinkStatus, DynBlockingSinkState,
};
use crate::{pipeline::PipelineResultType, NUM_CPUS};

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

    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let res = if let Self::Building(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("SortSink should be in Building state");
        };
        *self = Self::Done;
        Ok(res)
    }
}

impl DynBlockingSinkState for SortState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
        }
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state_handle: &BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        state_handle.with_state_mut::<SortState, _, _>(|state| {
            state.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        })
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn DynBlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        let mut parts = Vec::new();
        for mut state in states {
            let state = state
                .as_any_mut()
                .downcast_mut::<SortState>()
                .expect("State type mismatch");
            parts.extend(state.finalize()?);
        }
        assert!(
            !parts.is_empty(),
            "We can not finalize SortSink with no data"
        );
        let concated = MicroPartition::concat(
            &parts
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect::<Vec<_>>(),
        )?;
        let sorted = Arc::new(concated.sort(&self.sort_by, &self.descending)?);
        Ok(Some(sorted.into()))
    }

    fn name(&self) -> &'static str {
        "SortResult"
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynBlockingSinkState>> {
        Ok(Box::new(SortState::Building(Vec::new())))
    }

    // SortSink currently does not do any computation in the sink method, so no need to buffer.
    fn morsel_size(&self) -> Option<usize> {
        None
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }
}
