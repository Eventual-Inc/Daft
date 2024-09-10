use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::pipeline::PipelineResultType;
pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
}

enum SortState {
    Building(Vec<Arc<MicroPartition>>),
    Done(Vec<Arc<MicroPartition>>),
}

impl BlockingSinkState for SortState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finalize(&mut self) {
        if let SortState::Building(parts) = self {
            *self = SortState::Done(std::mem::take(parts));
        }
    }
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
        }
    }
    pub fn arced(self) -> Arc<dyn BlockingSink> {
        Arc::new(self)
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut dyn BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        let sort_state = state.as_any_mut().downcast_mut::<SortState>().unwrap();
        if let SortState::Building(parts) = sort_state {
            parts.push(input.clone());
        } else {
            panic!("SortSink should be in Building state");
        }
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: &[&dyn BlockingSinkState],
    ) -> DaftResult<Option<PipelineResultType>> {
        let parts = states
            .iter()
            .flat_map(|state| {
                let sort_state = state.as_any().downcast_ref::<SortState>().unwrap();
                if let SortState::Done(parts) = sort_state {
                    parts
                } else {
                    panic!("sortState should be in Done state");
                }
            })
            .collect::<Vec<_>>();

        let concated =
            MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let sorted = Arc::new(concated.sort(&self.sort_by, &self.descending)?);
        Ok(Some(sorted.into()))
    }

    fn name(&self) -> &'static str {
        "SortSink"
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(SortState::Building(vec![])))
    }
}
