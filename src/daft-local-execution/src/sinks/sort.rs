use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
use crate::pipeline::PipelineResultType;
pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    state: SortState,
}

enum SortState {
    Building(Vec<Arc<MicroPartition>>),
    #[allow(dead_code)]
    Done(Arc<MicroPartition>),
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
            state: SortState::Building(vec![]),
        }
    }
    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        if let SortState::Building(parts) = &mut self.state {
            parts.push(input.clone());
        } else {
            panic!("SortSink should be in Building state");
        }
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        if let SortState::Building(parts) = &mut self.state {
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
            self.state = SortState::Done(sorted.clone());
            Ok(Some(sorted.into()))
        } else {
            panic!("SortSink should be in Building state");
        }
    }
    fn name(&self) -> &'static str {
        "SortResult"
    }
}
