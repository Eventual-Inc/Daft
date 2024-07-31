use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use futures::{stream, StreamExt};
use tracing::instrument;

use crate::sources::source::Source;

use super::{
    blocking_sink::{BlockingSink, BlockingSinkStatus},
    sink::{Sink, SinkResultType},
};

pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    state: SortState,
}

enum SortState {
    Building(Vec<Arc<MicroPartition>>),
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
    // #[instrument(skip_all, name = "SortSink::sink")]
    // fn sink(&mut self, index: usize, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
    //     assert_eq!(index, 0);
    //     self.parts.push(input.clone());
    //     Ok(SinkResultType::NeedMoreInput)
    // }
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        if let SortState::Building(parts) = &mut self.state {
            parts.push(input.clone());
        } else {
            panic!("sink should be in building phase");
        }
        Ok(BlockingSinkStatus::NeedMoreInput)
    }
    fn name(&self) -> &'static str {
        "Sort"
    }
    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(&mut self) -> DaftResult<()> {
        if let SortState::Building(parts) = &mut self.state {
            let concated =
                MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
            let sorted = concated.sort(&self.sort_by, &self.descending)?;
            self.state = SortState::Done(Arc::new(sorted));
            Ok(())
        } else {
            panic!("finalize should be in building phase");
        }
    }
    fn as_source(&mut self) -> &mut dyn crate::sources::source::Source {
        self
    }
}

impl Source for SortSink {
    fn get_data(&self) -> crate::sources::source::SourceStream {
        if let SortState::Done(parts) = &self.state {
            stream::iter([Ok(parts.clone())]).boxed()
        } else {
            panic!("get_data should be in done phase");
        }
    }
}
